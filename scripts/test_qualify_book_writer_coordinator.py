"""Offline controls only; no real database, provider or workflow dispatch."""
import ast
import contextlib
import hashlib
import json
import os
from pathlib import Path
import signal
import tempfile
import types
import unittest
from unittest import mock

import qualify_book_writer_coordinator as qualifier


REQUEST = "11111111-1111-4111-8111-111111111111"


def config(**changes):
    value = dict(host="8.8.8.8", port=51002, user="book_writer_0123456789abcdef",
                 password_env="BOOK_WRITER_LEASE_TIDB_PASSWORD",
                 tls_ca_file="/a/runner/book-writer-lease/ca.pem", allow_private_tcp=False,
                 control_epoch=REQUEST, activation_revision=1, coverage_sha256="a" * 64)
    value.update(changes)
    return types.SimpleNamespace(**value)


class ReadonlyQualificationTests(unittest.TestCase):
    def test_default_off_never_loads_files_or_connects(self):
        with mock.patch.object(qualifier, "HeldFile", side_effect=AssertionError("no file")):
            value, budget = qualifier.run(False, "", "")
        self.assertEqual(value["status"], "default_off")
        self.assertIsNone(budget)

    def test_wrong_or_malformed_source_binding_stops_before_files_or_DB(self):
        for request, expected in ((REQUEST, "b" * 64), ("bad-request", "a" * 64), (REQUEST, "not-a-hash")):
            with self.subTest(request=request, expected=expected), mock.patch.dict(os.environ, {"BOOK_WRITER_LEASE_CONFIG_JSON": "{}"}), mock.patch.object(qualifier, "HeldFile", side_effect=AssertionError("no file")):
                value, budget = qualifier.run(True, request, expected)
                try:
                    self.assertEqual(value["status"], "blocked")
                    self.assertEqual(value["connection_attempts"], 0)
                    self.assertEqual(value["SELECT_attempts"], 0)
                finally:
                    budget.close()

    def test_source_hash_is_of_exact_bytes_not_normalized_JSON(self):
        raw = b'{"one":1}\n'
        qualifier.validate_request(REQUEST, hashlib.sha256(raw).hexdigest(), raw)
        with self.assertRaises(qualifier.Refused):
            qualifier.validate_request(REQUEST, hashlib.sha256(b'{ "one":1}\n').hexdigest(), raw)

    def test_dedicated_selector_rejects_private_plaintext_other_port_and_root(self):
        runtime = Path("/a/runner/book-writer-lease")
        qualifier.validate_dedicated(config(), runtime)
        for changes in ({"host": "127.0.0.1"}, {"host": "coordinator.invalid"},
                        {"port": 50001}, {"allow_private_tcp": True}, {"user": "root"},
                        {"tls_ca_file": "/a/guessed/ca.pem"}, {"activation_revision": 0}):
            with self.subTest(changes=changes), self.assertRaises((qualifier.Refused, ValueError)):
                qualifier.validate_dedicated(config(**changes), runtime)

    def test_tls_requires_actual_ssl_socket_not_a_success_flag(self):
        connection = types.SimpleNamespace(_sock=types.SimpleNamespace(verified=True))
        with self.assertRaises(qualifier.Refused):
            qualifier.validate_tls(connection, config().host)

    def test_TLS_peer_must_match_exact_selected_public_IP_and_port(self):
        # A spec mock exercises the selector; it is no real TLS evidence.
        wire = mock.Mock(spec=qualifier.ssl.SSLSocket)
        wire.context = types.SimpleNamespace(verify_mode=qualifier.ssl.CERT_REQUIRED, check_hostname=True)
        wire.server_hostname = config().host
        wire.getpeercert.return_value = {"fixture": "certificate"}
        wire.cipher.return_value = ("fixture-cipher", "TLSv1.3", 256)
        wire.version.return_value = "TLSv1.3"
        connection = types.SimpleNamespace(_sock=wire)
        for peer in (("1.1.1.1", 51002), (config().host, 50001)):
            wire.getpeername.return_value = peer
            with self.assertRaises(qualifier.Refused): qualifier.validate_tls(connection, config().host)
        wire.getpeername.return_value = (config().host, 51002)
        qualifier.validate_tls(connection, config().host)

    def test_native_identity_and_security_policy_must_match(self):
        row = {"server_version": "8.0.11-TiDB-v8.5.6", "effective_user": config().user + "@%",
               "selected_database": "user", "secure_transport": 1}
        qualifier.validate_identity(row, config())
        for changes in ({"server_version": "8.0.11-TiDB-v8.5.5"}, {"effective_user": "another@%"},
                        {"effective_user": config().user + "@localhost"},
                        {"selected_database": "mysql"}, {"secure_transport": 0}):
            other = dict(row); other.update(changes)
            with self.subTest(changes=changes), self.assertRaises(qualifier.Refused):
                qualifier.validate_identity(other, config())

    def test_matching_epoch_and_hash_are_not_enough_when_lease_is_busy(self):
        row = {"enabled": 1, "control_epoch": REQUEST, "activation_revision": 1,
               "coverage_sha256": "a" * 64, "idle": 1}
        qualifier.validate_control(row, config())
        for changes in ({"enabled": 0}, {"control_epoch": "22222222-2222-4222-8222-222222222222"},
                        {"coverage_sha256": "b" * 64}, {"idle": 0}):
            other = dict(row); other.update(changes)
            with self.subTest(changes=changes), self.assertRaises(qualifier.Refused):
                qualifier.validate_control(other, config())

    def connection(self, rows, columns):
        executed = []
        class Cursor:
            description = [(name,) for name in columns]
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def execute(self, sql, params): executed.append((sql, params))
            def fetchone(self): return rows.pop(0) if rows else None
        return types.SimpleNamespace(cursor=lambda: Cursor()), executed

    def test_closed_SQL_allowlist_refuses_arbitrary_statement_before_cursor(self):
        connection = types.SimpleNamespace(cursor=lambda: self.fail("no cursor"))
        for sql in ("UPDATE user.book_shared_writer_lease SET holder=''", "SELECT 1", "COMMIT", "ROLLBACK"):
            with self.subTest(sql=sql), self.assertRaises(qualifier.Refused):
                qualifier.select_one(connection, sql, (), ("pending",))

    def test_result_count_schema_and_size_are_bounded(self):
        for rows, columns in (([{"pending": 0}, {"pending": 0}], ("pending",)),
                              ([{"pending": 0}], ("other",)),
                              ([{"pending": "x" * 65536}], ("pending",))):
            connection, executed = self.connection(rows, columns)
            with self.assertRaises(qualifier.Refused):
                qualifier.select_one(connection, qualifier.PENDING_SQL, (), ("pending",))
            self.assertEqual(len(executed), 1)
        connection, executed = self.connection([{"pending": 0}], ("pending",))
        self.assertEqual(qualifier.select_one(connection, qualifier.PENDING_SQL, (), ("pending",)), {"pending": 0})

    def test_output_is_finite_and_cannot_claim_writer_or_publication_readiness(self):
        value = qualifier.fixed()
        self.assertFalse(value["actor_coverage_proved"])
        self.assertFalse(value["physical_durability_proved"])
        self.assertFalse(value["Book_admission"])
        for key in ("lease_operations", "DB_writes", "provider_requests", "publication_attempts"):
            self.assertEqual(value[key], 0)
        self.assertNotIn("config_sha256", value)
        self.assertLess(len(json.dumps(value)), 4096)

    def test_actual_source_uses_three_SELECTs_no_mutator_or_provider_calls(self):
        source = Path(qualifier.__file__).read_text()
        tree = ast.parse(source)
        calls = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
        self.assertFalse(calls & {"acquire", "renew", "run", "mutate_locked", "activation", "commit", "rollback", "read_lease", "finish"})
        self.assertEqual(len(qualifier.SQL), 3)
        self.assertTrue(all(sql.startswith("SELECT /*+ MAX_EXECUTION_TIME(3000) */") for sql in qualifier.SQL))
        self.assertNotIn("pymysql.connect", source)
        self.assertIn("expected_provenance", source)
        self.assertIn("config_file.raw == expected_runtime", source)

    def test_workflow_manual_only_read_permissions_no_provider_or_dispatch_step(self):
        workflow = Path(__file__).resolve().parents[1] / ".github/workflows/book_writer_coordinator_qualification.yml"
        text = workflow.read_text()
        self.assertIn("workflow_dispatch:", text)
        for trigger in ("schedule:", "workflow_call:", "push:", "pull_request:"):
            self.assertNotIn(trigger, text)
        self.assertIn("contents: read", text)
        self.assertIn("cancel-in-progress: false", text)
        self.assertIn("ref: ${{ github.sha }}", text)
        self.assertIn("timeout-minutes: 10", text)
        self.assertIn("inputs.expected_source_config_sha256", text)
        self.assertNotIn("gh ", text)
        self.assertNotIn("curl ", text)
        self.assertNotIn("kakao_book_collect", text)
        self.assertNotIn("shutil.rmtree", text)
        self.assertNotIn("unlink(", text)


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.now = 100.0; self.handler = signal.SIG_DFL; self.timer = (0.0, 0.0)
        def set_handler(_signal, handler):
            old = self.handler; self.handler = handler; return old
        def set_timer(_timer, interval):
            old = self.timer; self.timer = (interval, 0.0); return old
        patches = (mock.patch.object(qualifier.time, "monotonic", side_effect=lambda: self.now),
                   mock.patch.object(qualifier.signal, "getitimer", side_effect=lambda _: self.timer),
                   mock.patch.object(qualifier.signal, "getsignal", side_effect=lambda _: self.handler),
                   mock.patch.object(qualifier.signal, "signal", side_effect=set_handler),
                   mock.patch.object(qualifier.signal, "setitimer", side_effect=set_timer))
        for patch in patches:
            patch.start(); self.addCleanup(patch.stop)

    def test_one_clock_four_ordered_calls_and_no_replay(self):
        budget = qualifier.Budget()
        self.assertEqual(budget.end, 135.0)
        for ordinal in range(4):
            with budget.call(ordinal): self.now += 1
        with self.assertRaises(qualifier.Refused):
            with budget.call(3): pass

    def test_native_failure_restores_only_earliest_cleanup_not_original_26(self):
        budget = qualifier.Budget()
        with self.assertRaises(ValueError):
            with budget.call(0):
                self.now = 101.0; raise ValueError("fixture")
        self.assertEqual(budget.cleanup_end, 110.0)
        self.assertEqual(self.timer, (9.0, 0.0))
        self.now = 104.0; budget.fail(OSError())
        self.assertEqual(budget.cleanup_end, 110.0)
        self.assertEqual(self.timer, (6.0, 0.0))

    def test_insufficient_full_remaining_allowance_refuses_before_call(self):
        budget = qualifier.Budget(); self.now = 108.0
        with self.assertRaises(qualifier.Refused):
            with budget.call(0): self.fail("must not dispatch")
        self.assertEqual(budget.used, set())

    def test_expired_cleanup_cannot_restore_or_extend_an_alarm(self):
        budget = qualifier.Budget(); self.now = 101.0; budget.fail(ValueError())
        self.now = 110.0
        with self.assertRaises(qualifier.Deadline): budget.arm(budget.cleanup_end)
        self.assertEqual(budget.cleanup_end, 110.0)


class CompleteOfflinePathTests(unittest.TestCase):
    """Exercise the full runner path with an explicitly fake helper/connection."""
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.runtime = self.root / "book-writer-lease"
        self.executed = []; self.closed = 0
        owner = self
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def execute(self, sql, params):
                owner.executed.append((sql, params))
                if sql == qualifier.IDENTITY_SQL:
                    row = {"server_version": "8.0.11-TiDB-v8.5.6", "effective_user": "book_writer_0123456789abcdef@%",
                           "selected_database": "user", "secure_transport": 1}
                elif sql == qualifier.CONTROL_SQL:
                    row = {"enabled": 1, "control_epoch": REQUEST, "activation_revision": 1,
                           "coverage_sha256": "a" * 64, "idle": 1}
                elif sql == qualifier.PENDING_SQL:
                    row = {"pending": 0}
                else:
                    raise AssertionError("unexpected SQL")
                self.rows = [row]; self.description = [(key,) for key in row]
            def fetchone(self): return self.rows.pop(0) if self.rows else None
        class Connection:
            def cursor(self): return Cursor()
            def close(self): owner.closed += 1
        bridge = types.ModuleType("_offline_book_connection_fixture")
        bridge.connection = Connection()
        self.connection = bridge.connection
        patch = mock.patch.dict("sys.modules", {bridge.__name__: bridge})
        patch.start(); self.addCleanup(patch.stop)
        self.helper = ("import json,types\nfrom pathlib import Path\n"
                       "import _offline_book_connection_fixture as fixture\n"
                       "RESOURCE='statground-book-shared-writer-v1'\n"
                       "def load_config(path,expected_sha256=None):\n"
                       " return types.SimpleNamespace(**json.loads(Path(path).read_bytes()))\n"
                       "def connect(config):\n return fixture.connection\n").encode()
        pin = mock.patch.object(qualifier.preparation, "HELPER_SHA256", hashlib.sha256(self.helper).hexdigest())
        pin.start(); self.addCleanup(pin.stop)
        sql = self.root / "sql"
        helper = sql / qualifier.preparation.HELPER_RELATIVE
        helper.parent.mkdir(parents=True); helper.write_bytes(self.helper)
        value = vars(config()).copy()
        value.update(schema="sg.book.writer-lease.config.v1", resource="statground-book-shared-writer-v1",
                     lease_seconds=120, endpoint_pin="8.8.8.8:51002", tls_ca_file=qualifier.preparation.RUNNER_CA_TEMPLATE)
        self.source = (json.dumps(value, indent=2) + "\n").encode()
        self.sha256 = hashlib.sha256(self.source).hexdigest()
        environment = mock.patch.dict(os.environ, {"RUNNER_TEMP": str(self.root), "GITHUB_ACTIONS": "true",
            "BOOK_WRITER_LEASE_CONFIG_JSON": self.source.decode(), "BOOK_WRITER_LEASE_CA_PEM": "fixture-ca",
            "BOOK_WRITER_LEASE_TIDB_PASSWORD": "fixture-not-used"})
        environment.start(); self.addCleanup(environment.stop)
        qualifier.preparation.prepare(sql, self.runtime, Path("/usr/bin/python3"), self.source, b"fixture-ca", "fixture-not-used")

    def invoke(self):
        # This deliberately substitutes TLS only in this offline fixture; it
        # proves orchestration, not real TLS/native/physical qualification.
        with mock.patch.object(qualifier, "validate_tls"):
            value, budget = qualifier.run(True, REQUEST, self.sha256)
        try:
            return value
        finally:
            budget.close()

    def test_complete_fake_transport_is_nonadmitting_and_closes_exactly_once(self):
        value = self.invoke()
        self.assertEqual(value["status"], "qualified_readonly_nonadmitting")
        self.assertEqual((value["connection_attempts"], value["SELECT_attempts"], self.closed), (1, 3, 1))
        self.assertEqual([item[0] for item in self.executed], list(qualifier.SQL))
        self.assertFalse(value["actor_coverage_proved"])
        self.assertFalse(value["Book_admission"])

    def test_forged_provenance_cannot_replace_actual_source_to_runtime_comparison(self):
        path = self.runtime / "config.json"
        changed = json.loads(path.read_bytes()); changed["control_epoch"] = "22222222-2222-4222-8222-222222222222"
        raw = (json.dumps(changed) + "\n").encode(); path.write_bytes(raw)
        record_path = self.runtime / "config-provenance.json"
        record = json.loads(record_path.read_bytes()); record["runtime_config_sha256"] = hashlib.sha256(raw).hexdigest()
        record_path.write_text(json.dumps(record))
        value = self.invoke()
        self.assertEqual(value["status"], "blocked")
        self.assertEqual(value["connection_attempts"], 0)
        self.assertEqual(self.executed, [])

    def test_CA_secret_file_mismatch_refuses_before_connection(self):
        (self.runtime / "ca.pem").write_bytes(b"different-fixture-ca")
        value = self.invoke()
        self.assertEqual(value["status"], "blocked")
        self.assertEqual(value["connection_attempts"], 0)

    def test_unknown_connection_close_never_returns_qualified_status(self):
        with mock.patch.object(self.connection, "close", side_effect=qualifier.Deadline()):
            value = self.invoke()
        self.assertEqual(value["status"], "blocked")
        self.assertIs(value["connection_closed"], False)
        self.assertEqual(value["stage"], "cleanup")

    def test_cleanup_expiry_prevents_any_positive_return(self):
        created = []
        original = qualifier.Budget
        class ExpiringBudget(original):
            def __init__(self):
                super().__init__(); created.append(self)
            def cleanup(self):
                super().cleanup(); raise qualifier.Deadline()
        try:
            with mock.patch.object(qualifier, "Budget", ExpiringBudget), mock.patch.object(qualifier, "validate_tls"):
                with self.assertRaises(qualifier.Deadline): qualifier.run(True, REQUEST, self.sha256)
        finally:
            for budget in created: budget.close()
        self.assertEqual(self.closed, 0)  # Unknown cleanup, never a positive result.


if __name__ == "__main__":
    unittest.main()
