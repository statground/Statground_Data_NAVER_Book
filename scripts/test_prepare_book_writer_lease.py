import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import prepare_book_writer_lease as setup

class SourcePinContractTests(unittest.TestCase):
    def test_action_and_preparer_pin_the_same_qualified_sql_source(self):
        expected_commit = "4c90b20f64027019e098d9b848e3e702b7ee24b0"
        expected_helper = "064110bf82ddab9680a88ccf1c0a05f1ae5d6662cb37cc3ac6e8b189d5ba6e66"
        self.assertEqual(setup.SQL_COMMIT, expected_commit)
        self.assertEqual(setup.HELPER_SHA256, expected_helper)
        root = Path(__file__).resolve().parents[1]
        action = (root / ".github/actions/book-writer-lease/action.yml").read_text()
        self.assertEqual(re.findall(r"(?m)^        ref: ([0-9a-f]{40})$", action), [expected_commit])
        self.assertEqual(re.findall(r'rev-parse HEAD\)" = "([0-9a-f]{40})"', action), [expected_commit])
        self.assertIn("sparse-checkout: " + setup.HELPER_RELATIVE, action)
        self.assertIn("persist-credentials: false", action)
        self.assertIn("scripts/prepare_book_writer_lease.py", action)

class PrepareWriterLeaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.sql = self.root / "sql"
        self.helper = self.sql / setup.HELPER_RELATIVE
        self.helper.parent.mkdir(parents=True)
        self.helper.write_bytes(b"print('verified-helper')\n")
        self.patch = patch.object(setup, "HELPER_SHA256", hashlib.sha256(self.helper.read_bytes()).hexdigest())
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.runtime = self.root / "runtime"
        self.python = Path(sys.executable)

    def config(self, **changes):
        value = {"password_env": "BOOK_WRITER_LEASE_TIDB_PASSWORD", "tls_ca_file": "", "allow_private_tcp": True}
        value.update(changes)
        return (json.dumps(value, indent=2) + "\n").encode()

    def test_preserves_reviewed_bytes_and_protects_runtime(self):
        raw = self.config()
        wrapper, config = setup.prepare(self.sql, self.runtime, self.python, raw, b"", "fixture-only")
        self.assertEqual(config.read_bytes(), raw)
        self.assertEqual(stat.S_IMODE(config.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(wrapper.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.runtime.stat().st_mode), 0o700)
        result = subprocess.run([str(wrapper)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "verified-helper")
        self.assertNotIn("fixture-only", b"".join(p.read_bytes() for p in self.runtime.iterdir()).decode())

    def test_wrapper_rejects_helper_replacement(self):
        wrapper, _ = setup.prepare(self.sql, self.runtime, self.python, self.config(), b"", "fixture-only")
        (self.runtime / "helper.py").write_bytes(b"print('changed-helper')\n")
        result = subprocess.run([str(wrapper)], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("changed-helper", result.stdout)

    def test_pinned_source_mismatch_has_no_runtime_side_effect(self):
        self.helper.write_bytes(b"modified")
        with self.assertRaises(setup.PreparationError):
            setup.prepare(self.sql, self.runtime, self.python, self.config(), b"", "fixture-only")
        self.assertFalse(self.runtime.exists())

    def test_ca_is_written_to_exact_reviewed_path_without_config_rewrite(self):
        raw = self.config(tls_ca_file=str(self.runtime / "ca.pem"), allow_private_tcp=False)
        _, config = setup.prepare(self.sql, self.runtime, self.python, raw, b"fixture-ca", "fixture-only")
        self.assertEqual(config.read_bytes(), raw)
        self.assertEqual((self.runtime / "ca.pem").read_bytes(), b"fixture-ca")

    def test_missing_credentials_or_unreviewed_ca_path_fail_before_runtime_creation(self):
        for raw, ca, password in [(self.config(), b"", ""), (self.config(password_env="OTHER_PASSWORD"), b"", "fixture-only"), (self.config(tls_ca_file="/another/ca.pem"), b"ca", "fixture-only"), (self.config(allow_private_tcp=False), b"", "fixture-only")]:
            with self.subTest(config=raw):
                with self.assertRaises(setup.PreparationError):
                    setup.prepare(self.sql, self.runtime, self.python, raw, ca, password)
                self.assertFalse(self.runtime.exists())

    def test_existing_runtime_is_never_overwritten(self):
        setup.prepare(self.sql, self.runtime, self.python, self.config(), b"", "fixture-only")
        with self.assertRaises(FileExistsError):
            setup.prepare(self.sql, self.runtime, self.python, self.config(), b"", "fixture-only")

class RunnerCATemplateTests(unittest.TestCase):
    setUp = PrepareWriterLeaseTests.setUp

    def source(self, **changes):
        value = {"schema": "sg.book.writer-lease.config.v1",
                 "resource": "statground-book-shared-writer-v1", "host": "203.0.113.10",
                 "endpoint_pin": "203.0.113.10:51002",
                 "port": 51002, "user": "book_writer_0123456789abcdef",
                 "password_env": "BOOK_WRITER_LEASE_TIDB_PASSWORD",
                 "tls_ca_file": setup.RUNNER_CA_TEMPLATE, "allow_private_tcp": False,
                 "control_epoch": "11111111-1111-4111-8111-111111111111",
                 "activation_revision": 1, "coverage_sha256": "a" * 64,
                 "lease_seconds": 120}
        value.update(changes)
        return (json.dumps(value, indent=2) + "\n").encode()

    def runner(self):
        return patch.dict(os.environ, {"GITHUB_ACTIONS": "true", "RUNNER_TEMP": str(self.root)})

    def test_template_resolves_only_actual_runner_path_and_preserves_all_other_fields(self):
        self.runtime = self.root / "book-writer-lease"
        original = self.source()
        with self.runner():
            _, config_path = setup.prepare(self.sql, self.runtime, self.python, original, b"test-ca", "fixture-only")
        result = json.loads(config_path.read_bytes())
        expected = json.loads(original); expected["tls_ca_file"] = str(self.runtime / "ca.pem")
        self.assertEqual(result, expected)
        record = json.loads((self.runtime / "config-provenance.json").read_bytes())
        self.assertEqual(record["source_config_sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(record["runtime_config_sha256"], hashlib.sha256(config_path.read_bytes()).hexdigest())
        self.assertNotEqual(record["source_config_sha256"], record["runtime_config_sha256"])
        self.assertEqual(stat.S_IMODE((self.runtime / "config-provenance.json").stat().st_mode), 0o600)

    def test_template_requires_actual_runner_context_before_any_runtime_write(self):
        self.runtime = self.root / "book-writer-lease"
        for environment in ({"GITHUB_ACTIONS": "false", "RUNNER_TEMP": str(self.root)},
                            {"GITHUB_ACTIONS": "true", "RUNNER_TEMP": "/a/different/root"}):
            with self.subTest(environment=environment), patch.dict(os.environ, environment), self.assertRaises(setup.PreparationError):
                setup.prepare(self.sql, self.runtime, self.python, self.source(), b"ca", "fixture-only")
            self.assertFalse(self.runtime.exists())

    def test_template_refuses_duplicate_fields_password_fields_and_nonfinite_numbers(self):
        self.runtime = self.root / "book-writer-lease"
        duplicate = self.source().replace(b'"port": 51002,', b'"port": 51002, "port": 51002,')
        malformed = (duplicate, self.source(password="never-written"), self.source(lease_seconds=float("nan")))
        for raw in malformed:
            with self.runner(), self.assertRaises(setup.PreparationError):
                setup.prepare(self.sql, self.runtime, self.python, raw, b"ca", "fixture-only")
            self.assertFalse(self.runtime.exists())

    def test_no_general_environment_or_path_expansion(self):
        self.runtime = self.root / "book-writer-lease"
        for template in ("$RUNNER_TEMP/book-writer-lease/ca.pem", "${HOME}/ca.pem",
                         "${RUNNER_TEMP}/../ca.pem"):
            with self.runner(), self.assertRaises(setup.PreparationError):
                setup.prepare(self.sql, self.runtime, self.python, self.source(tls_ca_file=template), b"ca", "fixture-only")
            self.assertFalse(self.runtime.exists())

    def test_concrete_host_path_keeps_original_bytes_and_no_runner_provenance(self):
        original = self.source(tls_ca_file=str(self.runtime / "ca.pem"))
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false", "RUNNER_TEMP": "/unused"}):
            _, path = setup.prepare(self.sql, self.runtime, self.python, original, b"ca", "fixture-only")
        self.assertEqual(path.read_bytes(), original)
        self.assertFalse((self.runtime / "config-provenance.json").exists())

if __name__ == "__main__":
    unittest.main()
