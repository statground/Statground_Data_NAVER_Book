#!/usr/bin/env python3
"""Observe an existing Book publication; never ask a producer to publish it."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import http.client
import inspect
import json
import os
import re
import signal
import stat
import sys
import time
import types
import uuid
from contextlib import contextmanager
from pathlib import Path

SCHEMA = "sg.book.host-publication-observation.v1"
SDK_NAME = "book_serving_generation_publisher.py"
SDK_SHA256 = "3df02efd91950418ba8148b8438fa402c7dc187792ec85e9e43e9d7a0d0497d0"
LEGACY_NAME = "webr_book_generation_publisher.py"
LEGACY_SHA256 = "69ab441029cb628a999181a37f45554a48845411b39535077cb0e36ebbfa032c"
RELATIVE = Path("docker-compose/50005_Clickhouse_Statground")
REASONS = frozenset({"observed_complete", "awaiting_authoritative_host_publication",
    "legacy_contract_not_observed", "source_not_ready", "expected_identity_mismatch",
    "incomplete_inventory", "authority_changed", "source_pin_or_custody",
    "readonly_request_refused", "budget_exhausted", "invalid_input", "observation_failed"})
OVERFLOW_MODES = ("read_overflow_mode", "read_overflow_mode_leaf", "timeout_overflow_mode", "timeout_overflow_mode_leaf",
    "result_overflow_mode", "group_by_overflow_mode", "sort_overflow_mode",
    "join_overflow_mode", "transfer_overflow_mode", "distinct_overflow_mode")
PREFLIGHT_KEYS = frozenset({"status", "database_writes", "service",
    "source_refresh_started_at", "source_refresh_success_at", "source_generation",
    "source_rows", "raw_target_rows", "content_sha256", "policy_authority_revision"})
SDK_READ_CALLERS = frozenset({"read_only_session_gate", "source_ddl_guard",
    "object_preflight", "outbox_preflight", "replica_health_gate", "capacity_gate",
    "refresh_evidence", "source_generation", "target_replica_health_gate",
    "distribution_queue_gate", "raw_target_stats", "policy_authority", "source_stats",
    "_snapshot_complete", "_snapshot_rows_exact", "_snapshot_content_exact"})
# Anchored four-node inventory distinguishes a positive absence from a partial
# response. The projection is the read-only form of SDK _active_for_manifest.
ACTIVE_SQL = (
    "SELECT nodes.endpoint AS endpoint,toString(coalesce(active.n,0)) AS active_count,"
    "active.snapshot_uuid AS snapshot_uuid,active.source_manifest_sha256 AS source_manifest_sha256 "
    "FROM (SELECT hostName() AS endpoint FROM clusterAllReplicas("
    "'statground_cluster','system','one')) AS nodes LEFT JOIN ("
    "SELECT hostName() AS endpoint,count() AS n,any(toString(snapshot_uuid)) AS snapshot_uuid,"
    "any(source_manifest_sha256) AS source_manifest_sha256 FROM clusterAllReplicas("
    "'statground_cluster','webr_book','v_book_serving_active_generation') GROUP BY endpoint"
    ") AS active USING endpoint ORDER BY endpoint SETTINGS skip_unavailable_shards=0,join_use_nulls=1"
)
ATTRS = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink",
         "st_size", "st_mtime_ns", "st_ctime_ns")


class Refused(RuntimeError):
    def __init__(self, reason):
        self.reason = reason if reason in REASONS else "observation_failed"
        super().__init__(self.reason)


def need(value, reason="invalid_input"):
    if not value:
        raise Refused(reason)


def pairs(items):
    result = {}
    for key, value in items:
        need(key not in result)
        result[key] = value
    return result


def loads(raw):
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda _x: (_ for _ in ()).throw(Refused("invalid_input")))


class Held:
    def __init__(self, path, *, private=False, maximum=1 << 20, expected=None):
        self.path, self.fd = Path(path), None
        need(self.path.is_absolute() and self.path.resolve() == self.path, "source_pin_or_custody")
        before = self.path.lstat()
        need(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid()
             and before.st_nlink == 1 and 0 < before.st_size <= maximum
             and (stat.S_IMODE(before.st_mode) == 0o600 if private else not before.st_mode & 0o022),
             "source_pin_or_custody")
        self.meta = tuple(getattr(before, key) for key in ATTRS)
        self.fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            self.raw = os.read(self.fd, maximum + 1)
            self.same()
            need(len(self.raw) == before.st_size and
                 (expected is None or hashlib.sha256(self.raw).hexdigest() == expected),
                 "source_pin_or_custody")
        except BaseException:
            self.close()
            raise

    def same(self):
        need(self.fd is not None and self.path.resolve() == self.path
             and self.meta == tuple(getattr(os.fstat(self.fd), key) for key in ATTRS)
             and self.meta == tuple(getattr(self.path.lstat(), key) for key in ATTRS),
             "source_pin_or_custody")

    def close(self):
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)


def module_from_held(held, name):
    value = types.ModuleType(name)
    value.__file__ = str(held.path)
    sys.modules[name] = value
    exec(compile(held.raw, str(held.path), "exec"), value.__dict__)
    value.json = types.SimpleNamespace(loads=loads, dumps=json.dumps, JSONDecodeError=json.JSONDecodeError)
    return value


class Window:
    """An observation stays inside its existing workflow wall-clock limit."""
    def __init__(self, seconds):
        now = time.monotonic()
        prior = signal.getitimer(signal.ITIMER_REAL)[0]
        self.end = min(now + seconds, now + prior) if prior else now + seconds
        self.cleanup_end = None
        self.requests = 0
        self.armed = False
        self.retryable = ()

    def check(self):
        need(time.monotonic() < (self.end if self.cleanup_end is None else self.cleanup_end),
             "budget_exhausted")

    def cleanup(self):
        if self.cleanup_end is None:
            self.cleanup_end = min(self.end, time.monotonic() + 9)
        return self.cleanup_end

    def arm(self, deadline):
        left = deadline - time.monotonic()
        need(left > 0, "budget_exhausted")
        if self.armed:
            signal.setitimer(signal.ITIMER_REAL, left)

    def failure(self):
        self.arm(self.cleanup())

    def request_error(self, error):
        if not isinstance(error, self.retryable):
            self.failure()

    @contextmanager
    def wire(self, seconds, retryable=()):
        self.check()
        now = time.monotonic()
        need(self.cleanup_end is None and self.end - now >= seconds + 9, "budget_exhausted")
        self.arm(min(self.end - 9, now + seconds))
        previous, self.retryable = self.retryable, retryable
        try:
            yield
        except BaseException as error:
            self.request_error(error)
            raise
        finally:
            self.retryable = previous
            self.arm(self.cleanup_end if self.cleanup_end is not None else self.end - 9)


class CompleteHTTPResponse(http.client.HTTPResponse):
    """Reject prefix-only JSON and incomplete streamed chunk terminators.

    This is a helper-local adaptation of the reviewed native snapshot framing
    guard. SDK query, response, line and row limits remain unchanged.
    """
    window = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.complete_chunk = False
        self.consumed = 0
        self.declared = None

    def failed(self):
        if self.window is not None:
            self.window.request_error(sys.exc_info()[1])

    def begin(self):
        try:
            super().begin()
            headers = self.getheaders()
            lengths = [value.strip(" \t") for key, value in headers if key.lower() == "content-length"]
            transfers = [value.strip(" \t").lower() for key, value in headers if key.lower() == "transfer-encoding"]
            errors = [value.strip(" \t") for key, value in headers if key.lower() == "x-clickhouse-exception-code"]
            need(len(errors) <= 1 and all(value in ("", "0") for value in errors), "readonly_request_refused")
            need(not any(key.lower() == "trailer" for key, _value in headers), "readonly_request_refused")
            if transfers:
                need(transfers == ["chunked"] and not lengths and self.chunked is True,
                     "readonly_request_refused")
            else:
                need(len(lengths) == 1 and re.fullmatch(r"[0-9]+", lengths[0])
                     and self.chunked is False and self.length == int(lengths[0]), "readonly_request_refused")
                self.declared = int(lengths[0])
        except BaseException:
            self.failed()
            raise

    def _get_chunk_left(self):
        remaining = self.chunk_left
        if not remaining:
            if remaining is not None:
                need(self._safe_read(2) == b"\r\n", "readonly_request_refused")
            line = self.fp.readline(http.client._MAXLINE + 1)
            need(0 < len(line) <= http.client._MAXLINE
                 and re.fullmatch(rb"[0-9a-fA-F]+\r\n", line), "readonly_request_refused")
            remaining = int(line[:-2], 16)
            if remaining == 0:
                need(self.fp.readline(http.client._MAXLINE + 1) == b"\r\n", "readonly_request_refused")
                self.complete_chunk = True
                self._close_conn()
                remaining = None
            self.chunk_left = remaining
        return remaining

    def verify_end(self):
        if self.chunked:
            need(self.isclosed() and self.complete_chunk and self.chunk_left is None, "readonly_request_refused")
        else:
            need(self.declared is not None and self.consumed == self.declared and self.length == 0,
                 "readonly_request_refused")

    def read(self, amt=None):
        try:
            remaining = self.length
            requested = (remaining if amt is None else min(amt, remaining)) if type(remaining) is int else None
            raw = super().read(amt)
            self.consumed += len(raw)
            need(not (requested is not None and len(raw) < requested and self.length > 0),
                 "readonly_request_refused")
            if self.length == 0 or self.isclosed() or not raw and requested != 0:
                self.verify_end()
            return raw
        except BaseException:
            self.failed()
            raise

    def readline(self, limit=-1):
        try:
            # Chunked BufferedIOBase.readline uses this class's read method.
            if self.chunked:
                return super().readline(limit)
            raw = super().readline(limit)
            self.consumed += len(raw)
            if not raw or self.isclosed():
                self.verify_end()
            return raw
        except BaseException:
            self.failed()
            raise


def complete_http(sdk, window):
    original = sdk.http.client
    class Response(CompleteHTTPResponse):
        pass
    Response.window = window
    def connection(base):
        class Connection(base):
            response_class = Response
            def request(self, *args, **kwargs):
                try:
                    return super().request(*args, **kwargs)
                except BaseException as error:
                    window.request_error(error)
                    raise
            def getresponse(self):
                try:
                    return super().getresponse()
                except BaseException as error:
                    window.request_error(error)
                    raise
        return Connection
    sdk.http = types.SimpleNamespace(client=types.SimpleNamespace(
        HTTPConnection=connection(original.HTTPConnection), HTTPSConnection=connection(original.HTTPSConnection),
        HTTPResponse=Response, HTTPException=original.HTTPException))


def readonly_client_class(sdk, held, window, legacy_sql=()):
    """Only pinned source call sites or the fixed active/legacy SELECTs reach HTTP.

    The SDK still owns transport, TLS, response/stream bounds and its original
    stream retry protocol. Its constructor is replaced too, so metadata and
    outbox clients cannot silently escape readonly=2.
    """
    original = sdk.ClickHouseHTTP
    source = sdk.__file__
    allowed_legacy = frozenset(item.rstrip().rstrip(";") for item in legacy_sql)

    def same():
        window.check()
        for item in held:
            item.same()

    def allowed(sql):
        need(type(sql) is str and ";" not in sql and "/*" not in sql and "--" not in sql
             and not re.search(r"\bgenerateSerialID\s*\(", sql, re.I), "readonly_request_refused")
        literal = sql.removesuffix(" FORMAT JSONEachRow")
        if literal == ACTIVE_SQL or literal in allowed_legacy:
            need(re.match(r"\s*(SELECT|WITH)\b", sql, re.I), "readonly_request_refused")
            return
        frame = inspect.currentframe().f_back
        while frame is not None:
            if frame.f_code.co_filename == source and frame.f_code.co_name in SDK_READ_CALLERS:
                need(re.match(r"\s*(SELECT|WITH|CHECK\s+GRANT)\b", sql, re.I), "readonly_request_refused")
                return
            frame = frame.f_back
        raise Refused("readonly_request_refused")

    class ReadOnly(original):
        def __init__(self, endpoint, timeout=45.0, *, read_only=True):
            need(read_only is True and 0 < timeout <= sdk.COORDINATOR_TIMEOUT_SECONDS,
                 "readonly_request_refused")
            super().__init__(endpoint, timeout=timeout, read_only=True)

        def rows(self, sql, params=None, *, max_response_bytes=1 << 26):
            allowed(sql)
            try:
                rows = super().rows(sql, params, max_response_bytes=max_response_bytes)
            except BaseException:
                window.failure()
                raise
            frame = inspect.currentframe().f_back
            while frame is not None:
                if frame.f_code.co_filename == source and frame.f_code.co_name in {
                        "_snapshot_complete", "_snapshot_rows_exact", "_snapshot_content_exact"}:
                    need(len(rows) == 4 and {row.get("endpoint") for row in rows} ==
                         set(sdk.SOURCE_DDL_ENDPOINTS), "incomplete_inventory")
                    break
                frame = frame.f_back
            return rows

        def _query_parameters(self, params=None):
            query = super()._query_parameters(params)
            query.update({name: "throw" for name in OVERFLOW_MODES})
            return query

        def _request(self, sql, params=None, body=None, max_response_bytes=1 << 26):
            same()
            allowed(sql)
            need(body is None and self.read_only and self._query_parameters().get("readonly") == "2",
                 "readonly_request_refused")
            window.requests += 1
            need(window.requests <= 256, "budget_exhausted")
            try:
                with window.wire(self.timeout):
                    return super()._request(sql, params, body=None, max_response_bytes=max_response_bytes)
            finally:
                same()

        def stream_ordered_identity_content_sha256(self, sql, *args, **kwargs):
            same()
            allowed(sql)
            need(self.read_only, "readonly_request_refused")
            window.requests += 1
            need(window.requests <= 256, "budget_exhausted")
            try:
                with window.wire(self.timeout, (sdk.PublishError, OSError, sdk.http.client.HTTPException)):
                    return super().stream_ordered_identity_content_sha256(sql, *args, **kwargs)
            finally:
                same()

        def stream_ordered_identities(self, sql, *args, **kwargs):
            same()
            allowed(sql)
            need(self.read_only, "readonly_request_refused")
            window.requests += 1
            need(window.requests <= 256, "budget_exhausted")
            try:
                with window.wire(self.timeout):
                    return super().stream_ordered_identities(sql, *args, **kwargs)
            finally:
                same()

        def stream_endpoint_identity_content_sha256(self, *_args, **_kwargs):
            raise Refused("readonly_request_refused")

        def exec(self, *_args, **_kwargs):
            raise Refused("readonly_request_refused")

        def insert_json(self, *_args, **_kwargs):
            raise Refused("readonly_request_refused")

    return ReadOnly


def preflight_receipt(sdk, started, completed, generation, stats, target):
    return {"status": "preflight_ok", "database_writes": False, "service": "webr",
        "source_refresh_started_at": sdk.format_time(started, sdk.KST),
        "source_refresh_success_at": sdk.format_time(completed, sdk.KST),
        "source_generation": sdk.format_time(generation, sdk.KST), "source_rows": stats["rows"],
        "raw_target_rows": target["rows"], "content_sha256": stats["content_sha256"],
        "policy_authority_revision": stats["policy_revision"]}


def expected_receipt(sdk, value):
    need(type(value) is dict and set(value) == PREFLIGHT_KEYS)
    need(value["status"] == "preflight_ok" and value["database_writes"] is False
         and value["service"] == "webr")
    for key in ("source_rows", "raw_target_rows", "policy_authority_revision"):
        need(type(value[key]) is int and 0 < value[key] < 1 << 64)
    need(type(value["content_sha256"]) is str and sdk.DIGEST.fullmatch(value["content_sha256"]))
    for key in ("source_refresh_started_at", "source_refresh_success_at", "source_generation"):
        need(type(value[key]) is str)
        sdk.parse_time(value[key])
    return value


def result(contract, status, reason, heartbeat=None, rows=None):
    need(reason in REASONS and status in {"complete", "pending", "refused", "compatibility_unknown"})
    positive = {"complete": "observed_complete", "pending": "awaiting_authoritative_host_publication",
                "compatibility_unknown": "legacy_contract_not_observed"}
    need(contract in {"public-v2", "legacy"})
    need((reason == positive[status] if status in positive else reason not in positive.values())
         and (status != "pending" or contract == "public-v2")
         and (status != "compatibility_unknown" or contract == "legacy"))
    return {"schema": SCHEMA, "contract": contract, "service": "webr", "status": status,
        "complete": status == "complete", "reason": reason, "observed_state": status != "refused",
        "wire_ack_proved": False, "host_request_sent": False,
        "source_manifest_sha256": None if heartbeat is None else heartbeat["source_manifest_sha256"],
        "content_sha256": None if heartbeat is None else heartbeat["content_sha256"], "rows": rows}


def active_inventory(sdk, client):
    rows = client.rows(ACTIVE_SQL)
    expected = set(sdk.SOURCE_DDL_ENDPOINTS)
    need(len(rows) == 4 and len(expected) == 4 and
         {x.get("endpoint") for x in rows} == expected, "incomplete_inventory")
    signatures = []
    for row in rows:
        need(set(row) == {"endpoint", "active_count", "snapshot_uuid", "source_manifest_sha256"},
             "incomplete_inventory")
        count = sdk.exact_uint(row["active_count"], "active count")
        need(count in (0, 1), "incomplete_inventory")
        if count == 0:
            need(row["snapshot_uuid"] in (None, "") and row["source_manifest_sha256"] in (None, ""),
                 "incomplete_inventory")
            signatures.append((0, None, None))
        else:
            need(type(row["snapshot_uuid"]) is str, "incomplete_inventory")
            snapshot = str(sdk.exact_uuid(row["snapshot_uuid"], "active snapshot"))
            manifest = row["source_manifest_sha256"]
            need(type(manifest) is str and sdk.DIGEST.fullmatch(manifest), "incomplete_inventory")
            signatures.append((1, snapshot, manifest))
    need(len(set(signatures)) == 1, "incomplete_inventory")
    return signatures[0]


def observe_public(sdk, config, expected=None, *, preflight_only=False):
    service = sdk.SERVICES["webr"]
    client = sdk.ClickHouseHTTP(config.coordinator, timeout=sdk.COORDINATOR_TIMEOUT_SECONDS, read_only=True)
    publisher = sdk.Publisher(client, config, service)
    # The pinned readonly branch also returns the exact refresh counters needed
    # for its original assert_publication_inputs after the observation.
    started, completed, generation, stats, target, refresh = publisher._preflight(read_only=True)
    receipt = preflight_receipt(sdk, started, completed, generation, stats, target)
    if preflight_only:
        return receipt
    need(receipt == expected_receipt(sdk, expected), "expected_identity_mismatch")
    heartbeat = publisher.heartbeat(started, completed, generation, stats)
    before = active_inventory(sdk, client)
    matching = before == (1, heartbeat["snapshot_uuid"], heartbeat["source_manifest_sha256"])
    if matching:
        need(publisher._snapshot_complete(heartbeat), "incomplete_inventory")
        need(publisher._snapshot_rows_exact(heartbeat), "incomplete_inventory")
    elif before[0] and before[2] == heartbeat["source_manifest_sha256"]:
        raise Refused("expected_identity_mismatch")
    publisher.assert_publication_inputs(refresh, generation, stats, target, "host-handoff-post")
    need(active_inventory(sdk, client) == before, "authority_changed")
    return result("public-v2", "complete" if matching else "pending",
        "observed_complete" if matching else "awaiting_authoritative_host_publication",
        heartbeat, stats["rows"])


def observe_legacy(sdk, legacy, client):
    """Keep old marker/pointer semantics; no public-v2 state is consulted."""
    publisher = legacy.Publisher(client)
    publisher.validate_health()
    refresh = publisher.refresh_evidence()
    generation = publisher.select_generation(refresh)
    signature = publisher.validate_generation(refresh, generation)
    payload = legacy.marker_payload(refresh, signature, refresh.last_success_time)
    marker_present = publisher.verify_marker(payload)
    if marker_present:
        publisher.verify_pointer(payload)
    need(publisher.validate_generation(refresh, generation) == signature, "authority_changed")
    if marker_present:
        need(publisher.verify_marker(payload), "authority_changed")
        publisher.verify_pointer(payload)
    publisher.validate_health()
    need(publisher.refresh_evidence() == refresh, "authority_changed")
    # Old27cb has no absolute-age guard; retain the public13h limit rather than
    # letting an old compatible marker become a fresh successful observation.
    success = refresh.last_success_time.replace(tzinfo=sdk.KST)
    now = dt.datetime.now(sdk.KST)
    need(dt.timedelta(0) <= now - success <= dt.timedelta(hours=13), "source_not_ready")
    if not marker_present:
        return result("legacy", "compatibility_unknown", "legacy_contract_not_observed", rows=signature.row_count)
    return result("legacy", "complete", "observed_complete", rows=signature.row_count)


def legacy_inventory(sdk, legacy, sql, rows):
    if sql == legacy.CANDIDATE_GENERATIONS_SQL:
        need(len(rows) == 1, "incomplete_inventory")
    if sql == legacy.MARKER_ROWS_SQL and rows:
        need(len(rows) == 4 and {r.get("endpoint") for r in rows} == set(sdk.SOURCE_DDL_ENDPOINTS),
             "incomplete_inventory")
    if sql == legacy.REPLICA_HEALTH_SQL:
        need(len(rows) == 8, "incomplete_inventory")
        for table in ("naver_r_book_catalog_local", "naver_r_book_catalog_generation_local"):
            group = [row for row in rows if row.get("table") == table]
            need(len(group) == 4 and {row.get("endpoint") for row in group} == set(sdk.SOURCE_DDL_ENDPOINTS),
                 "incomplete_inventory")
    return rows


def legacy_adapter(sdk, legacy, config):
    constants = tuple(getattr(legacy, name) for name in (
        "REFRESH_STATE_SQL", "REPLICA_HEALTH_SQL", "CANDIDATE_GENERATIONS_SQL",
        "GLOBAL_SIGNATURE_SQL", "PHYSICAL_SIGNATURE_SQL", "MARKER_ROWS_SQL", "POINTER_SQL"))
    http = sdk.ClickHouseHTTP(sdk.Endpoint(config.endpoint, config.user, config.password,
        None if config.ca_file is None else str(config.ca_file)), timeout=120, read_only=True)

    class Adapter:
        def query_json(self, sql, params=None):
            need(sql in constants, "readonly_request_refused")
            rows = http.rows(sql, params, max_response_bytes=legacy.HTTP_RESPONSE_MAX_BYTES)
            return legacy_inventory(sdk, legacy, sql, rows)
    return Adapter(), constants


def run(args):
    held = []
    old_handler, old_timer = signal.getsignal(signal.SIGALRM), signal.getitimer(signal.ITIMER_REAL)
    window = Window(3600 if args.contract == "public-v2" else 600)
    def expired(_signum, _frame):
        raise Refused("budget_exhausted")
    signal.signal(signal.SIGALRM, expired)
    window.armed = True
    try:
        window.arm(window.end - 9)
        source = Held(args.sql_source_root.resolve() / RELATIVE / SDK_NAME, expected=SDK_SHA256)
        held.append(source)
        sdk = module_from_held(source, "book_host_readonly_sdk")
        complete_http(sdk, window)
        config_file = Held(args.config, private=True, maximum=131072)
        held.append(config_file)
        if args.contract == "public-v2":
            need(not args.legacy_source_root)
            config = sdk.load_config(args.config, sdk.SERVICES["webr"])
            for ca in {item.ca_file for item in (config.coordinator, *(x.client for x in config.outboxes)) if item.ca_file}:
                held.append(Held(Path(ca), maximum=262144))
            sdk.ClickHouseHTTP = readonly_client_class(sdk, held, window)
            expected = None
            if not args.preflight_only:
                need(args.expected_json is not None)
                item = Held(args.expected_json, private=True, maximum=65536)
                held.append(item)
                expected = loads(item.raw)
            else:
                need(args.expected_json is None)
            value = observe_public(sdk, config, expected, preflight_only=args.preflight_only)
        else:
            need(not args.preflight_only and args.expected_json is None and args.legacy_source_root is not None)
            item = Held(args.legacy_source_root.resolve() / RELATIVE / LEGACY_NAME, expected=LEGACY_SHA256)
            held.append(item)
            legacy = module_from_held(item, "book_host_legacy_protocol")
            config = legacy.load_http_config(args.config)
            need(config.user == "webr_book_generation_publisher" and config.verify_tls,
                 "invalid_input")
            if config.ca_file:
                held.append(Held(config.ca_file, maximum=262144))
            constants = tuple(getattr(legacy, name) for name in (
                "REFRESH_STATE_SQL", "REPLICA_HEALTH_SQL", "CANDIDATE_GENERATIONS_SQL",
                "GLOBAL_SIGNATURE_SQL", "PHYSICAL_SIGNATURE_SQL", "MARKER_ROWS_SQL", "POINTER_SQL"))
            sdk.ClickHouseHTTP = readonly_client_class(sdk, held, window, constants)
            client, _ = legacy_adapter(sdk, legacy, config)
            value = observe_legacy(sdk, legacy, client)
        for item in held:
            item.same()
        return value
    finally:
        cutoff = window.cleanup()
        try:
            window.arm(cutoff)
            error = None
            for item in reversed(held):
                window.check()
                try:
                    item.close()
                except OSError as caught:
                    error = error or caught
            if error is not None:
                raise Refused("source_pin_or_custody")
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)
            if old_timer[0]:
                remaining = window.end - time.monotonic()
                if remaining > 0:
                    signal.setitimer(signal.ITIMER_REAL, remaining, old_timer[1])


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", required=True, choices=("public-v2", "legacy"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--sql-source-root", type=Path, required=True)
    parser.add_argument("--legacy-source-root", type=Path)
    parser.add_argument("--expected-json", type=Path)
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args(argv)
    try:
        value = run(args)
    except Refused as error:
        value = result(args.contract, "refused", error.reason)
    except Exception:
        value = result(args.contract, "refused", "observation_failed")
    print(json.dumps(value, sort_keys=True, separators=(",", ":")))
    if args.preflight_only and value.get("status") == "preflight_ok":
        return 0
    return 0 if value["complete"] else 1 if value["status"] == "refused" else 2


if __name__ == "__main__":
    raise SystemExit(main())
