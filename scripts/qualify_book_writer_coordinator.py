#!/usr/bin/env python3
"""Manual, read-only route qualification; never grants Book writer admission."""
import argparse
import contextlib
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import ssl
import stat
import sys
import time
import types
import uuid

import prepare_book_writer_lease as preparation

IDENTITY_SQL = (
    "SELECT /*+ MAX_EXECUTION_TIME(3000) */ VERSION() AS server_version,"
    "CURRENT_USER() AS effective_user,DATABASE() AS selected_database,"
    "CAST(@@GLOBAL.require_secure_transport AS UNSIGNED) AS secure_transport"
)
CONTROL_SQL = (
    "SELECT /*+ MAX_EXECUTION_TIME(3000) */ enabled,control_epoch,activation_revision,"
    "coverage_sha256,(holder='' AND active_operation='' AND lease_expires_at IS NULL) AS idle "
    "FROM `user`.`book_shared_writer_lease` WHERE resource=%s LIMIT 2"
)
PENDING_SQL = (
    "SELECT /*+ MAX_EXECUTION_TIME(3000) */ COUNT(*) AS pending "
    "FROM `user`.`book_shared_writer_operation` WHERE state='pending'"
)
SQL = (IDENTITY_SQL, CONTROL_SQL, PENDING_SQL)
ATTRS = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink",
         "st_size", "st_mtime_ns", "st_ctime_ns")


class Refused(Exception):
    pass


class Deadline(BaseException):
    pass


def need(condition):
    if not condition:
        raise Refused("readonly_qualification_refused")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(info):
    return tuple(getattr(info, key) for key in ATTRS)


class HeldFile:
    def __init__(self, path, maximum, *, mode=0o600):
        self.path = Path(path)
        self.fd = None
        try:
            self.fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            info = os.fstat(self.fd)
            need(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                 and stat.S_IMODE(info.st_mode) == mode and info.st_nlink == 1
                 and 0 < info.st_size <= maximum)
            self.pin = identity(info)
            self.raw = os.pread(self.fd, maximum + 1, 0)
            need(len(self.raw) == info.st_size)
            self.sha256 = digest(self.raw)
            self.same()
        except BaseException:
            self.close()
            raise

    def same(self):
        need(self.fd is not None and identity(os.fstat(self.fd)) == self.pin
             and identity(self.path.lstat()) == self.pin
             and os.pread(self.fd, len(self.raw) + 1, 0) == self.raw)

    def close(self):
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)


class Budget:
    """One35-second client lifetime, connect/three SELECTs5 each, cleanup9."""
    def __init__(self):
        need(signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0))
        self.clock = time.monotonic
        self.started = self.clock()
        self.end = self.started + 35
        self.cleanup_end = self.end
        self.failure = None
        self.used = set()
        self.old_handler = signal.getsignal(signal.SIGALRM)
        self.handler = self.timeout
        signal.signal(signal.SIGALRM, self.handler)
        self.arm(self.end - 9)

    def timeout(self, *_):
        raise Deadline()

    def arm(self, end):
        need(signal.getsignal(signal.SIGALRM) is self.handler)
        left = end - self.clock()
        if left <= 0:
            raise Deadline()
        signal.setitimer(signal.ITIMER_REAL, left)

    def check(self):
        need(self.failure is None and self.clock() < self.end - 9)

    def fail(self, error):
        if self.failure is None:
            self.failure = type(error).__name__
            self.cleanup_end = min(self.end, self.clock() + 9)
        self.arm(self.cleanup_end)

    @contextlib.contextmanager
    def call(self, ordinal):
        self.check()
        need(type(ordinal) is int and ordinal in range(4) and ordinal not in self.used
             and self.used == set(range(ordinal))
             and self.clock() + (4 - ordinal) * 5 + 9 <= self.end)
        self.used.add(ordinal)
        cutoff = min(self.clock() + 5, self.end - 9)
        self.arm(cutoff)
        try:
            yield
            self.check()
            need(self.clock() < cutoff)
        except BaseException as error:
            self.fail(error)
            raise
        finally:
            self.arm(self.cleanup_end if self.failure is not None else self.end - 9)

    def cleanup(self):
        self.cleanup_end = min(self.cleanup_end, self.end, self.clock() + 9)
        self.arm(self.cleanup_end)

    def close(self):
        need(signal.getsignal(signal.SIGALRM) is self.handler and self.clock() < self.cleanup_end)
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, self.old_handler)


def validate_request(request_id, expected_source_config_sha256, source):
    need(type(request_id) is str and str(uuid.UUID(request_id)) == request_id
         and uuid.UUID(request_id).int != 0)
    need(type(expected_source_config_sha256) is str
         and re.fullmatch("[0-9a-f]{64}", expected_source_config_sha256)
         and type(source) is bytes and 0 < len(source) <= 65536
         and digest(source) == expected_source_config_sha256)


def validate_dedicated(config, runtime):
    address = ipaddress.IPv4Address(config.host)
    need(address.is_global and str(address) == config.host and config.port == 51002
         and re.fullmatch("book_writer_[0-9a-f]{16}", config.user)
         and config.password_env == "BOOK_WRITER_LEASE_TIDB_PASSWORD"
         and config.tls_ca_file == str(runtime / "ca.pem")
         and config.allow_private_tcp is False and config.control_epoch != "00000000-0000-0000-0000-000000000000"
         and config.activation_revision > 0 and re.fullmatch("[0-9a-f]{64}", config.coverage_sha256))


def validate_tls(connection, host):
    wire = connection._sock
    need(isinstance(wire, ssl.SSLSocket) and wire.context.verify_mode == ssl.CERT_REQUIRED
         and wire.context.check_hostname is True and wire.server_hostname == host
         and wire.getpeername() == (host, 51002)
         and wire.getpeercert() and wire.cipher() and wire.version() in ("TLSv1.2", "TLSv1.3"))


def select_one(connection, sql, params, columns):
    need(sql in SQL)
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        need(cursor.description and tuple(row[0] for row in cursor.description) == columns)
        rows = []
        size = 0
        for _ in range(33):
            row = cursor.fetchone()
            if row is None:
                break
            need(type(row) is dict and set(row) == set(columns)
                 and all(value is None or type(value) in (str, int) for value in row.values()))
            size += len(json.dumps(row, ensure_ascii=True, allow_nan=False).encode())
            need(size <= 65536)
            rows.append(row)
        else:
            need(False)
    need(len(rows) == 1)
    return rows[0]


def validate_identity(row, config):
    need(type(row["server_version"]) is str
         and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+-TiDB-v8\.5\.6(?:[-+][A-Za-z0-9_.-]+)?", row["server_version"])
         and type(row["effective_user"]) is str
         and row["effective_user"] == config.user + "@%"
         and row["selected_database"] == "user"
         and type(row["secure_transport"]) is int and row["secure_transport"] == 1)


def validate_control(row, config):
    need(type(row["enabled"]) is int and row["enabled"] == 1
         and row["control_epoch"] == config.control_epoch
         and type(row["activation_revision"]) is int
         and row["activation_revision"] == config.activation_revision
         and row["coverage_sha256"] == config.coverage_sha256
         and type(row["idle"]) is int and row["idle"] == 1)


def fixed():
    return {"schema": "statground.book-coordinator-readonly-qualification.v1",
            "status": "default_off", "stage": "default_off", "request_id": None,
            "source_config_matches": False, "runtime_config_matches": False,
            "tls_verified": False, "runtime_identity_matches": False,
            "activation_matches_idle": False, "pending_absent": False,
            "connection_attempts": 0, "SELECT_attempts": 0, "connection_closed": None,
            "lease_operations": 0, "DB_writes": 0, "provider_requests": 0,
            "publication_attempts": 0, "physical_durability_proved": False,
            "actor_coverage_proved": False, "Book_admission": False}


def run(execute, request_id, expected_source_config_sha256):
    result = fixed()
    if execute is not True:
        return result, None
    budget = Budget()
    result.update(status="blocked", stage="source")
    connection, files, checks_complete = None, [], False
    try:
        source = os.environ.get("BOOK_WRITER_LEASE_CONFIG_JSON", "").encode()
        validate_request(request_id, expected_source_config_sha256, source)
        result.update(request_id=request_id, source_config_matches=True)
        runner_temp = Path(os.environ["RUNNER_TEMP"])
        need(os.environ.get("GITHUB_ACTIONS") == "true" and runner_temp.is_absolute())
        runtime = runner_temp / "book-writer-lease"
        need(runtime.resolve(strict=True) == runtime and stat.S_IMODE(runtime.stat().st_mode) == 0o700)
        helper = HeldFile(runtime / "helper.py", 65536, mode=0o700); files.append(helper)
        config_file = HeldFile(runtime / "config.json", 65536); files.append(config_file)
        provenance_file = HeldFile(runtime / "config-provenance.json", 4096); files.append(provenance_file)
        ca = HeldFile(runtime / "ca.pem", 1048576); files.append(ca)
        need(helper.sha256 == preparation.HELPER_SHA256)
        provenance = json.loads(provenance_file.raw)
        expected_runtime, expected_provenance = preparation.resolve_runner_ca_template(
            source, json.loads(source), runtime)
        need(type(provenance) is dict and set(provenance) == {
            "schema", "ca_template", "source_config_sha256", "runtime_config_sha256"}
            and provenance["schema"] == preparation.PROVENANCE_SCHEMA
            and provenance["ca_template"] == preparation.RUNNER_CA_TEMPLATE
            and provenance["source_config_sha256"] == expected_source_config_sha256
            and provenance["runtime_config_sha256"] == config_file.sha256
            and provenance == expected_provenance and config_file.raw == expected_runtime
            and ca.raw == os.environ.get("BOOK_WRITER_LEASE_CA_PEM", "").encode())
        name = "_qualified_book_readonly_sql_helper"
        need(name not in sys.modules)
        module = types.ModuleType(name); module.__file__ = str(helper.path)
        sys.modules[name] = module
        try:
            exec(compile(helper.raw, str(helper.path), "exec", dont_inherit=True), module.__dict__)
            config = module.load_config(config_file.path, expected_sha256=config_file.sha256)
            result["stage"] = "config"
            validate_dedicated(config, runtime)
            result["runtime_config_matches"] = True
            for file in files: file.same()
            result["stage"] = "connect"
            with budget.call(0):
                result["connection_attempts"] += 1
                connection = module.connect(config)
                validate_tls(connection, config.host)
                result["tls_verified"] = True
            requests = (
                (IDENTITY_SQL, (), ("server_version", "effective_user", "selected_database", "secure_transport"), "identity"),
                (CONTROL_SQL, (module.RESOURCE,), ("enabled", "control_epoch", "activation_revision", "coverage_sha256", "idle"), "control"),
                (PENDING_SQL, (), ("pending",), "pending"),
            )
            for ordinal, (sql, params, columns, stage) in enumerate(requests, 1):
                result["stage"] = stage
                for file in files: file.same()
                with budget.call(ordinal):
                    result["SELECT_attempts"] += 1
                    row = select_one(connection, sql, params, columns)
                    if stage == "identity":
                        validate_identity(row, config); result["runtime_identity_matches"] = True
                    elif stage == "control":
                        validate_control(row, config); result["activation_matches_idle"] = True
                    else:
                        need(type(row["pending"]) is int and row["pending"] == 0)
                        result["pending_absent"] = True
                for file in files: file.same()
            checks_complete = True
        finally:
            sys.modules.pop(name, None)
    except BaseException as error:
        budget.fail(error)
    finally:
        if checks_complete:
            result["stage"] = "cleanup"
        budget.cleanup()
        if connection is not None:
            try:
                connection.close(); result["connection_closed"] = True
            except BaseException as error:
                budget.fail(error); result["connection_closed"] = False; result["status"] = "blocked"
        for file in reversed(files):
            try:
                file.close()
            except BaseException as error:
                budget.fail(error); result["status"] = "blocked"
        budget.arm(budget.cleanup_end)
    if checks_complete and budget.failure is None and result["connection_closed"] is True:
        result.update(status="qualified_readonly_nonadmitting", stage="complete")
    return result, budget


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--request-id", default="")
    parser.add_argument("--expected-source-config-sha256", default="")
    args = parser.parse_args(argv)
    budget = None
    result = fixed()
    try:
        result, budget = run(args.execute, args.request_id, args.expected_source_config_sha256)
        raw = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
        need(len(raw) <= 4096 and os.write(1, raw) == len(raw))
        return 0 if result["status"] in ("default_off", "qualified_readonly_nonadmitting") else 2
    finally:
        if budget is not None:
            budget.close()


if __name__ == "__main__":
    raise SystemExit(main())
