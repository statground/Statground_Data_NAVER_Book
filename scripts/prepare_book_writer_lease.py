#!/usr/bin/env python3
"""Prepare pinned, private Book lease runtime without database access."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

SQL_COMMIT = "66e604d1fc2befcc7753191219c98940dc548790"
HELPER_SHA256 = "6d3f1368061f7b00091de96ec8bd2083aa84b953eb1d31581727c366788057a4"
HELPER_RELATIVE = "docker-compose/50005_Clickhouse_Statground/book_shared_writer_lease.py"

class PreparationError(Exception):
    pass

def require(condition):
    if not condition:
        raise PreparationError("Book writer lease runtime configuration rejected")

def write_private(path, data, mode=0o600):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())

def prepare(sql_root, runtime, python, config_bytes, ca_bytes, password):
    require(runtime.is_absolute() and python.is_absolute() and "\n" not in str(runtime) and "\r" not in str(runtime))
    require(bool(password) and 0 < len(config_bytes) <= 65536)
    helper = sql_root / HELPER_RELATIVE
    require(helper.is_file() and not helper.is_symlink())
    source = helper.read_bytes()
    require(hashlib.sha256(source).hexdigest() == HELPER_SHA256)
    try:
        config = json.loads(config_bytes)
    except (ValueError, TypeError):
        raise PreparationError("Book writer lease runtime configuration rejected") from None
    require(isinstance(config, dict) and config.get("password_env") == "BOOK_WRITER_LEASE_TIDB_PASSWORD")
    ca_path = runtime / "ca.pem"
    if config.get("tls_ca_file"):
        require(config["tls_ca_file"] == str(ca_path) and 0 < len(ca_bytes) <= 1048576)
    else:
        require(not ca_bytes and config.get("allow_private_tcp") is True)
    runtime.mkdir(mode=0o700)
    require(stat.S_IMODE(runtime.stat().st_mode) == 0o700 and not runtime.is_symlink())
    write_private(runtime / "helper.py", source, 0o700)
    write_private(runtime / "config.json", config_bytes)
    if ca_bytes:
        write_private(ca_path, ca_bytes)
    # Execute the same verified source bytes. The wrapper pins the SQL helper
    # at every call and uses the isolated, hash-locked Python environment.
    wrapper = ("#!" + str(python) + "\n" +
        "import hashlib, pathlib, sys\n" +
        "path = pathlib.Path(__file__).with_name('helper.py')\n" +
        "source = path.read_bytes()\n" +
        "if hashlib.sha256(source).hexdigest() != '" + HELPER_SHA256 + "':\n" +
        "    raise SystemExit('Book writer lease helper bytes rejected')\n" +
        "sys.argv[0] = str(path)\n" +
        "exec(compile(source, str(path), 'exec'), {'__name__': '__main__', '__file__': str(path)})\n").encode()
    write_private(runtime / "lease-helper", wrapper, 0o700)
    fd = os.open(runtime, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return runtime / "lease-helper", runtime / "config.json"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-root", required=True, type=Path)
    parser.add_argument("--runtime", required=True, type=Path)
    parser.add_argument("--python", required=True, type=Path)
    args = parser.parse_args()
    try:
        helper, config = prepare(args.sql_root, args.runtime, args.python,
            os.environ.get("BOOK_WRITER_LEASE_CONFIG_JSON", "").encode(),
            os.environ.get("BOOK_WRITER_LEASE_CA_PEM", "").encode(),
            os.environ.get("BOOK_WRITER_LEASE_TIDB_PASSWORD", ""))
        with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as stream:
            stream.write(f"BOOK_WRITER_LEASE_HELPER={helper}\nBOOK_WRITER_LEASE_CONFIG={config}\n")
    except (PreparationError, OSError, KeyError):
        print("Book writer lease runtime preparation failed", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
