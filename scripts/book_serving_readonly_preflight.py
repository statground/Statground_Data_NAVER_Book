#!/usr/bin/env python3
"""Run one pinned Book serving read-only preflight without logging credentials."""

import argparse
import datetime as dt
import json
import os
import re
import stat
import subprocess
import sys
import uuid
from pathlib import Path


USERS = {
    "webr": "webr_book_serving_publisher",
    "mirtype": "mirtype_book_serving_publisher",
    "statground-provider": "statground_provider_book_serving_publisher",
}
REQUIRED_ENV = (
    "RUNNER_TEMP", "GITHUB_REPOSITORY", "GITHUB_EVENT_NAME",
    "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT",
    "COORDINATOR_ENDPOINT", "PUBLISHER_PASSWORD", "NAVER_OUTBOX_ENDPOINT",
    "KAKAO_OUTBOX_ENDPOINT", "OUTBOX_OBSERVER_PASSWORD",
)
RECEIPT_KEYS = {
    "status", "database_writes", "service", "read_only_preflight",
    "replica_sync_requested", "source_refresh_started_at",
    "source_refresh_success_at", "source_generation", "source_rows",
    "raw_target_rows", "content_sha256", "policy_authority_revision",
}
TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}")
DIGEST = re.compile(r"[0-9a-f]{64}")


def fail(reason):
    raise SystemExit("Book serving read-only preflight rejected: " + reason)


def positive_uint(value):
    return type(value) is int and 0 < value < (1 << 64)


def exact_time(value):
    if not isinstance(value, str) or TIME.fullmatch(value) is None:
        fail("invalid refresh timestamp")
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        fail("invalid refresh timestamp")


def validate_receipt(value, service):
    if not isinstance(value, dict) or set(value) != RECEIPT_KEYS:
        fail("receipt fields are not exact")
    if (value["status"] != "preflight_ok"
            or value["database_writes"] is not False
            or value["read_only_preflight"] is not True
            or value["replica_sync_requested"] is not False
            or value["service"] != service):
        fail("receipt mode or service is invalid")
    if not all(positive_uint(value[key]) for key in (
            "source_rows", "raw_target_rows", "policy_authority_revision")):
        fail("receipt count or authority revision is invalid")
    if not isinstance(value["content_sha256"], str) or DIGEST.fullmatch(value["content_sha256"]) is None:
        fail("receipt digest is invalid")
    started = exact_time(value["source_refresh_started_at"])
    succeeded = exact_time(value["source_refresh_success_at"])
    generation = exact_time(value["source_generation"])
    if started > succeeded or generation < started or generation >= succeeded + dt.timedelta(seconds=1):
        fail("refresh and generation timestamps are inconsistent")


def private_file(path, content):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", choices=tuple(USERS), required=True)
    parser.add_argument("--publisher", type=Path, required=True)
    args = parser.parse_args()

    if any(not os.environ.get(name) for name in REQUIRED_ENV):
        fail("required runner setting or credential is missing")
    if (os.environ["GITHUB_REPOSITORY"] != "statground/Statground_Data_NAVER_Book"
            or os.environ["GITHUB_EVENT_NAME"] != "workflow_dispatch"):
        fail("run identity is invalid")
    if os.environ.get("PUBLISHER_USER") != USERS[args.service]:
        fail("selected profile principal is invalid")
    if not args.publisher.is_absolute():
        fail("publisher path is not absolute")
    try:
        info = args.publisher.lstat()
    except OSError:
        fail("pinned publisher is unavailable")
    if not stat.S_ISREG(info.st_mode):
        fail("pinned publisher is not a regular file")

    runner_temp = Path(os.environ["RUNNER_TEMP"])
    if not runner_temp.is_absolute() or not re.fullmatch(r"[0-9]+", os.environ["GITHUB_RUN_ID"]):
        fail("runner identity is invalid")
    if not re.fullmatch(r"[0-9]+", os.environ["GITHUB_RUN_ATTEMPT"]):
        fail("runner identity is invalid")
    os.umask(0o077)
    prefix = runner_temp / (
        f"book-read-only-{os.environ['GITHUB_RUN_ID']}-"
        f"{os.environ['GITHUB_RUN_ATTEMPT']}-{args.service}"
    )
    config_path = prefix.with_suffix(".json")
    ca_path = prefix.with_suffix(".ca.pem")
    ca_pem = os.environ.get("BOOK_SERVING_CA_PEM", "")
    config_created = False
    ca_created = False
    try:
        if ca_pem:
            private_file(ca_path, ca_pem)
            ca_created = True
        run_identity = ":".join((
            os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"],
            os.environ["GITHUB_RUN_ATTEMPT"],
        ))
        ca_file = str(ca_path) if ca_pem else None
        config = {
            "coordinator": {
                "endpoint": os.environ["COORDINATOR_ENDPOINT"],
                "user": USERS[args.service],
                "password": os.environ["PUBLISHER_PASSWORD"],
                "ca_file": ca_file,
            },
            "coordinator_host": "clickhouse-s1-r1",
            "holder_uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, run_identity)),
            "capacity_gate": {
                "min_free_bytes": 21474836480,
                "min_free_ratio": 0.15,
                "copy_multiplier": 3,
            },
            "outbox_endpoints": [
                {
                    "name": name,
                    "object": object_name,
                    "connection": {
                        "endpoint": endpoint,
                        "user": "book_serving_outbox_observer",
                        "password": os.environ["OUTBOX_OBSERVER_PASSWORD"],
                        "ca_file": ca_file,
                    },
                }
                for name, object_name, endpoint in (
                    ("naver-producer", "Data_Book_NAVER_Log.naver_direct_insert_outbox",
                     os.environ["NAVER_OUTBOX_ENDPOINT"]),
                    ("kakao-producer", "Data_Book_KAKAO_Log.kakao_direct_insert_outbox",
                     os.environ["KAKAO_OUTBOX_ENDPOINT"]),
                )
            ],
        }
        private_file(config_path, json.dumps(config, ensure_ascii=True, sort_keys=True) + "\n")
        config_created = True
        completed = subprocess.run(
            [sys.executable, str(args.publisher), "--config", str(config_path),
             "--service", args.service, "--read-only-preflight"],
            check=False, capture_output=True, text=True,
            env={"PYTHONUNBUFFERED": "1"},
        )
        if completed.returncode != 0:
            fail(f"{args.service}: preflight_blocked")
        if completed.stderr:
            fail(f"{args.service}: unexpected_publisher_stderr")
        if len(completed.stdout.encode("utf-8")) > 16384:
            fail("publisher receipt is too large")
        lines = completed.stdout.splitlines()
        if len(lines) != 1:
            fail("publisher did not emit one receipt")
        try:
            receipt = json.loads(lines[0])
        except json.JSONDecodeError:
            fail("publisher receipt is not JSON")
        validate_receipt(receipt, args.service)
        print(f"{args.service}: read-only preflight passed")
    finally:
        if config_created:
            config_path.unlink(missing_ok=True)
        if ca_created:
            ca_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
