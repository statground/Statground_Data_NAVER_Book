import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts import book_serving_readonly_preflight as readonly


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/book_serving_readonly_preflight.yml"
RUNNER = ROOT / "scripts/book_serving_readonly_preflight.py"
SQL_PUBLISHER_COMMIT = "0750ff8a099c29274ca24ea9ba7da4b101715645"


def receipt(**changes):
    value = {
        "status": "preflight_ok",
        "database_writes": False,
        "service": "webr",
        "read_only_preflight": True,
        "replica_sync_requested": False,
        "source_refresh_started_at": "2026-09-20 00:00:00.000",
        "source_refresh_success_at": "2026-09-20 00:10:00.000",
        "source_generation": "2026-09-20 00:05:00.000",
        "source_rows": 7,
        "raw_target_rows": 7,
        "content_sha256": "a" * 64,
        "policy_authority_revision": 9,
    }
    value.update(changes)
    return value


class BookServingReadOnlyPreflightTest(unittest.TestCase):
    def test_manual_workflow_has_no_publish_or_schedule_path(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("on:\n  workflow_dispatch:\n", workflow)
        self.assertIn("group: statground-book-serving-generation-publisher", workflow)
        self.assertIn("if: ${{ github.ref == 'refs/heads/main' }}", workflow)
        self.assertIn("ref: ${{ github.sha }}", workflow)
        for forbidden in ("  push:", "  schedule:", "  workflow_call:",
                          "  repository_dispatch:", "- name: Publish", "--preflight-only"):
            self.assertNotIn(forbidden, workflow)
        self.assertEqual(workflow.count(f'ref: "{SQL_PUBLISHER_COMMIT}"'), 1)
        self.assertEqual(workflow.count(f'expected_sql_commit="{SQL_PUBLISHER_COMMIT}"'), 1)
        self.assertIn('test "$expected_sql_commit" != "0000000000000000000000000000000000000000"', workflow)
        self.assertEqual(workflow.count("PUBLISHER_PASSWORD: ${{ secrets."), 3)
        self.assertEqual(workflow.count("scripts/book_serving_readonly_preflight.py"), 3)
        self.assertEqual(workflow.count("--service "), 3)

    def test_receipt_requires_exact_read_only_proof(self):
        readonly.validate_receipt(receipt(), "webr")
        readonly.validate_receipt(receipt(source_rows=5, raw_target_rows=7), "webr")
        for value in (
            receipt(replica_sync_requested=True),
            receipt(read_only_preflight=False),
            receipt(database_writes=True),
            receipt(source_rows=0),
            receipt(source_rows=True),
            receipt(content_sha256="B" * 64),
            receipt(unexpected="field"),
            receipt(source_generation="2026-09-20 00:12:00.000"),
        ):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                readonly.validate_receipt(value, "webr")

    def test_runner_passes_only_read_only_flag_and_cleans_secrets(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            fake = directory / "fake_publisher.py"
            fake.write_text(
                "import argparse, json, os\n"
                "from pathlib import Path\n"
                "p = argparse.ArgumentParser()\n"
                "p.add_argument('--config', required=True)\n"
                "p.add_argument('--service', required=True)\n"
                "p.add_argument('--read-only-preflight', action='store_true')\n"
                "a = p.parse_args()\n"
                "assert a.read_only_preflight and a.service == 'webr'\n"
                "assert 'PUBLISHER_PASSWORD' not in os.environ\n"
                "c = json.loads(Path(a.config).read_text())\n"
                "assert c['coordinator']['password'] == 'publisher-secret'\n"
                "assert c['coordinator_host'] == 'clickhouse-s1-r1'\n"
                "print(json.dumps(" + repr(receipt()) + "))\n",
                encoding="utf-8",
            )
            env = {
                "PATH": os.environ.get("PATH", ""),
                "RUNNER_TEMP": temporary,
                "GITHUB_REPOSITORY": "statground/Statground_Data_NAVER_Book",
                "GITHUB_EVENT_NAME": "workflow_dispatch",
                "GITHUB_RUN_ID": "123",
                "GITHUB_RUN_ATTEMPT": "1",
                "COORDINATOR_ENDPOINT": "https://coordinator.invalid:8443",
                "PUBLISHER_USER": "webr_book_serving_publisher",
                "PUBLISHER_PASSWORD": "publisher-secret",
                "NAVER_OUTBOX_ENDPOINT": "https://naver.invalid:8443",
                "KAKAO_OUTBOX_ENDPOINT": "https://kakao.invalid:8443",
                "OUTBOX_OBSERVER_PASSWORD": "observer-secret",
                "BOOK_SERVING_CA_PEM": "ca-secret",
            }
            command = [sys.executable, str(RUNNER), "--service", "webr", "--publisher", str(fake)]
            result = subprocess.run(command, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("webr: read-only preflight passed", result.stdout)
            for secret in ("publisher-secret", "observer-secret", "ca-secret"):
                self.assertNotIn(secret, result.stdout + result.stderr)
            self.assertFalse(list(directory.glob("book-read-only-*")))

            fake.write_text("raise SystemExit('publisher-secret in raw error')\n", encoding="utf-8")
            blocked = subprocess.run(command, env=env, capture_output=True, text=True, check=False)
            self.assertNotEqual(blocked.returncode, 0)
            self.assertIn("webr: preflight_blocked", blocked.stderr)
            self.assertNotIn("publisher-secret", blocked.stdout + blocked.stderr)
            self.assertFalse(list(directory.glob("book-read-only-*")))

            wrong_event = dict(env, GITHUB_EVENT_NAME="push")
            rejected = subprocess.run(command, env=wrong_event, capture_output=True, text=True, check=False)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("run identity is invalid", rejected.stderr)


if __name__ == "__main__":
    unittest.main()
