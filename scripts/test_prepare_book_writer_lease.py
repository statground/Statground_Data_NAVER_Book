import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import prepare_book_writer_lease as setup

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

if __name__ == "__main__":
    unittest.main()
