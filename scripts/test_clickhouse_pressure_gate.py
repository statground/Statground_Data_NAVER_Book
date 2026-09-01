import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("clickhouse_pressure_gate.py")
SPEC = importlib.util.spec_from_file_location("clickhouse_pressure_gate", MODULE_PATH)
assert SPEC and SPEC.loader
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class ClickHousePressureGateTest(unittest.TestCase):
    def setUp(self):
        self.thresholds = gate.load_thresholds({})
        self.healthy = {
            "endpoint_hostname": "Clickhouse_S1_R1",
            "expected_endpoint_hostname": "Clickhouse_S1_R1",
            "distributed_files": 20,
            "broken_distributed_files": 0,
            "available_samples": 1,
            "available_bytes": 500 * 1024**3,
            "total_samples": 1,
            "total_bytes": 1000 * 1024**3,
            "available_inode_samples": 1,
            "available_inodes": 8_000_000,
            "total_inode_samples": 1,
            "total_inodes": 10_000_000,
            "iowait_samples": 1,
            "iowait_normalized": 0.10,
            "expected_replica_target_count": 2,
            "replica_target_count": 2,
            "expected_local_target_count": 1,
            "local_target_count": 1,
            "invalid_local_target_engines": 0,
            "unhealthy_replicas": 0,
            "max_replica_queue": 12,
            "max_replica_delay_seconds": 4,
            "max_parts_to_check": 0,
        }

    def test_healthy_snapshot_passes(self):
        self.assertEqual(gate.evaluate(self.healthy, self.thresholds), [])

    def test_distributed_threshold_is_configurable_and_validated(self):
        self.assertEqual(self.thresholds["max_distributed_files"], 10_000)
        custom = gate.load_thresholds({"CLICKHOUSE_PRESSURE_GATE_MAX_DISTRIBUTED_FILES": "123"})
        self.assertEqual(custom["max_distributed_files"], 123)
        for raw in ("-1", "not-a-number"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                gate.load_thresholds({"CLICKHOUSE_PRESSURE_GATE_MAX_DISTRIBUTED_FILES": raw})

    def test_expected_endpoint_contract_is_strict_and_fail_closed(self):
        self.assertEqual(
            gate.load_expected_endpoint_hostname({gate.ENDPOINT_ENV: "Clickhouse_S1_R1"}),
            "Clickhouse_S1_R1",
        )
        for raw in ("", " Clickhouse_S1_R1", "clickhouse-gateway", "bad/host"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                gate.load_expected_endpoint_hostname({gate.ENDPOINT_ENV: raw})
        mismatch = dict(self.healthy, endpoint_hostname="Clickhouse_S1_R2")
        self.assertIn("endpoint_hostname_mismatch", gate.evaluate(mismatch, self.thresholds))
        missing = dict(self.healthy, endpoint_hostname="")
        self.assertIn("endpoint_hostname_mismatch", gate.evaluate(missing, self.thresholds))

    def test_each_pressure_dimension_blocks(self):
        cases = {
            "unhealthy_replicas": 1,
            "available_bytes": 10,
            "available_inodes": 10,
            "iowait_normalized": 0.75,
            "max_replica_queue": 2001,
            "max_replica_delay_seconds": 901,
            "max_parts_to_check": 1,
        }
        for key, value in cases.items():
            with self.subTest(key=key):
                snapshot = dict(self.healthy, **{key: value})
                self.assertTrue(gate.evaluate(snapshot, self.thresholds))

    def test_distributed_backlog_and_broken_files_fail_closed(self):
        at_limit = dict(self.healthy, distributed_files=10_000)
        self.assertEqual(gate.evaluate(at_limit, self.thresholds), [])
        over_limit = dict(self.healthy, distributed_files=606_448)
        self.assertIn("distributed_files=606448", gate.evaluate(over_limit, self.thresholds))
        broken = dict(self.healthy, broken_distributed_files=7)
        self.assertIn("broken_distributed_files=7", gate.evaluate(broken, self.thresholds))

    def test_idle_replica_delay_does_not_block(self):
        snapshot = dict(self.healthy, max_replica_queue=0, max_replica_delay_seconds=9_999_999)
        self.assertEqual(gate.evaluate(snapshot, self.thresholds), [])

    def test_missing_filesystem_metrics_fails_closed(self):
        snapshot = dict(self.healthy, available_samples=0)
        self.assertIn("filesystem_metrics_unavailable", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, total_inode_samples=0)
        self.assertIn("filesystem_inode_metrics_unavailable", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, iowait_samples=0)
        self.assertIn("iowait_metrics_unavailable", gate.evaluate(snapshot, self.thresholds))

    def test_target_contract_is_strict_and_fail_closed(self):
        valid = gate.load_targets(
            {
                gate.TARGET_ENV: (
                    "replica:Data_Book_NAVER_Raw.naver_book_raw_local,"
                    "local:Data_Book_NAVER_Log.naver_direct_insert_outbox"
                )
            }
        )
        self.assertEqual(len(valid), 2)
        for raw in ("", "naver_book_raw_local", "replica:db.table,", "replica:db.table,replica:db.table"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                gate.load_targets({gate.TARGET_ENV: raw})

        snapshot = dict(self.healthy, replica_target_count=1)
        self.assertIn("replica_targets=1/2", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, local_target_count=0)
        self.assertIn("local_targets=0/1", gate.evaluate(snapshot, self.thresholds))
        snapshot = dict(self.healthy, invalid_local_target_engines=1)
        self.assertIn("invalid_local_target_engines=1", gate.evaluate(snapshot, self.thresholds))

    def test_unrelated_legacy_queue_is_excluded_but_target_queue_blocks(self):
        targets = gate.load_targets(
            {gate.TARGET_ENV: "replica:Data_Book_NAVER_Raw.naver_book_raw_local"}
        )
        query = gate.build_pressure_query(targets)
        self.assertIn("naver_book_raw_local", query)
        self.assertNotIn("polymarket_market_latest_v2_local", query)
        self.assertIn("WHERE (database, table) IN", query)
        snapshot = dict(self.healthy, max_replica_queue=2001)
        self.assertIn("replica_queue=2001", gate.evaluate(snapshot, self.thresholds))

    def test_query_uses_only_bounded_system_tables(self):
        targets = gate.load_targets(
            {
                gate.TARGET_ENV: (
                    "replica:Data_Book_NAVER_Raw.naver_book_raw_local,"
                    "local:Data_Book_NAVER_Log.naver_direct_insert_outbox"
                )
            }
        )
        query = gate.build_pressure_query(targets)
        self.assertIn("hostName() AS endpoint_hostname", query)
        self.assertIn("system.metrics", query)
        self.assertIn("system.asynchronous_metrics", query)
        self.assertIn("system.disks", query)
        self.assertIn("system.replicas", query)
        self.assertIn("system.tables", query)
        self.assertNotIn("system.distribution_queue", query)
        self.assertNotIn("ReadonlyReplica", query)
        self.assertIn("maxIf(absolute_delay, queue_size > 0)", query)
        self.assertIn("max_threads = 1", query)

    def test_url_supports_both_env_families(self):
        self.assertEqual(
            gate.clickhouse_url({"CH_HOST": "db.example", "CH_PORT": "8123", "CH_SECURE": "true"}),
            "https://db.example:8123/",
        )
        self.assertEqual(
            gate.clickhouse_url({"CLICKHOUSE_HOST": "http://db.example:9000/base"}),
            "http://db.example:9000/base",
        )

    def test_provider_workflows_gate_before_api_collection(self):
        root = Path(__file__).parents[1]
        workflows = root / ".github" / "workflows"
        naver = (workflows / "naver_book_collect_all.yml").read_text()
        kakao = (workflows / "kakao_book_collect.yml").read_text()
        nlk = (workflows / "nlk_service_backfill.yml").read_text()
        gate_step = "run: python3 scripts/clickhouse_pressure_gate.py"
        self.assertEqual(naver.count(gate_step), 1)
        self.assertLess(naver.index(gate_step), naver.index("go run -mod=mod ./cmd/batch_aladin_publisher_seed"))
        self.assertEqual(kakao.count(gate_step), 1)
        self.assertLess(kakao.index(gate_step), kakao.index("go run -mod=mod ./cmd/collect_kakao"))
        self.assertEqual(nlk.count(gate_step), 1)
        self.assertLess(nlk.index(gate_step), nlk.index("go run -mod=mod ./cmd/backfill_nlk_service"))
        endpoint_binding = "CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME: clickhouse-s1-r1"
        self.assertEqual(naver.count(endpoint_binding), 1)
        self.assertEqual(kakao.count(endpoint_binding), 1)
        self.assertEqual(nlk.count(endpoint_binding), 1)
        self.assertNotIn("secrets.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", naver + kakao + nlk)
        self.assertNotIn("vars.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", naver + kakao + nlk)
        self.assertEqual(
            (naver + kakao + nlk).count('CLICKHOUSE_PRESSURE_GATE_MAX_DISTRIBUTED_FILES: "10000"'),
            3,
        )
        kakao_gate_block = kakao[kakao.index("- name: Gate ClickHouse writes on storage pressure") : kakao.index("- name: Collect Kakao books into provider tables")]
        self.assertNotIn("\n        if:", kakao_gate_block)
        self.assertIn("CLICKHOUSE_PRESSURE_GATE_TARGETS: >-", naver)
        self.assertIn("replica:Data_Book_NAVER_Raw.naver_book_raw_local", naver)
        self.assertIn("replica:mirtype_content.provider_practice_content_serving_local", naver)
        self.assertIn("local:Data_Book_NAVER_Log.naver_direct_insert_outbox", naver)
        self.assertIn("CLICKHOUSE_PRESSURE_GATE_TARGETS: >-", kakao)
        self.assertIn("replica:Data_Book_KAKAO_Raw.kakao_book_raw_local", kakao)
        self.assertIn("replica:Data_Book_Service.book_isbn_alias_local", kakao)
        self.assertIn("local:Data_Book_KAKAO_Log.kakao_direct_insert_outbox", kakao)
        self.assertIn("replica:Data_Book_NLK_Raw.nlk_resource_raw_local", nlk)
        self.assertNotIn("polymarket_market_latest_v2_local", naver + kakao)
        kakao_main = (root / "cmd/collect_kakao/main.go").read_text()
        self.assertLess(kakao_main.index("store.Validate(ctx)"), kakao_main.index('boolEnv("KAKAO_DRY_RUN", false)'))

    def test_contract_workflow_is_secret_free_and_read_only(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/book_contract_tests.yml").read_text()
        self.assertIn("push:\n    branches: [main]", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("go test -mod=mod ./...", workflow)
        self.assertIn("python3 -m unittest scripts/test_clickhouse_pressure_gate.py", workflow)
        self.assertIn("github.com/rhysd/actionlint/cmd/actionlint@v1.7.11", workflow)
        self.assertNotIn("secrets.", workflow)
        self.assertNotIn("run: python3 scripts/clickhouse_pressure_gate.py", workflow)


if __name__ == "__main__":
    unittest.main()
