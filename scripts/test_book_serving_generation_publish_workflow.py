import unittest
from pathlib import Path


WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github/workflows/book_serving_generation_publish.yml"
)


class BookServingGenerationPublishWorkflowTest(unittest.TestCase):
    def test_coordinator_identity_matches_clickhouse_hostname(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('"coordinator_host": "clickhouse-s1-r1"', workflow)
        self.assertNotIn('"coordinator_host": "Clickhouse_S1_R1"', workflow)

    def test_all_preflights_precede_first_publication(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        first_publish = workflow.index("- name: Publish Web-R serving profile")
        for profile in ("Web-R", "MirType", "Statground"):
            self.assertLess(
                workflow.index(f"- name: Preflight {profile} serving profile"),
                first_publish,
            )

    def test_manual_entry_is_preflight_only_by_default(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        dispatch = workflow.split("  workflow_dispatch:", 1)[1].split("  workflow_call:", 1)[0]
        self.assertIn("default: false", dispatch)
        self.assertIn("type: boolean", dispatch)
        guard = "if: ${{ github.event_name == 'workflow_call' || inputs.publish == true }}"
        self.assertEqual(workflow.count(guard), 3)
        self.assertIn("- name: Remove Book serving publication credentials and receipts", workflow)
        self.assertIn("if: ${{ always() }}", workflow)


if __name__ == "__main__":
    unittest.main()
