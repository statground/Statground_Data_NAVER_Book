package workflowcontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBookProviderRunsShareNonCancellingConcurrency(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"../../.github/workflows/naver_book_schedule.yml",
		"../../.github/workflows/kakao_book_schedule.yml",
	} {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(source)
		if !strings.Contains(text, "group: statground-book-provider-schedule") {
			t.Errorf("%s does not use the shared provider schedule group", path)
		}
		if !strings.Contains(text, "cancel-in-progress: false") {
			t.Errorf("%s can still cancel an in-progress provider collection", path)
		}
	}
}

func TestLegacyNAVERWorkflowIsManualOnly(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, "../../.github/workflows/naver_book_schedule.yml")
	if strings.Contains(text, "  schedule:\n") {
		t.Fatal("legacy NAVER workflow still has a scheduled trigger")
	}
	if !strings.Contains(text, "  workflow_dispatch:\n") {
		t.Fatal("legacy NAVER manual rollback trigger is missing")
	}
	if !strings.Contains(text, "name: NAVER Book Manual Rollback Pipeline") {
		t.Fatal("legacy NAVER workflow is still presented as a scheduled pipeline")
	}
}

func TestNAVERWorkflowUsesBoundedFailClosedDirectInsertOutbox(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, "../../.github/workflows/naver_book_collect_all.yml")
	if !strings.Contains(text, "group: statground-book-naver-writer-replayer") ||
		!strings.Contains(text, "cancel-in-progress: false") ||
		strings.Contains(text, "cancel-in-progress: true") {
		t.Fatal("NAVER writers and replayers are not serialized without cancellation")
	}
	for _, contract := range []string{
		`NAVER_OUTBOX_REPLAY_LIMIT: "25"`,
		`NAVER_OUTBOX_MAX_REPLICA_QUEUE: "1000"`,
		`NAVER_OUTBOX_MAX_REPLICA_DELAY_SECONDS: "900"`,
		`ALADIN_PUBLISHER_CACHE_REQUIRED: "true"`,
	} {
		if count := strings.Count(text, contract); count != 1 {
			t.Errorf("NAVER workflow contract %q count=%d, want one", contract, count)
		}
	}
	if strings.Contains(text, `SEARCH_LOG_REQUIRED: "false"`) ||
		!strings.Contains(text, `SEARCH_LOG_REQUIRED: "true"`) {
		t.Fatal("NAVER search-log persistence is not fail-closed")
	}
	if count := strings.Count(text, `CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME: clickhouse-s1-r1`); count != 1 {
		t.Fatalf("NAVER physical endpoint hostname binding count=%d, want one job-level value", count)
	}
	if strings.Contains(text, `secrets.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME`) || strings.Contains(text, `vars.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME`) {
		t.Fatal("NAVER physical endpoint identity still depends on manual repository configuration")
	}
	if count := strings.Count(text, `test -n "$CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME"`); count != 1 {
		t.Fatalf("NAVER required physical endpoint hostname validation count=%d, want one", count)
	}
	if count := strings.Count(text, `if "gateway" in expected.lower():`); count != 1 {
		t.Fatalf("NAVER gateway hostname rejection count=%d, want one", count)
	}
}

func TestKakaoWorkflowAllowsOnlyApprovedClickHouseTransportTuples(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, "../../.github/workflows/kakao_book_collect.yml")
	if !strings.Contains(text, "group: statground-kakao-book-api") ||
		!strings.Contains(text, "cancel-in-progress: false") ||
		strings.Contains(text, "cancel-in-progress: true") {
		t.Fatal("Kakao writers and replayers are not serialized without cancellation")
	}
	for _, contract := range []string{
		"ClickHouse host must be a hostname without a URL scheme",
		"ClickHouse endpoint path must be empty",
		"Approved ClickHouse IP/HTTP endpoint requires its explicit non-TLS port",
		"ClickHouse hostname/HTTPS endpoint requires port 443",
		"Approved legacy ClickHouse IP/HTTP transport is active",
		`echo "protocol=http" >> "$GITHUB_OUTPUT"`,
		`echo "protocol=https" >> "$GITHUB_OUTPUT"`,
	} {
		if !strings.Contains(text, contract) {
			t.Errorf("Kakao workflow is missing approved transport contract %q", contract)
		}
	}
	if count := strings.Count(text, `KAKAO_REQUIRE_CLICKHOUSE_HTTPS: "false"`); count != 1 {
		t.Fatalf("Kakao HTTP override count=%d, want exactly one collector-step override", count)
	}
	if count := strings.Count(text, `CH_PROTOCOL: ${{ steps.clickhouse_transport.outputs.protocol }}`); count != 3 {
		t.Fatalf("derived ClickHouse protocol consumer count=%d, want pressure gate, collect, and refresh", count)
	}
	if count := strings.Count(text, `CH_PORT: ${{ steps.clickhouse_transport.outputs.port }}`); count != 3 {
		t.Fatalf("derived ClickHouse port consumer count=%d, want pressure gate, collect, and refresh", count)
	}
	if count := strings.Count(text, `KAKAO_CLICKHOUSE_RAW_WRITE_TIMEOUT_SECONDS: "660"`); count != 1 {
		t.Fatalf("Kakao raw-write timeout count=%d, want exactly one bounded collector setting", count)
	}
	if count := strings.Count(text, `CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS: "90"`); count != 1 {
		t.Fatalf("Kakao preflight retry budget count=%d, want one job-level setting", count)
	}
	if count := strings.Count(text, `CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS: "5"`); count != 1 {
		t.Fatalf("Kakao preflight retry backoff count=%d, want one job-level setting", count)
	}
	if count := strings.Count(text, `KAKAO_OUTBOX_REPLAY_LIMIT: "25"`); count != 1 {
		t.Fatalf("Kakao outbox replay limit count=%d, want one bounded job-level setting", count)
	}
	if count := strings.Count(text, `KAKAO_OUTBOX_MAX_REPLICA_QUEUE: "1000"`); count != 1 {
		t.Fatalf("Kakao replica queue gate count=%d, want one bounded job-level setting", count)
	}
	if count := strings.Count(text, `KAKAO_OUTBOX_MAX_REPLICA_DELAY_SECONDS: "900"`); count != 1 {
		t.Fatalf("Kakao replica delay gate count=%d, want one bounded job-level setting", count)
	}
	if !strings.Contains(text, `echo "port=$CH_PORT" >> "$GITHUB_OUTPUT"`) {
		t.Fatal("Kakao collector does not expose the validated ClickHouse port to downstream steps")
	}
	if count := strings.Count(text, `CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME: clickhouse-s1-r1`); count != 1 {
		t.Fatalf("Kakao physical endpoint hostname binding count=%d, want one job-level value", count)
	}
	if count := strings.Count(text, `test -n "$CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME"`); count != 1 {
		t.Fatalf("Kakao required physical endpoint hostname validation count=%d, want one", count)
	}
	if count := strings.Count(text, `if "gateway" in expected.lower():`); count != 1 {
		t.Fatalf("Kakao gateway hostname rejection count=%d, want one", count)
	}

	schedule := readWorkflow(t, "../../.github/workflows/kakao_book_schedule.yml")
	if strings.Contains(text+schedule, `secrets.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME`) || strings.Contains(text+schedule, `vars.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME`) {
		t.Fatal("Kakao physical endpoint identity still depends on manual repository configuration")
	}
	if strings.Contains(text+schedule, "CLICKHOUSE_PROTOCOL") {
		t.Fatal("Kakao workflows still propagate the stale protocol secret instead of the approved host tuple")
	}
}

func TestKakaoHTTPOverrideIsNotSharedWithOtherWorkflows(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob("../../.github/workflows/*.yml")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if strings.HasSuffix(path, "kakao_book_collect.yml") {
			continue
		}
		text := readWorkflow(t, path)
		if strings.Contains(text, `KAKAO_REQUIRE_CLICKHOUSE_HTTPS: "false"`) {
			t.Errorf("Kakao HTTP override leaked into %s", path)
		}
	}
}

func TestNLKRangeBackfillIsPressureGatedToItsExactTables(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, "../../.github/workflows/nlk_service_backfill.yml")
	if count := strings.Count(text, "Gate ClickHouse writes on storage pressure"); count != 1 {
		t.Fatalf("NLK pressure-gate step count=%d, want one", count)
	}
	for _, target := range []string{
		"replica:Data_Book_NLK_Raw.nlk_resource_raw_local",
		"replica:Data_Book_NLK_Log.nlk_service_projection_checkpoint_local",
		"replica:Data_Book_NLK_Service.nlk_authority_local",
		"replica:Data_Book_NLK_Service.nlk_bibliography_local",
		"replica:Data_Book_NLK_Service.nlk_library_local",
		"replica:Data_Book_Service.book_provider_latest_local",
		"replica:Data_Book_Service.book_bibliography_context_local",
		"replica:Data_Book_Service.book_kdc_summary_local",
		"replica:Data_Book_Service.book_isbn_alias_local",
	} {
		if count := strings.Count(text, target); count != 1 {
			t.Errorf("NLK pressure-gate target %q count=%d, want one", target, count)
		}
	}
	if strings.Contains(text, "CLICKHOUSE_PRESSURE_GATE_MAX_DISTRIBUTED_FILES") {
		t.Fatal("NLK backfill is coupled to unrelated distributed queues")
	}
	if !strings.Contains(text, "run: python3 scripts/clickhouse_pressure_gate.py") {
		t.Fatal("NLK pressure gate does not execute the shared fail-closed checker")
	}
}

func readWorkflow(t *testing.T, path string) string {
	t.Helper()

	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(source)
}
