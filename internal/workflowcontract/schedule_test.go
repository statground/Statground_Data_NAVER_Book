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

func TestKakaoWorkflowAllowsOnlyApprovedClickHouseTransportTuples(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, "../../.github/workflows/kakao_book_collect.yml")
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
	if count := strings.Count(text, `CH_PROTOCOL: ${{ steps.clickhouse_transport.outputs.protocol }}`); count != 2 {
		t.Fatalf("derived ClickHouse protocol consumer count=%d, want collect and refresh", count)
	}
	if count := strings.Count(text, `CH_PORT: ${{ steps.clickhouse_transport.outputs.port }}`); count != 2 {
		t.Fatalf("derived ClickHouse port consumer count=%d, want collect and refresh", count)
	}
	if !strings.Contains(text, `echo "port=$CH_PORT" >> "$GITHUB_OUTPUT"`) {
		t.Fatal("Kakao collector does not expose the validated ClickHouse port to downstream steps")
	}

	schedule := readWorkflow(t, "../../.github/workflows/kakao_book_schedule.yml")
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

func readWorkflow(t *testing.T, path string) string {
	t.Helper()

	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(source)
}
