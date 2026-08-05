package workflowcontract

import (
	"os"
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

func TestKakaoWorkflowRequiresApprovedTLSEndpointTuple(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, "../../.github/workflows/kakao_book_collect.yml")
	for _, contract := range []string{
		"ClickHouse host must be a hostname without a URL scheme",
		"ClickHouse TLS endpoint must use a certificate hostname instead of an IP address",
		"ClickHouse TLS protocol is required",
		"ClickHouse TLS endpoint must use port 443",
		"ClickHouse TLS endpoint path must be empty",
	} {
		if !strings.Contains(text, contract) {
			t.Errorf("Kakao workflow is missing TLS endpoint contract %q", contract)
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
