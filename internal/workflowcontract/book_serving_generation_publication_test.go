package workflowcontract

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	bookServingPublisherCommit = "6ee7206c13d6694de1d1ef6bf95b3f6b2b7a7022"
	bookServingWorkflow        = "../../.github/workflows/book_serving_generation_publish.yml"
	legacyWebRWorkflow         = "../../.github/workflows/webr_book_generation_publish.yml"
)

var bookServingSecrets = []string{
	"STATGROUND_SQL_READ_TOKEN",
	"BOOK_SERVING_COORDINATOR_ENDPOINT",
	"WEBR_BOOK_SERVING_PUBLISHER_PASSWORD",
	"MIRTYPE_BOOK_SERVING_PUBLISHER_PASSWORD",
	"STATGROUND_PROVIDER_BOOK_SERVING_PUBLISHER_PASSWORD",
	"BOOK_SERVING_NAVER_OUTBOX_ENDPOINT",
	"BOOK_SERVING_KAKAO_OUTBOX_ENDPOINT",
	"BOOK_SERVING_OUTBOX_OBSERVER_PASSWORD",
	"BOOK_SERVING_CA_PEM",
}

func TestBookServingPublisherUsesPinnedThreeProfileSQLSource(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, bookServingWorkflow)
	for _, contract := range []string{
		"repository: statground/Statground_SQL",
		"ref: " + bookServingPublisherCommit,
		"token: ${{ secrets.STATGROUND_SQL_READ_TOKEN }}",
		"persist-credentials: false",
		"sparse-checkout: docker-compose/50005_Clickhouse_Statground/book_serving_generation_publisher.py",
		`readonly expected_sql_commit="` + bookServingPublisherCommit + `"`,
		`test "$(git -C "$sql_root" rev-parse HEAD)" = "$expected_sql_commit"`,
		`readonly expected_publisher_blob="$(git -C "$sql_root" rev-parse "$expected_sql_commit:$publisher_relative")"`,
		`test "$(git -C "$sql_root" hash-object "$publisher_relative")" = "$expected_publisher_blob"`,
		"group: statground-book-serving-generation-publisher",
		"cancel-in-progress: false",
		"queue: max",
		"umask 077",
		"os.O_EXCL",
		`getattr(os, "O_NOFOLLOW", 0)`,
		"0o600",
		`"webr": "webr_book_serving_publisher"`,
		`"mirtype": "mirtype_book_serving_publisher"`,
		`"statground-provider": "statground_provider_book_serving_publisher"`,
		`"user": "book_serving_outbox_observer"`,
		`"Data_Book_NAVER_Log.naver_direct_insert_outbox"`,
		`"Data_Book_KAKAO_Log.kakao_direct_insert_outbox"`,
		`"coordinator_host": "clickhouse-s1-r1"`,
		"--preflight-only",
		"config_path.unlink(missing_ok=True)",
		"ca_path.unlink(missing_ok=True)",
		`env={"PYTHONUNBUFFERED": "1"}`,
		"capture_output=True",
		`"raw_target_rows"`,
		`preflight = validate_preflight(read_private_json(preflight_path), args.service)`,
		`!= preflight["content_sha256"]`,
		`receipt["rows"]`,
		`preflight["source_rows"]`,
		"- name: Remove Book serving publication credentials and receipts",
		"if: ${{ always() }}",
	} {
		if !strings.Contains(text, contract) {
			t.Errorf("three-profile publisher workflow is missing contract %q", contract)
		}
	}
	if count := strings.Count(text, bookServingPublisherCommit); count != 2 {
		t.Fatalf("immutable SQL publisher commit count=%d, want checkout and readback", count)
	}
	orderedSteps := []string{
		"- name: Preflight Web-R serving profile",
		"- name: Preflight MirType serving profile",
		"- name: Preflight Statground serving profile",
		"- name: Publish Web-R serving profile",
		"- name: Publish MirType serving profile",
		"- name: Publish Statground serving profile",
	}
	previous := -1
	for _, step := range orderedSteps {
		position := strings.Index(text, step)
		if position <= previous {
			t.Fatalf("single-profile publication step %q is absent or out of order", step)
		}
		previous = position
	}
	if count := strings.Count(text, "PUBLISHER_PASSWORD: ${{ secrets."); count != 6 {
		t.Fatalf("single-profile credential mapping count=%d, want six isolated steps", count)
	}
	for index, step := range orderedSteps {
		start := strings.Index(text, step)
		end := len(text)
		if index+1 < len(orderedSteps) {
			end = strings.Index(text, orderedSteps[index+1])
		}
		block := text[start:end]
		if count := strings.Count(block, "PUBLISHER_PASSWORD: ${{ secrets."); count != 1 {
			t.Fatalf("step %q exposes %d publisher credentials, want one", step, count)
		}
	}
	if strings.Contains(text, "webr_book_generation_publisher.py") ||
		strings.Contains(text, "continue-on-error") || strings.Contains(text, "github.token") ||
		strings.Contains(strings.ToLower(text), "--bootstrap") ||
		strings.Contains(text, "secrets.CLICKHOUSE_USER") ||
		strings.Contains(text, "secrets.CLICKHOUSE_PASSWORD") {
		t.Fatal("three-profile workflow can use a legacy publisher, ignore failure, or reuse collector credentials")
	}
}

func TestBookServingRunnerValidatesReceiptsAndStripsSecrets(t *testing.T) {
	t.Parallel()

	runner := extractBookServingRunner(t)
	temp := t.TempDir()
	runnerPath := filepath.Join(temp, "runner.py")
	if err := os.WriteFile(runnerPath, []byte(runner), 0o700); err != nil {
		t.Fatal(err)
	}
	fakePath := filepath.Join(temp, "publisher.py")
	if err := os.WriteFile(fakePath, []byte(fakeBookServingPublisher(false, false)), 0o700); err != nil {
		t.Fatal(err)
	}

	environment := bookServingRunnerEnvironment(temp, fakePath, "receipt-success")
	for _, arguments := range [][]string{
		{runnerPath, "--service", "webr", "--preflight-only"},
		{runnerPath, "--service", "webr"},
	} {
		command := exec.Command("python3", arguments...)
		command.Env = environment
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("runner failed: %v\n%s", err, output)
		}
		if strings.Contains(string(output), "publisher-secret") ||
			strings.Contains(string(output), "observer-secret") ||
			strings.Contains(string(output), "ca-secret") {
			t.Fatal("runner output exposed a publication secret")
		}
	}

	receiptDir := filepath.Join(temp, "book-serving-receipts-receipt-success-1")
	for _, name := range []string{"webr.preflight.json", "webr.publish.json"} {
		info, err := os.Stat(filepath.Join(receiptDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("receipt %s mode=%#o, want 0600", name, info.Mode().Perm())
		}
	}
	for _, suffix := range []string{".json", ".ca.pem"} {
		path := filepath.Join(temp, "book-serving-receipt-success-1-webr-publish"+suffix)
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("ephemeral credential file remains: %s", path)
		}
	}
}

func TestBookServingRunnerAcceptsExactAlreadyActiveReceipt(t *testing.T) {
	t.Parallel()

	runner := extractBookServingRunner(t)
	temp := t.TempDir()
	runnerPath := filepath.Join(temp, "runner.py")
	if err := os.WriteFile(runnerPath, []byte(runner), 0o700); err != nil {
		t.Fatal(err)
	}
	fakePath := filepath.Join(temp, "publisher.py")
	if err := os.WriteFile(fakePath, []byte(fakeBookServingPublisher(false, true)), 0o700); err != nil {
		t.Fatal(err)
	}
	environment := bookServingRunnerEnvironment(temp, fakePath, "already-active")
	for _, arguments := range [][]string{
		{runnerPath, "--service", "webr", "--preflight-only"},
		{runnerPath, "--service", "webr"},
	} {
		command := exec.Command("python3", arguments...)
		command.Env = environment
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("runner rejected exact already-active receipt: %v\n%s", err, output)
		}
	}
}

func TestBookServingRunnerRejectsReceiptDriftWithoutLeakingSecrets(t *testing.T) {
	t.Parallel()

	runner := extractBookServingRunner(t)
	temp := t.TempDir()
	runnerPath := filepath.Join(temp, "runner.py")
	if err := os.WriteFile(runnerPath, []byte(runner), 0o700); err != nil {
		t.Fatal(err)
	}
	fakePath := filepath.Join(temp, "publisher.py")
	if err := os.WriteFile(fakePath, []byte(fakeBookServingPublisher(false, false)), 0o700); err != nil {
		t.Fatal(err)
	}
	environment := bookServingRunnerEnvironment(temp, fakePath, "receipt-drift")

	preflight := exec.Command("python3", runnerPath, "--service", "webr", "--preflight-only")
	preflight.Env = environment
	if output, err := preflight.CombinedOutput(); err != nil {
		t.Fatalf("preflight failed: %v\n%s", err, output)
	}
	if err := os.WriteFile(fakePath, []byte(fakeBookServingPublisher(true, false)), 0o700); err != nil {
		t.Fatal(err)
	}
	publish := exec.Command("python3", runnerPath, "--service", "webr")
	publish.Env = environment
	output, err := publish.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "publication content digest differs from preflight") {
		t.Fatalf("drifted publication was not rejected: err=%v output=%s", err, output)
	}
	for _, secret := range []string{"publisher-secret", "observer-secret", "ca-secret"} {
		if strings.Contains(string(output), secret) {
			t.Fatalf("failure output exposed %s", secret)
		}
	}
	publishReceipt := filepath.Join(temp, "book-serving-receipts-receipt-drift-1", "webr.publish.json")
	if _, err := os.Lstat(publishReceipt); !os.IsNotExist(err) {
		t.Fatal("drifted publication wrote a success receipt")
	}
}

func extractBookServingRunner(t *testing.T) string {
	t.Helper()
	workflow := readWorkflow(t, bookServingWorkflow)
	const startMarker = "          cat >\"$runner\" <<'PY'\n"
	const endMarker = "\n          PY\n"
	start := strings.Index(workflow, startMarker)
	if start < 0 {
		t.Fatal("Book serving runner heredoc is missing")
	}
	start += len(startMarker)
	end := strings.Index(workflow[start:], endMarker)
	if end < 0 {
		t.Fatal("Book serving runner heredoc terminator is missing")
	}
	lines := strings.Split(workflow[start:start+end], "\n")
	for index, line := range lines {
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "          ") {
			t.Fatalf("runner line %d is not indented as a workflow heredoc", index+1)
		}
		lines[index] = strings.TrimPrefix(line, "          ")
	}
	return strings.Join(lines, "\n") + "\n"
}

func bookServingRunnerEnvironment(temp, publisher, runID string) []string {
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"RUNNER_TEMP=" + temp,
		"GITHUB_RUN_ID=" + runID,
		"GITHUB_RUN_ATTEMPT=1",
		"GITHUB_REPOSITORY=statground/Statground_Data_NAVER_Book",
		"BOOK_SERVING_PUBLISHER_PATH=" + publisher,
		"COORDINATOR_ENDPOINT=https://coordinator.invalid:8443",
		"PUBLISHER_USER=webr_book_serving_publisher",
		"PUBLISHER_PASSWORD=publisher-secret",
		"NAVER_OUTBOX_ENDPOINT=https://naver.invalid:8443",
		"KAKAO_OUTBOX_ENDPOINT=https://kakao.invalid:8443",
		"OUTBOX_OBSERVER_PASSWORD=observer-secret",
		"BOOK_SERVING_CA_PEM=ca-secret",
	}
}

func fakeBookServingPublisher(drift, alreadyActive bool) string {
	content := strings.Repeat("a", 64)
	if drift {
		content = strings.Repeat("b", 64)
	}
	published := `{
        "status": "published", "service": args.service,
        "snapshot_uuid": "11111111-1111-5111-8111-111111111111",
        "heartbeat_uuid": "22222222-2222-5222-8222-222222222222",
        "content_sha256": "` + content + `",
        "source_manifest_sha256": "` + strings.Repeat("c", 64) + `",
        "rows": 7, "marker_rows": 12,
    }`
	if alreadyActive {
		published = `{
        "status": "already_active", "service": args.service,
        "snapshot_uuid": "11111111-1111-5111-8111-111111111111",
        "source_manifest_sha256": "` + strings.Repeat("c", 64) + `",
        "rows": 7,
    }`
	}
	return `#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
parser.add_argument("--service", required=True)
parser.add_argument("--preflight-only", action="store_true")
args = parser.parse_args()
for name in ("COORDINATOR_ENDPOINT", "PUBLISHER_PASSWORD", "NAVER_OUTBOX_ENDPOINT",
             "KAKAO_OUTBOX_ENDPOINT", "OUTBOX_OBSERVER_PASSWORD", "BOOK_SERVING_CA_PEM",
             "GITHUB_TOKEN", "ACTIONS_RUNTIME_TOKEN"):
    if name in os.environ:
        raise SystemExit("secret environment reached publisher")
config = json.loads(Path(args.config).read_text())
if config["coordinator"]["password"] != "publisher-secret":
    raise SystemExit("selected publisher credential is absent")
if parser.parse_args().preflight_only:
    result = {
        "status": "preflight_ok", "database_writes": False, "service": args.service,
        "source_refresh_started_at": "2026-09-20 00:00:00.000",
        "source_refresh_success_at": "2026-09-20 00:10:00.000",
        "source_generation": "2026-09-20 00:05:00.000",
        "source_rows": 7, "raw_target_rows": 7,
        "content_sha256": "` + strings.Repeat("a", 64) + `",
        "policy_authority_revision": 9,
    }
else:
    result = ` + published + `
print(json.dumps(result, sort_keys=True, separators=(",", ":")))
`
}

func TestBookServingPublicationIsGatedAfterRefreshWithMutuallyExclusiveLegacyFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		path            string
		needs           string
		newCondition    string
		legacyCondition string
		refreshCue      string
	}{
		{
			name:            "naver",
			path:            "../../.github/workflows/naver_book_collect_all.yml",
			needs:           "needs: collect_all",
			newCondition:    "success() && needs.collect_all.outputs.book_serving_refresh_verified == 'true' && vars.BOOK_SERVING_GENERATION_PUBLISH_ENABLED == 'true' && (inputs.stage == 'all' || inputs.stage == 'refresh') && github.event_name == 'workflow_dispatch'",
			legacyCondition: "success() && needs.collect_all.outputs.webr_refresh_verified == 'true' && vars.BOOK_SERVING_GENERATION_PUBLISH_ENABLED != 'true' && vars.WEBR_BOOK_GENERATION_PUBLISH_ENABLED == 'true' && (inputs.stage == 'all' || inputs.stage == 'refresh') && github.event_name == 'workflow_dispatch'",
			refreshCue:      "Refresh Web-R and MirType book catalogs",
		},
		{
			name:            "kakao",
			path:            "../../.github/workflows/kakao_book_collect.yml",
			needs:           "needs: collect",
			newCondition:    "success() && needs.collect.outputs.book_serving_refresh_verified == 'true' && vars.BOOK_SERVING_GENERATION_PUBLISH_ENABLED == 'true' && inputs.dry_run != true",
			legacyCondition: "success() && needs.collect.outputs.webr_refresh_verified == 'true' && vars.BOOK_SERVING_GENERATION_PUBLISH_ENABLED != 'true' && vars.WEBR_BOOK_GENERATION_PUBLISH_ENABLED == 'true' && inputs.dry_run != true",
			refreshCue:      "Refresh provider-neutral serving catalogs for manual runs",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			text := readWorkflow(t, test.path)
			for _, contract := range []string{
				"publish_book_serving_generations:",
				"publish_legacy_webr_generation:",
				test.needs,
				test.newCondition,
				test.legacyCondition,
				"uses: ./.github/workflows/book_serving_generation_publish.yml",
				"uses: ./.github/workflows/webr_book_generation_publish.yml",
				"book_serving_refresh_verified:",
				`echo "verified=true" >> "$GITHUB_OUTPUT"`,
				test.refreshCue,
			} {
				if !strings.Contains(text, contract) {
					t.Errorf("%s workflow is missing publication contract %q", test.name, contract)
				}
			}
			for _, name := range bookServingSecrets {
				mapping := name + ": ${{ secrets." + name + " }}"
				want := 1
				if name == "STATGROUND_SQL_READ_TOKEN" {
					want = 2 // The mutually exclusive legacy publisher pins its own SQL source too.
				}
				if count := strings.Count(text, mapping); count != want {
					t.Errorf("%s workflow secret mapping %s count=%d, want %d", test.name, name, count, want)
				}
			}
			if count := strings.Count(text, "vars.BOOK_SERVING_GENERATION_PUBLISH_ENABLED"); count != 2 {
				t.Errorf("%s migration feature-gate count=%d, want new and legacy exclusion", test.name, count)
			}
			if strings.Contains(text, "book_serving_generation_publisher.py") {
				t.Errorf("%s collector duplicated the pinned SQL publisher", test.name)
			}
		})
	}
}

func TestKakaoScheduleForwardsDedicatedNewAndLegacyPublisherInputs(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, "../../.github/workflows/kakao_book_schedule.yml")
	for _, name := range append(bookServingSecrets, []string{
		"WEBR_BOOK_PUBLISHER_ENDPOINT",
		"WEBR_BOOK_PUBLISHER_USER",
		"WEBR_BOOK_PUBLISHER_PASSWORD",
		"WEBR_BOOK_PUBLISHER_CA_PEM",
	}...) {
		contract := name + ": ${{ secrets." + name + " }}"
		if count := strings.Count(text, contract); count != 1 {
			t.Errorf("Kakao schedule publisher secret %s count=%d, want one", name, count)
		}
	}
}

func TestThreeProfilePublisherHasNoIndependentScheduleAndLegacySourceRemainsPinnedForMigration(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, bookServingWorkflow)
	if strings.Contains(text, "  schedule:\n") || !strings.Contains(text, "  workflow_call:\n") {
		t.Fatal("three-profile publisher can run outside the provider workflow owner")
	}
	for _, path := range []string{
		bookServingWorkflow,
		"../../.github/workflows/kakao_book_collect.yml",
		"../../.github/workflows/kakao_book_schedule.yml",
		"../../.github/workflows/naver_book_collect_all.yml",
		"../../.github/workflows/naver_book_schedule.yml",
	} {
		if strings.Contains(strings.ToLower(readWorkflow(t, path)), "bootstrap") {
			t.Fatalf("scheduled publication path %s can bootstrap database objects", path)
		}
	}
	legacy := readWorkflow(t, legacyWebRWorkflow)
	if !strings.Contains(legacy, "ref: 27cbfecbb883c6c834ae2c58be74dad47c690b4d") ||
		!strings.Contains(legacy, "webr_book_generation_publisher.py") {
		t.Fatal("mutually exclusive migration fallback no longer uses its reviewed immutable source")
	}
}
