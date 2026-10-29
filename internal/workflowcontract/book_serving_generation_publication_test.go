package workflowcontract

import (
	"crypto/sha256"
	"fmt"
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

	environment := bookServingRunnerEnvironment(temp, fakePath, "receipt-success", "mirtype")
	for _, arguments := range [][]string{
		{runnerPath, "--service", "mirtype", "--preflight-only"},
		{runnerPath, "--service", "mirtype"},
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
	for _, name := range []string{"mirtype.preflight.json", "mirtype.publish.json"} {
		info, err := os.Stat(filepath.Join(receiptDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("receipt %s mode=%#o, want 0600", name, info.Mode().Perm())
		}
	}
	for _, suffix := range []string{".json", ".ca.pem"} {
		path := filepath.Join(temp, "book-serving-receipt-success-1-mirtype-publish"+suffix)
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
	environment := bookServingRunnerEnvironment(temp, fakePath, "already-active", "mirtype")
	for _, arguments := range [][]string{
		{runnerPath, "--service", "mirtype", "--preflight-only"},
		{runnerPath, "--service", "mirtype"},
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
	environment := bookServingRunnerEnvironment(temp, fakePath, "receipt-drift", "mirtype")

	preflight := exec.Command("python3", runnerPath, "--service", "mirtype", "--preflight-only")
	preflight.Env = environment
	if output, err := preflight.CombinedOutput(); err != nil {
		t.Fatalf("preflight failed: %v\n%s", err, output)
	}
	if err := os.WriteFile(fakePath, []byte(fakeBookServingPublisher(true, false)), 0o700); err != nil {
		t.Fatal(err)
	}
	publish := exec.Command("python3", runnerPath, "--service", "mirtype")
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
	publishReceipt := filepath.Join(temp, "book-serving-receipts-receipt-drift-1", "mirtype.publish.json")
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

func bookServingRunnerEnvironment(temp, publisher, runID string, services ...string) []string {
	service := "webr"
	if len(services) != 0 {
		service = services[0]
	}
	users := map[string]string{"webr": "webr_book_serving_publisher", "mirtype": "mirtype_book_serving_publisher", "statground-provider": "statground_provider_book_serving_publisher"}
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"RUNNER_TEMP=" + temp,
		"GITHUB_RUN_ID=" + runID,
		"GITHUB_RUN_ATTEMPT=1",
		"GITHUB_REPOSITORY=statground/Statground_Data_NAVER_Book",
		"BOOK_SERVING_PUBLISHER_PATH=" + publisher,
		"COORDINATOR_ENDPOINT=https://coordinator.invalid:8443",
		"PUBLISHER_USER=" + users[service],
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
			wantGates := 2
			if test.name == "kakao" {
				wantGates++ // New discovery must verify publication configuration before calling Kakao.
			}
			if count := strings.Count(text, "vars.BOOK_SERVING_GENERATION_PUBLISH_ENABLED"); count != wantGates {
				t.Errorf("%s publication feature-gate count=%d, want %d", test.name, count, wantGates)
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

func TestDiscoveryPublicationConfigurationGuardExecutesBeforeAPIAndRedactsSecrets(t *testing.T) {
	text := readWorkflow(t, "../../.github/workflows/kakao_book_collect.yml")
	marker := "      - name: Require three-profile publication before discovery collection\n"
	start := strings.Index(text, marker)
	end := strings.Index(text[start+len(marker):], "\n      - name:")
	if start < 0 || end < 0 || start >= strings.Index(text, "      - name: Collect Kakao books into provider tables") {
		t.Fatal("publication readiness guard is absent or runs after provider collection")
	}
	step := text[start : start+len(marker)+end]
	runStart := strings.Index(step, "        run: |\n")
	if runStart < 0 {
		t.Fatal("guard run block is absent")
	}
	var script strings.Builder
	for _, line := range strings.Split(step[runStart+len("        run: |\n"):], "\n") {
		script.WriteString(strings.TrimPrefix(line, "          "))
		script.WriteByte('\n')
	}
	aliases := []string{"PUB_SQL_TOKEN", "PUB_COORDINATOR_ENDPOINT", "PUB_WEBR_PASSWORD", "PUB_MIRTYPE_PASSWORD", "PUB_STATGROUND_PASSWORD", "PUB_NAVER_OUTBOX_ENDPOINT", "PUB_KAKAO_OUTBOX_ENDPOINT", "PUB_OUTBOX_OBSERVER_PASSWORD"}
	for _, enabled := range []bool{false, true} {
		command := exec.Command("bash", "-c", script.String())
		command.Env = []string{"PATH=" + os.Getenv("PATH")}
		if enabled {
			command.Env = append(command.Env, "PUB_ENABLED=true")
			for _, alias := range aliases {
				command.Env = append(command.Env, alias+"=private-test-token")
			}
		}
		output, err := command.CombinedOutput()
		if enabled && err != nil || !enabled && err == nil {
			t.Fatalf("enabled=%t guard error=%v output=%s", enabled, err, output)
		}
		if strings.Contains(string(output), "private-test-token") {
			t.Fatal("publication configuration guard exposed a secret")
		}
		if !enabled && !strings.Contains(string(output), "before API collection") {
			t.Fatalf("guard did not explain the pre-API stop: %s", output)
		}
	}
}

func TestHostHandoffPreservesOtherProfileStepBytes(t *testing.T) {
	text := readWorkflow(t, bookServingWorkflow)
	pins := map[string]string{
		"Preflight MirType serving profile":    "43b9f35bc90cb8f3e698f94a14ef9b9c524343e7aae5de8fb0323615fdcb4659",
		"Preflight Statground serving profile": "ac34a3064730c1575a49a13011ad7b698afc9a098c65639742b10f34668a3296",
		"Publish MirType serving profile":      "a25e3a81a25faef6ae3c6efe17461a61e77420d3f0febd71a5a3d134310bf803",
		"Publish Statground serving profile":   "fb955cb6a2e9c4f68afa7a052abee3a5a0025c72b9c65fe68d56a44cd7a7733f",
	}
	for name, expected := range pins {
		marker := "      - name: " + name + "\n"
		start := strings.Index(text, marker)
		if start < 0 {
			t.Fatalf("unchanged profile step missing: %s", name)
		}
		end := strings.Index(text[start+len(marker):], "\n      - name:")
		if end < 0 {
			t.Fatalf("step terminator missing: %s", name)
		}
		block := text[start : start+len(marker)+end]
		actual := fmt.Sprintf("%x", sha256.Sum256([]byte(block)))
		if actual != expected {
			t.Fatalf("unrelated profile step changed: %s", name)
		}
	}
}

func TestHostHandoffSourceCheckoutsHaveIndependentImmutableBindings(t *testing.T) {
	for _, path := range []string{bookServingWorkflow, legacyWebRWorkflow} {
		text := readWorkflow(t, path)
		start := strings.Index(text, "      - name: Checkout immutable read-only host handoff source\n")
		end := strings.Index(text, "      - name: Checkout immutable read-only SQL source\n")
		if start < 0 || end <= start {
			t.Fatal("fixed handoff source checkout absent")
		}
		data := text[start:end]
		if !strings.Contains(data, "repository: statground/Statground_Data_NAVER_Book") ||
			!strings.Contains(data, "path: .host-handoff/data") || !strings.Contains(data, "persist-credentials: false") ||
			strings.Contains(data, "token:") || strings.Contains(data, "github.sha") || strings.Contains(data, "UNBOUND") {
			t.Fatal("helper checkout is not independently pinned public source")
		}
		if !strings.Contains(text, "ref: 0913a89bf171dbc6a3b104b87795ce6ff8231d47") ||
			!strings.Contains(text, `git -C "$root" hash-object "$relative"`) ||
			!strings.Contains(text, "--contract") || strings.Contains(text, "continue-on-error") {
			t.Fatal("readonly SQL custody or failure propagation was weakened")
		}
	}
}

func TestWebRRunnerOnlyObservesAndNoncompleteFailsClosed(t *testing.T) {
	for _, mode := range []string{"complete", "pending", "refused", "compatibility_unknown", "forged_ack", "forged_exit", "content_drift", "contradictory_reason"} {
		t.Run(mode, func(t *testing.T) {
			temp := t.TempDir()
			runner := filepath.Join(temp, "runner.py")
			publisher := filepath.Join(temp, "publisher.py")
			helper := filepath.Join(temp, "handoff.py")
			outputPath := filepath.Join(temp, "outputs")
			for path, body := range map[string]string{
				runner: extractBookServingRunner(t), publisher: "raise SystemExit('WebR mutation publisher must never run')\n",
				helper: fakeHostHandoff(mode),
			} {
				if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			env := bookServingRunnerEnvironment(temp, publisher, "host-case")
			env = append(env, "BOOK_HOST_HANDOFF_PATH="+helper, "BOOK_HOST_READONLY_SQL_ROOT="+temp, "GITHUB_OUTPUT="+outputPath)
			preflight := exec.Command("python3", runner, "--service", "webr", "--preflight-only")
			preflight.Env = env
			if output, err := preflight.CombinedOutput(); err != nil {
				t.Fatalf("readonly preflight failed: %v %s", err, output)
			}
			publish := exec.Command("python3", runner, "--service", "webr")
			publish.Env = env
			output, err := publish.CombinedOutput()
			if (err == nil) != (mode == "complete") {
				t.Fatalf("mode=%s error=%v output=%s", mode, err, output)
			}
			for _, secret := range []string{"publisher-secret", "observer-secret", "ca-secret"} {
				if strings.Contains(string(output), secret) {
					t.Fatal("handoff exposed credential")
				}
			}
			fields, _ := os.ReadFile(outputPath)
			if mode == "complete" && !strings.Contains(string(fields), "complete=true\n") {
				t.Fatal("positive completion output missing")
			}
			if mode != "complete" && strings.Contains(string(fields), "complete=true") {
				t.Fatal("noncomplete exported true")
			}
			if mode == "pending" && !strings.Contains(string(fields), "status=pending\ncomplete=false\n") {
				t.Fatal("pending truth not propagated")
			}
		})
	}
}

func extractLegacyHostObservation(t *testing.T) string {
	t.Helper()
	text := readWorkflow(t, legacyWebRWorkflow)
	const marker = "          PUBLISHER_CONFIG=\"$publisher_config\" python3 - <<'PY'\n"
	start := strings.Index(text, marker)
	if start < 0 {
		t.Fatal("legacy readonly handoff wrapper missing")
	}
	start += len(marker)
	end := strings.Index(text[start:], "\n          PY\n")
	if end < 0 {
		t.Fatal("legacy handoff terminator missing")
	}
	lines := strings.Split(text[start:start+end], "\n")
	for index := range lines {
		lines[index] = strings.TrimPrefix(lines[index], "          ")
	}
	return strings.Join(lines, "\n") + "\n"
}

func TestLegacyObservationNeverSubstitutesV2OrIgnoresNoncomplete(t *testing.T) {
	for _, mode := range []string{"complete", "pending", "refused", "compatibility_unknown", "forged_ack", "forged_exit", "contradictory_reason"} {
		t.Run(mode, func(t *testing.T) {
			temp := t.TempDir()
			helper := filepath.Join(temp, ".host-handoff", "data", "scripts", "read_book_host_publication.py")
			if err := os.MkdirAll(filepath.Dir(helper), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(helper, []byte(fakeHostHandoff(mode)), 0o700); err != nil {
				t.Fatal(err)
			}
			wrapper := filepath.Join(temp, "legacy-wrapper.py")
			if err := os.WriteFile(wrapper, []byte(extractLegacyHostObservation(t)), 0o700); err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(temp, "config.json")
			if err := os.WriteFile(config, []byte(`{"password":"publisher-secret"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			outputs := filepath.Join(temp, "outputs")
			command := exec.Command("python3", wrapper)
			command.Env = []string{"PATH=" + os.Getenv("PATH"), "GITHUB_WORKSPACE=" + temp, "PUBLISHER_CONFIG=" + config, "GITHUB_OUTPUT=" + outputs, "PUBLISHER_PASSWORD=must-not-reach-child"}
			output, err := command.CombinedOutput()
			if (err == nil) != (mode == "complete") {
				t.Fatalf("mode=%s error=%v output=%s", mode, err, output)
			}
			if strings.Contains(string(output), "publisher-secret") || strings.Contains(string(output), "must-not-reach-child") {
				t.Fatal("legacy leaked a secret")
			}
			fields, _ := os.ReadFile(outputs)
			if mode != "complete" && strings.Contains(string(fields), "complete=true") {
				t.Fatal("legacy noncomplete exported true")
			}
			if mode == "compatibility_unknown" && !strings.Contains(string(fields), "complete=false") {
				t.Fatal("legacy compatibility gap concealed")
			}
		})
	}
}

func fakeHostHandoff(mode string) string {
	return `import argparse,json,os,stat
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--contract',required=True)
p.add_argument('--config',required=True)
p.add_argument('--sql-source-root',required=True)
p.add_argument('--legacy-source-root')
p.add_argument('--expected-json')
p.add_argument('--preflight-only',action='store_true')
a=p.parse_args()
for key in ('PUBLISHER_PASSWORD','COORDINATOR_ENDPOINT','NAVER_OUTBOX_ENDPOINT','OUTBOX_OBSERVER_PASSWORD','GITHUB_TOKEN','ACTIONS_RUNTIME_TOKEN'):
 if key in os.environ:raise SystemExit('secret environment reached readonly helper')
c=json.loads(Path(a.config).read_text())
if (c.get('coordinator',c)).get('password')!='publisher-secret':raise SystemExit('wrong selected config')
if stat.S_IMODE(Path(a.config).stat().st_mode)!=0o600:raise SystemExit('not private')
pre={'status':'preflight_ok','database_writes':False,'service':'webr','source_refresh_started_at':'2026-09-20 00:00:00.000','source_refresh_success_at':'2026-09-20 00:10:00.000','source_generation':'2026-09-20 00:05:00.000','source_rows':7,'raw_target_rows':7,'content_sha256':'a'*64,'policy_authority_revision':9}
if a.preflight_only:
 if a.contract!='public-v2' or a.expected_json:raise SystemExit('wrong preflight interface')
 print(json.dumps(pre));raise SystemExit(0)
if a.contract=='public-v2':
 if not a.expected_json or json.loads(Path(a.expected_json).read_text())!=pre:raise SystemExit('preflight not bound')
elif a.contract=='legacy':
 if not a.legacy_source_root or a.expected_json:raise SystemExit('legacy selector not isolated')
else:raise SystemExit('wrong contract')
mode='` + mode + `'
status=mode if mode in ('complete','pending','refused','compatibility_unknown')else 'complete'
reason={'complete':'observed_complete','pending':'awaiting_authoritative_host_publication','refused':'readonly_request_refused','compatibility_unknown':'legacy_contract_not_observed'}[status]
if mode=='contradictory_reason':reason='budget_exhausted'
v={'schema':'sg.book.host-publication-observation.v1','contract':a.contract,'service':'webr','status':status,'complete':status=='complete','reason':reason,'observed_state':status!='refused','wire_ack_proved':mode=='forged_ack','host_request_sent':False,'source_manifest_sha256':'c'*64,'content_sha256':('b'if mode=='content_drift'else'a')*64,'rows':7}
print(json.dumps(v))
raise SystemExit(2 if mode=='forged_exit'else 0 if status=='complete'else 1 if status=='refused'else 2)
`
}

func TestBothUnaffectedProfilesRetainDirectPublisherBehavior(t *testing.T) {
	for _, service := range []string{"mirtype", "statground-provider"} {
		t.Run(service, func(t *testing.T) {
			temp := t.TempDir()
			runner, publisher := filepath.Join(temp, "runner.py"), filepath.Join(temp, "publisher.py")
			if err := os.WriteFile(runner, []byte(extractBookServingRunner(t)), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(publisher, []byte(fakeBookServingPublisher(false, false)), 0o700); err != nil {
				t.Fatal(err)
			}
			env := bookServingRunnerEnvironment(temp, publisher, "untouched-profile", service)
			for _, args := range [][]string{{runner, "--service", service, "--preflight-only"}, {runner, "--service", service}} {
				command := exec.Command("python3", args...)
				command.Env = env // No handoff path supplied: either profile must keep its direct route.
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("profile=%s failed: %v %s", service, err, output)
				}
				if !strings.Contains(string(output), `"service":"`+service+`"`) {
					t.Fatal("selected profile result changed")
				}
				for _, secret := range []string{"publisher-secret", "observer-secret", "ca-secret"} {
					if strings.Contains(string(output), secret) {
						t.Fatal("direct profile leaked credentials")
					}
				}
			}
		})
	}
}
