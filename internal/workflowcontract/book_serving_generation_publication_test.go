package workflowcontract

import (
	"strings"
	"testing"
)

const (
	bookServingPublisherCommit = "d06b7b32a1d8c6c992f3bf9e997b5cdb1a7345ae"
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
		"group: statground-book-serving-generation-publisher",
		"cancel-in-progress: false",
		"queue: max",
		"umask 077",
		"os.O_EXCL",
		`getattr(os, "O_NOFOLLOW", 0)`,
		"0o600",
		`"webr": ("webr_book_serving_publisher"`,
		`"mirtype": ("mirtype_book_serving_publisher"`,
		`"statground_provider_book_serving_publisher"`,
		`"user": "book_serving_outbox_observer"`,
		`"Data_Book_NAVER_Log.naver_direct_insert_outbox"`,
		`"Data_Book_KAKAO_Log.kakao_direct_insert_outbox"`,
		`"coordinator_host": "Clickhouse_S1_R1"`,
		"--preflight-only",
		"trap 'rm -f --",
		"unset COORDINATOR_ENDPOINT",
	} {
		if !strings.Contains(text, contract) {
			t.Errorf("three-profile publisher workflow is missing contract %q", contract)
		}
	}
	if count := strings.Count(text, bookServingPublisherCommit); count != 2 {
		t.Fatalf("immutable SQL publisher commit count=%d, want checkout and readback", count)
	}
	loop := "for service in webr mirtype statground-provider; do"
	if count := strings.Count(text, loop); count != 2 {
		t.Fatalf("serial three-profile loop count=%d, want preflight and publish", count)
	}
	preflight := strings.Index(text, `--service "$service" --preflight-only`)
	publish := strings.LastIndex(text, `--service "$service"`)
	if preflight < 0 || publish <= preflight {
		t.Fatal("all-profile preflight does not precede serial publication")
	}
	if strings.Contains(text, "webr_book_generation_publisher.py") ||
		strings.Contains(text, "continue-on-error") || strings.Contains(text, "github.token") ||
		strings.Contains(text, "secrets.CLICKHOUSE_USER") ||
		strings.Contains(text, "secrets.CLICKHOUSE_PASSWORD") {
		t.Fatal("three-profile workflow can use a legacy publisher, ignore failure, or reuse collector credentials")
	}
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
	legacy := readWorkflow(t, legacyWebRWorkflow)
	if !strings.Contains(legacy, "ref: 27cbfecbb883c6c834ae2c58be74dad47c690b4d") ||
		!strings.Contains(legacy, "webr_book_generation_publisher.py") {
		t.Fatal("mutually exclusive migration fallback no longer uses its reviewed immutable source")
	}
}
