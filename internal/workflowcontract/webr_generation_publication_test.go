package workflowcontract

import (
	"strings"
	"testing"
)

const (
	webrGenerationPublisherCommit = "27cbfecbb883c6c834ae2c58be74dad47c690b4d"
	webrGenerationWorkflow        = "../../.github/workflows/webr_book_generation_publish.yml"
)

func TestWebRGenerationPublisherUsesPinnedSQLSourceAndOwnerOnlyConfig(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, webrGenerationWorkflow)
	for _, contract := range []string{
		"repository: statground/Statground_SQL",
		"ref: " + webrGenerationPublisherCommit,
		"token: ${{ secrets.STATGROUND_SQL_READ_TOKEN }}",
		"persist-credentials: false",
		"sparse-checkout: docker-compose/50005_Clickhouse_Statground/webr_book_generation_publisher.py",
		`readonly expected_sql_commit="` + webrGenerationPublisherCommit + `"`,
		`test "$(git -C "$sql_root" rev-parse HEAD)" = "$expected_sql_commit"`,
		"umask 077",
		"os.O_EXCL",
		`getattr(os, "O_NOFOLLOW", 0)`,
		"0o600",
		`"tls": {"verify": True`,
		"unset PUBLISHER_ENDPOINT PUBLISHER_USER PUBLISHER_PASSWORD PUBLISHER_CA_PEM",
		"--transport http",
		`--config "$publisher_config"`,
		"--timeout-seconds 120",
		"publish",
	} {
		if !strings.Contains(text, contract) {
			t.Errorf("publisher workflow is missing contract %q", contract)
		}
	}
	if count := strings.Count(text, webrGenerationPublisherCommit); count != 2 {
		t.Fatalf("immutable SQL publisher commit count=%d, want checkout and readback", count)
	}
	if strings.Contains(text, "github.token") || strings.Contains(text, "continue-on-error") {
		t.Fatal("enabled publication can fall back to a repository token or ignore failure")
	}
	for _, generic := range []string{
		"secrets.CLICKHOUSE_USER",
		"secrets.CH_USER",
		"secrets.CLICKHOUSE_PASSWORD",
		"secrets.CH_PASSWORD",
	} {
		if strings.Contains(text, generic) {
			t.Errorf("publisher workflow reuses generic collector credential %q", generic)
		}
	}
	if !strings.Contains(text, `user != "webr_book_generation_publisher"`) {
		t.Fatal("publisher workflow does not enforce the dedicated principal identity")
	}
}

func TestWebRGenerationPublicationIsGatedAfterSuccessfulRefreshJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		needs      string
		condition  string
		refreshCue string
	}{
		{
			name:       "naver",
			path:       "../../.github/workflows/naver_book_collect_all.yml",
			needs:      "needs: collect_all",
			condition:  "success() && needs.collect_all.outputs.webr_refresh_verified == 'true' && vars.WEBR_BOOK_GENERATION_PUBLISH_ENABLED == 'true' && (inputs.stage == 'all' || inputs.stage == 'refresh') && github.event_name == 'workflow_dispatch'",
			refreshCue: "Refresh Web-R and MirType book catalogs",
		},
		{
			name:       "kakao",
			path:       "../../.github/workflows/kakao_book_collect.yml",
			needs:      "needs: collect",
			condition:  "success() && needs.collect.outputs.webr_refresh_verified == 'true' && vars.WEBR_BOOK_GENERATION_PUBLISH_ENABLED == 'true' && inputs.dry_run != true",
			refreshCue: "Refresh provider-neutral serving catalogs for manual runs",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			text := readWorkflow(t, test.path)
			for _, contract := range []string{
				"publish_webr_generation:",
				test.needs,
				test.condition,
				"uses: ./.github/workflows/webr_book_generation_publish.yml",
				"STATGROUND_SQL_READ_TOKEN: ${{ secrets.STATGROUND_SQL_READ_TOKEN }}",
				"WEBR_BOOK_PUBLISHER_ENDPOINT: ${{ secrets.WEBR_BOOK_PUBLISHER_ENDPOINT }}",
				"WEBR_BOOK_PUBLISHER_USER: ${{ secrets.WEBR_BOOK_PUBLISHER_USER }}",
				"WEBR_BOOK_PUBLISHER_PASSWORD: ${{ secrets.WEBR_BOOK_PUBLISHER_PASSWORD }}",
				"webr_refresh_verified:",
				`echo "verified=true" >> "$GITHUB_OUTPUT"`,
				test.refreshCue,
			} {
				if !strings.Contains(text, contract) {
					t.Errorf("%s workflow is missing publication contract %q", test.name, contract)
				}
			}
			if count := strings.Count(text, "vars.WEBR_BOOK_GENERATION_PUBLISH_ENABLED == 'true'"); count != 1 {
				t.Errorf("%s feature-gate count=%d, want one", test.name, count)
			}
			if strings.Contains(text, "--transport http") {
				t.Errorf("%s workflow duplicated the pinned SQL publisher", test.name)
			}
		})
	}
}

func TestKakaoScheduleForwardsOnlyDedicatedPublisherInputs(t *testing.T) {
	t.Parallel()

	text := readWorkflow(t, "../../.github/workflows/kakao_book_schedule.yml")
	for _, name := range []string{
		"STATGROUND_SQL_READ_TOKEN",
		"WEBR_BOOK_PUBLISHER_ENDPOINT",
		"WEBR_BOOK_PUBLISHER_USER",
		"WEBR_BOOK_PUBLISHER_PASSWORD",
		"WEBR_BOOK_PUBLISHER_CA_PEM",
	} {
		contract := name + ": ${{ secrets." + name + " }}"
		if count := strings.Count(text, contract); count != 1 {
			t.Errorf("Kakao schedule publisher secret %s count=%d, want one", name, count)
		}
	}
}
