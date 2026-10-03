package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/lexicon"
	"statground_naver_book_go/internal/provider"
	"statground_naver_book_go/internal/quota"
)

func discoveryTestEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("LEXICON_STATE_DIR", t.TempDir())
	t.Setenv("LEXICON_RUN_ID", "kakao-test-run")
	t.Setenv("KAKAO_DISCOVERY_KEYWORD_LIMIT", "")
}

func TestDictionaryDiscoveryPreservesManualQueriesWithoutReader(t *testing.T) {
	discoveryTestEnvironment(t)
	t.Setenv("KAKAO_LEXICON_ENABLED", "")
	curated := []string{"통계학", "R 프로그래밍", "외국어 학습", "언어 학습"}
	batch, err := selectDiscoveryQueries(context.Background(), curated, "manual", time.Now(), func(context.Context, string) ([]lexicon.Candidate, string, error) {
		t.Fatal("manual fixed-keyword collection read the dictionary")
		return nil, "", nil
	})
	if err != nil || batch.Degraded || len(batch.Selections) != len(curated) {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	for index, selection := range batch.Selections {
		if selection.Keyword != curated[index] || selection.Origin != "curated" {
			t.Fatalf("manual selection=%+v", selection)
		}
	}
}

func TestDictionaryDiscoveryIsBalancedWithinExistingBatch(t *testing.T) {
	discoveryTestEnvironment(t)
	t.Setenv("KAKAO_LEXICON_ENABLED", "true")
	pool := []lexicon.Candidate{
		{Keyword: "회귀", NormalizedWord: "회귀", Language: "ko", Confidence: 0.99, Sources: []string{"Data_Book_KAKAO_Raw.kakao_book_raw"}, SnapshotID: strings.Repeat("a", 64)},
		{Keyword: "분석", NormalizedWord: "분석", Language: "ko", Confidence: 0.99, Sources: []string{"Data_Book_KAKAO_Raw.kakao_book_raw"}, SnapshotID: strings.Repeat("a", 64)},
		{Keyword: "모형", NormalizedWord: "모형", Language: "ko", Confidence: 0.99, Sources: []string{"Data_Book_KAKAO_Raw.kakao_book_raw"}, SnapshotID: strings.Repeat("a", 64)},
	}
	loader := func(_ context.Context, scope string) ([]lexicon.Candidate, string, error) {
		if scope != "kakao-book" {
			t.Fatalf("scope=%q", scope)
		}
		return pool, strings.Repeat("a", 64), nil
	}
	curated := []string{"통계학", "R 프로그래밍", "외국어 학습", "언어 학습"}
	batch, err := selectDiscoveryQueries(context.Background(), curated, "scheduled", time.Now(), loader)
	legacy, nouns := discoveryComposition(batch.Selections)
	if err != nil || batch.Degraded || len(batch.Selections) != 4 || legacy != 2 || nouns != 2 {
		t.Fatalf("batch=%+v curated=%d dictionary=%d err=%v", batch, legacy, nouns, err)
	}
	// The receipt retains the exact batch even if the committed dictionary
	// snapshot gains or loses candidates before the same run is retried.
	pool = append(pool, lexicon.Candidate{Keyword: "확률", NormalizedWord: "확률", Language: "ko", Confidence: 0.99, Sources: []string{"book"}, SnapshotID: strings.Repeat("b", 64)})
	retry, err := selectDiscoveryQueries(context.Background(), curated, "scheduled", time.Now(), loader)
	if err != nil || !reflect.DeepEqual(batch.Selections, retry.Selections) {
		t.Fatalf("retry=%+v original=%+v err=%v", retry, batch, err)
	}
}

func TestDictionaryDiscoveryFailureDefersWithoutReplacingNounHalf(t *testing.T) {
	discoveryTestEnvironment(t)
	t.Setenv("KAKAO_LEXICON_ENABLED", "true")
	t.Setenv("KAKAO_DISCOVERY_KEYWORD_LIMIT", "2")
	batch, err := selectDiscoveryQueries(context.Background(), []string{"a", "b", "c", "d"}, "scheduled", time.Now(), func(context.Context, string) ([]lexicon.Candidate, string, error) {
		return nil, "", errors.New("private credential-bearing failure")
	})
	if err == nil || len(batch.Selections) != 0 || strings.Contains(err.Error(), "credential") {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestDictionaryDiscoveryShortPoolShrinksBothHalves(t *testing.T) {
	discoveryTestEnvironment(t)
	t.Setenv("KAKAO_LEXICON_ENABLED", "true")
	batch, err := selectDiscoveryQueries(context.Background(), []string{"통계학", "언어 학습", "프로그래밍", "외국어"}, "scheduled", time.Now(), func(context.Context, string) ([]lexicon.Candidate, string, error) {
		return []lexicon.Candidate{{Keyword: "확률", NormalizedWord: "확률", Language: "ko", Confidence: .99, Sources: []string{"book"}, SnapshotID: strings.Repeat("a", 64)}}, strings.Repeat("a", 64), nil
	})
	curated, nouns := discoveryComposition(batch.Selections)
	if err != nil || len(batch.Selections) != 2 || curated != 1 || nouns != 1 {
		t.Fatalf("short pool replaced the noun half: batch=%+v err=%v", batch, err)
	}
}

func TestDictionaryDiscoveryReceiptConflictNeverReplacesRetryWithCuratedBatch(t *testing.T) {
	discoveryTestEnvironment(t)
	t.Setenv("KAKAO_LEXICON_ENABLED", "true")
	loader := func(context.Context, string) ([]lexicon.Candidate, string, error) {
		return []lexicon.Candidate{{Keyword: "확률", NormalizedWord: "확률", Language: "ko", Confidence: .99, Sources: []string{"book"}, SnapshotID: strings.Repeat("a", 64)}}, strings.Repeat("a", 64), nil
	}
	if _, err := selectDiscoveryQueries(context.Background(), []string{"통계학", "언어 학습"}, "scheduled", time.Now(), loader); err != nil {
		t.Fatalf("initial selection: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(os.Getenv("LEXICON_STATE_DIR"), "*.json"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("receipt paths=%v err=%v", paths, err)
	}
	if err := os.WriteFile(paths[0], []byte("incomplete receipt"), 0600); err != nil {
		t.Fatal(err)
	}
	batch, err := selectDiscoveryQueries(context.Background(), []string{"통계학", "언어 학습"}, "scheduled", time.Now(), loader)
	if err == nil || len(batch.Selections) != 0 || batch.Degraded {
		t.Fatalf("corrupt receipt replaced the batch: %+v err=%v", batch, err)
	}
}

func TestDictionaryDiscoveryRejectsCapExpansionAndLeavesOddSlotUnused(t *testing.T) {
	discoveryTestEnvironment(t)
	t.Setenv("KAKAO_LEXICON_ENABLED", "true")
	loads := 0
	loader := func(context.Context, string) ([]lexicon.Candidate, string, error) {
		loads++
		return nil, "", errors.New("unavailable")
	}
	for _, limit := range []string{"0", "1", "5"} {
		t.Setenv("KAKAO_DISCOVERY_KEYWORD_LIMIT", limit)
		if _, err := selectDiscoveryQueries(context.Background(), []string{"a", "b", "c", "d"}, "scheduled", time.Now(), loader); err == nil {
			t.Fatalf("limit=%s accepted", limit)
		}
	}
	if loads != 0 {
		t.Fatalf("invalid cap read dictionary %d times", loads)
	}
	t.Setenv("KAKAO_DISCOVERY_KEYWORD_LIMIT", "3")
	batch, err := selectDiscoveryQueries(context.Background(), []string{"a", "b", "c", "d"}, "scheduled", time.Now(), func(context.Context, string) ([]lexicon.Candidate, string, error) {
		return []lexicon.Candidate{{Keyword: "확률", NormalizedWord: "확률", Language: "ko", Confidence: .99, Sources: []string{"book"}, SnapshotID: strings.Repeat("a", 64)}}, strings.Repeat("a", 64), nil
	})
	if err != nil || len(batch.Selections) != 2 {
		t.Fatalf("odd-cap batch=%+v err=%v", batch, err)
	}
}

func TestDueAndQuotaRemovalsRetainBalancedAttemptPairs(t *testing.T) {
	batch := discoveryBatch{Selections: []lexicon.Selection{
		{Keyword: "curated-a", Origin: "curated"},
		{Keyword: "curated-b", Origin: "curated"},
		{Keyword: "noun-a", Origin: "lexicon"},
		{Keyword: "noun-b", Origin: "lexicon"},
	}}
	candidates := []quota.Candidate{}
	for _, query := range []string{"curated-a", "noun-a", "noun-b"} {
		candidates = append(candidates, quota.Candidate{Request: provider.SearchRequest{Query: query}, EstimatedCalls: 1, Priority: 0.5})
	}
	balanced := balanceDiscoveryCandidates(candidates, batch)
	if len(balanced) != 2 || balanced[0].Request.Query != "curated-a" || balanced[1].Request.Query != "noun-a" {
		t.Fatalf("balanced=%+v", balanced)
	}
	if got := balanceDiscoveryCandidates(candidates[1:], batch); len(got) != 0 {
		t.Fatalf("dictionary-only due remainder executed: %+v", got)
	}
	cfg := quota.DefaultConfig()
	cfg.MaxRequestsPerRun = 1
	budget, _ := quota.NewBudget(cfg, 0)
	plan := balanceDiscoveryPlan(quota.BuildPlan(balanced, budget), batch)
	if len(plan.Selected) != 0 || plan.PlannedCalls != 0 {
		t.Fatalf("one-slot quota remainder executed an unbalanced plan: %+v", plan)
	}
}

func TestDiscoveryPlanOnlyDoesNotRequireProviderOrCollectionCredentials(t *testing.T) {
	discoveryTestEnvironment(t)
	t.Setenv("KAKAO_DISCOVERY_PLAN_ONLY", "true")
	t.Setenv("KAKAO_QUERIES", "통계학;언어 학습")
	t.Setenv("KAKAO_RUN_KIND", "manual")
	t.Setenv("KAKAO_LEXICON_ENABLED", "false")
	t.Setenv("KAKAO_REST_API_KEY", "")
	t.Setenv("CH_HOST", "")
	if err := run(); err != nil {
		t.Fatalf("secret-free discovery plan failed: %v", err)
	}
}
