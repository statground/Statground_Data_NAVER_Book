package main

import (
	"reflect"
	"testing"
	"time"

	"statground_naver_book_go/internal/kakaocollector"
)

func TestSplitQueriesNormalizesAndDeduplicatesWithoutCommaSplitting(t *testing.T) {
	got := splitQueries("  R, statistics  ; language   learning\nR, Statistics\n")
	want := []string{"R, statistics", "language learning"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitQueries=%#v want=%#v", got, want)
	}
}

func TestCollectionSummaryDistinguishesAllFrontierDueSkips(t *testing.T) {
	summary := collectionSummary{plannedRequests: 4}
	for range 4 {
		summary.recordResult(kakaocollector.Result{SkippedDue: true})
	}
	want := "provider=kakao status=completed calls=0 fetched=0 inserted=0 new_isbn=0 changed_isbn=0 duplicates=0 planned_requests=4 processed_requests=0 skipped_due_requests=4"
	if got := summary.String(); got != want {
		t.Fatalf("summary=%q want=%q", got, want)
	}
}

func TestCollectionSummaryRetainsMixedRequestAndBookTotals(t *testing.T) {
	summary := collectionSummary{plannedRequests: 4}
	for _, result := range []kakaocollector.Result{
		{SkippedDue: true},
		{Calls: 2, Fetched: 7, Inserted: 4, NewISBN: 3, ChangedISBN: 1, Duplicates: 3},
		{SkippedDue: true},
		{Calls: 1, Fetched: 5, Inserted: 2, NewISBN: 1, ChangedISBN: 1, Duplicates: 3},
	} {
		summary.recordResult(result)
	}
	want := "provider=kakao status=completed calls=3 fetched=12 inserted=6 new_isbn=4 changed_isbn=2 duplicates=6 planned_requests=4 processed_requests=2 skipped_due_requests=2"
	if got := summary.String(); got != want {
		t.Fatalf("summary=%q want=%q", got, want)
	}
}

func TestCollectionSummaryDoesNotCountUnattemptedBudgetRemainder(t *testing.T) {
	summary := collectionSummary{plannedRequests: 3}
	summary.recordResult(kakaocollector.Result{Calls: 1})
	if summary.processedRequests != 1 || summary.skippedDueRequests != 0 || summary.plannedRequests != 3 {
		t.Fatalf("summary=%s", summary)
	}
}

func TestScheduleFlagDefaultsDisabled(t *testing.T) {
	t.Setenv("KAKAO_BOOK_SCHEDULE_ENABLED", "")
	if boolEnv("KAKAO_BOOK_SCHEDULE_ENABLED", false) {
		t.Fatal("Kakao schedule must default disabled")
	}
	t.Setenv("KAKAO_BOOK_SCHEDULE_ENABLED", "true")
	if !boolEnv("KAKAO_BOOK_SCHEDULE_ENABLED", false) {
		t.Fatal("explicit Kakao schedule enable was ignored")
	}
}

func TestRunScheduledDisabledBeforeReadingSecrets(t *testing.T) {
	t.Setenv("KAKAO_RUN_KIND", "scheduled")
	t.Setenv("KAKAO_BOOK_SCHEDULE_ENABLED", "")
	t.Setenv("KAKAO_REST_API_KEY", "")
	t.Setenv("CH_HOST", "")
	if err := run(); err != nil {
		t.Fatalf("disabled scheduled run should stop before secret or DB access: %v", err)
	}
}

func TestOperationalHoldDurationsCanBeConfigured(t *testing.T) {
	t.Setenv("KAKAO_QUOTA_EXHAUSTED_HOLD_HOURS", "12")
	t.Setenv("KAKAO_RATE_LIMIT_HOLD_MINUTES", "15")
	if got := durationHours("KAKAO_QUOTA_EXHAUSTED_HOLD_HOURS", 24*time.Hour); got != 12*time.Hour {
		t.Fatalf("quota hold=%s", got)
	}
	if got := durationMinutes("KAKAO_RATE_LIMIT_HOLD_MINUTES", 30*time.Minute); got != 15*time.Minute {
		t.Fatalf("rate hold=%s", got)
	}
}

func TestSafeErrorIncludesOnlyPreSanitizedStage(t *testing.T) {
	err := (&safeError{category: "clickhouse_contract", stage: "insert_call_log", reason: "read_timeout"}).Error()
	if err != "kakao book collection failed category=clickhouse_contract stage=insert_call_log reason=read_timeout" {
		t.Fatalf("safe error=%q", err)
	}
}
