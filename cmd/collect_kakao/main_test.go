package main

import (
	"reflect"
	"testing"
	"time"
)

func TestSplitQueriesNormalizesAndDeduplicatesWithoutCommaSplitting(t *testing.T) {
	got := splitQueries("  R, statistics  ; language   learning\nR, Statistics\n")
	want := []string{"R, statistics", "language learning"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitQueries=%#v want=%#v", got, want)
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
	err := (&safeError{category: "clickhouse_contract", stage: "insert_call_log"}).Error()
	if err != "kakao book collection failed category=clickhouse_contract stage=insert_call_log" {
		t.Fatalf("safe error=%q", err)
	}
}
