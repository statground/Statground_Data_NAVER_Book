package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"statground_naver_book_go/internal/kakaocollector"
	"statground_naver_book_go/internal/kakaostore"
	"statground_naver_book_go/internal/provider"
	"statground_naver_book_go/internal/quota"
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
	want := "provider=kakao status=completed calls=0 fetched=0 inserted=0 new_isbn=0 changed_isbn=0 duplicates=0 planned_requests=4 processed_requests=0 skipped_due_requests=4 candidate_requests=0 deferred_before_planning_requests=0"
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
	want := "provider=kakao status=completed calls=3 fetched=12 inserted=6 new_isbn=4 changed_isbn=2 duplicates=6 planned_requests=4 processed_requests=2 skipped_due_requests=2 candidate_requests=0 deferred_before_planning_requests=0"
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

type frontierReaderFunc func(context.Context, kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error)

func (f frontierReaderFunc) LoadFrontier(ctx context.Context, key kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error) {
	return f(ctx, key)
}

func TestFrontierEligibilityPreservesDueCandidateNearBudget(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	candidates := []quota.Candidate{
		{Request: provider.SearchRequest{Query: "a_future"}, EstimatedCalls: 1, Priority: 0.5},
		{Request: provider.SearchRequest{Query: "b_due"}, EstimatedCalls: 1, Priority: 0.5},
	}
	reader := frontierReaderFunc(func(_ context.Context, key kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error) {
		due := now
		if key.Query == "a_future" {
			due = now.Add(time.Hour)
		}
		return kakaostore.FrontierSnapshot{Found: true, State: quota.FrontierState{Active: true}, NextDueAt: due}, nil
	})
	cfg := quota.DefaultConfig()
	cfg.AutoDailyBudget = 2000
	cfg.MaxRequestsPerRun = 2000
	baseline, _ := quota.NewBudget(cfg, 11999)
	old := quota.BuildPlan(candidates, baseline)
	if len(old.Selected) != 1 || old.Selected[0].Request.Query != "a_future" {
		t.Fatalf("baseline %+v", old)
	}
	budget, _ := quota.NewBudget(cfg, 11999)
	eligible, deferred, err := frontierEligibleCandidates(context.Background(), candidates, reader, "fixed_keyword", true, now)
	if err != nil || deferred != 1 || budget.Snapshot().ReservedCallsThisRun != 0 {
		t.Fatalf("prefilter deferred=%d err=%v", deferred, err)
	}
	plan := quota.BuildPlan(eligible, budget)
	if len(plan.Selected) != 1 || plan.Selected[0].Request.Query != "b_due" || plan.PlannedCalls != 1 {
		t.Fatalf("plan %+v", plan)
	}
}

func TestFrontierEligibilityBoundariesAndIdentity(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		snapshot kakaostore.FrontierSnapshot
		deferred int
	}{
		{"missing", kakaostore.FrontierSnapshot{}, 0},
		{"inactive", kakaostore.FrontierSnapshot{Found: true}, 1},
		{"zero_due", kakaostore.FrontierSnapshot{Found: true, State: quota.FrontierState{Active: true}}, 0},
		{"exact_due", kakaostore.FrontierSnapshot{Found: true, State: quota.FrontierState{Active: true}, NextDueAt: now}, 0},
		{"past_due", kakaostore.FrontierSnapshot{Found: true, State: quota.FrontierState{Active: true}, NextDueAt: now.Add(-time.Nanosecond)}, 0},
		{"future", kakaostore.FrontierSnapshot{Found: true, State: quota.FrontierState{Active: true}, NextDueAt: now.Add(time.Nanosecond)}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			candidate := quota.Candidate{Request: provider.SearchRequest{Query: "  R   statistics ", Sort: " LATEST ", Target: " TITLE "}, EstimatedCalls: 1}
			reader := frontierReaderFunc(func(_ context.Context, key kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error) {
				calls++
				want := kakaostore.FrontierKey{Provider: "kakao", Mode: "fixed_keyword", Query: "R statistics", Target: "title", Sort: "latest"}
				if key != want || kakaocollector.QueryHash(candidate.Request, key.Mode) != kakaocollector.QueryHash(provider.SearchRequest{Query: key.Query, Target: key.Target, Sort: key.Sort}, "fixed_keyword") {
					t.Fatalf("identity %+v", key)
				}
				return tc.snapshot, nil
			})
			got, deferred, err := frontierEligibleCandidates(context.Background(), []quota.Candidate{candidate}, reader, " fixed_keyword ", true, now)
			if err != nil || calls != 1 || deferred != tc.deferred || len(got) != 1-tc.deferred {
				t.Fatalf("got=%d deferred=%d calls=%d err=%v", len(got), deferred, calls, err)
			}
		})
	}
}

func TestFrontierEligibilityAllFutureAndAllDue(t *testing.T) {
	now := time.Now()
	candidates := []quota.Candidate{}
	for _, q := range []string{"a", "b", "c", "d"} {
		candidates = append(candidates, quota.Candidate{Request: provider.SearchRequest{Query: q}, EstimatedCalls: 1})
	}
	for _, future := range []bool{false, true} {
		reader := frontierReaderFunc(func(_ context.Context, key kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error) {
			if key.Mode != "manual" || key.Sort != "accuracy" {
				t.Fatalf("defaults %+v", key)
			}
			due := now
			if future {
				due = now.Add(time.Hour)
			}
			return kakaostore.FrontierSnapshot{Found: true, State: quota.FrontierState{Active: true}, NextDueAt: due}, nil
		})
		got, deferred, err := frontierEligibleCandidates(context.Background(), candidates, reader, "", true, now)
		if err != nil || len(got)+deferred != 4 || (future && deferred != 4) || (!future && len(got) != 4) {
			t.Fatalf("got=%d deferred=%d err=%v", len(got), deferred, err)
		}
		if future {
			summary := collectionSummary{candidateRequests: 4, deferredBeforePlanningRequests: deferred}
			want := "provider=kakao status=completed calls=0 fetched=0 inserted=0 new_isbn=0 changed_isbn=0 duplicates=0 planned_requests=0 processed_requests=0 skipped_due_requests=0 candidate_requests=4 deferred_before_planning_requests=4"
			if summary.String() != want {
				t.Fatal(summary.String())
			}
		}
	}
}

func TestFrontierEligibilityManualBypassAndReadErrorFailClosed(t *testing.T) {
	candidates := []quota.Candidate{{Request: provider.SearchRequest{Query: "a"}, EstimatedCalls: 1}, {Request: provider.SearchRequest{Query: "b"}, EstimatedCalls: 1}}
	calls := 0
	cause := errors.New("private cause")
	reader := frontierReaderFunc(func(context.Context, kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error) {
		calls++
		if calls == 2 {
			return kakaostore.FrontierSnapshot{}, cause
		}
		return kakaostore.FrontierSnapshot{}, nil
	})
	got, deferred, err := frontierEligibleCandidates(context.Background(), candidates, reader, "manual", false, time.Now())
	if err != nil || deferred != 0 || calls != 0 || !reflect.DeepEqual(got, candidates) {
		t.Fatalf("manual got=%v deferred=%d reads=%d err=%v", got, deferred, calls, err)
	}
	budget, _ := quota.NewBudget(quota.DefaultConfig(), 0)
	got, deferred, err = frontierEligibleCandidates(context.Background(), candidates, reader, "fixed_keyword", true, time.Now())
	if !errors.Is(err, cause) || got != nil || deferred != 0 || budget.Snapshot().ReservedCallsThisRun != 0 {
		t.Fatalf("error got=%v deferred=%d budget=%+v err=%v", got, deferred, budget.Snapshot(), err)
	}
}

func TestFrontierEligibilityPreservesInvalidPlanningCountsWithoutReads(t *testing.T) {
	candidates := []quota.Candidate{{Request: provider.SearchRequest{Query: " "}, EstimatedCalls: 1}, {Request: provider.SearchRequest{Query: "valid"}, EstimatedCalls: 0}}
	reader := frontierReaderFunc(func(context.Context, kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error) {
		t.Fatal("invalid candidate read frontier")
		return kakaostore.FrontierSnapshot{}, nil
	})
	got, deferred, err := frontierEligibleCandidates(context.Background(), candidates, reader, "fixed_keyword", true, time.Now())
	budget, _ := quota.NewBudget(quota.DefaultConfig(), 0)
	plan := quota.BuildPlan(got, budget)
	if err != nil || deferred != 0 || len(plan.Selected) != 0 || plan.SkippedInvalid != 2 || budget.Snapshot().ReservedCallsThisRun != 0 {
		t.Fatalf("plan=%+v deferred=%d err=%v", plan, deferred, err)
	}
}
