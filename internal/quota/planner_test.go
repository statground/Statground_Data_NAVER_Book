package quota

import (
	"testing"

	"statground_naver_book_go/internal/provider"
)

func TestBuildPlanPrioritizesYieldAndDeduplicates(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.MaxRequestsPerRun = 3
	cfg.AutoDailyBudget = 3
	cfg.ManualDailyReserve = 1
	cfg.DaumSearchSharedReserve = 1
	cfg.EmergencyReserve = 1
	cfg.OfficialDailyLimit = 6
	budget, err := NewBudget(cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	candidates := []Candidate{
		{
			Request:        provider.SearchRequest{Query: "low yield", Sort: "accuracy", Page: 1, Size: 50},
			EstimatedCalls: 2,
			Priority:       0.1,
		},
		{
			Request:        provider.SearchRequest{Query: "High   Yield", Sort: "latest", Page: 1, Size: 50},
			EstimatedCalls: 2,
			Priority:       0.9,
		},
		{
			Request:        provider.SearchRequest{Query: "high yield", Sort: "latest", Page: 1, Size: 50},
			EstimatedCalls: 2,
			Priority:       0.8,
		},
		{
			Request:        provider.SearchRequest{Query: "medium", Sort: "accuracy", Page: 1, Size: 10},
			EstimatedCalls: 1,
			Priority:       0.5,
		},
	}

	plan := BuildPlan(candidates, budget)
	if len(plan.Selected) != 2 {
		t.Fatalf("selected=%d want=2: %+v", len(plan.Selected), plan)
	}
	if plan.Selected[0].Request.Query != "High Yield" {
		t.Fatalf("first query=%q", plan.Selected[0].Request.Query)
	}
	if plan.Selected[1].Request.Query != "medium" {
		t.Fatalf("second query=%q", plan.Selected[1].Request.Query)
	}
	if plan.SkippedDuplicate != 1 || plan.SkippedOverBudget != 1 || plan.PlannedCalls != 3 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
}

func TestBuildPlanIsDeterministicForTies(t *testing.T) {
	t.Parallel()

	makePlan := func() Plan {
		cfg := DefaultConfig()
		cfg.MaxRequestsPerRun = 2
		budget, err := NewBudget(cfg, 0)
		if err != nil {
			t.Fatal(err)
		}
		return BuildPlan([]Candidate{
			{Request: provider.SearchRequest{Query: "zeta"}, EstimatedCalls: 1, Priority: 0.5},
			{Request: provider.SearchRequest{Query: "alpha"}, EstimatedCalls: 1, Priority: 0.5},
		}, budget)
	}
	first := makePlan()
	second := makePlan()
	for i := range first.Selected {
		if first.Selected[i].Fingerprint != second.Selected[i].Fingerprint {
			t.Fatalf("non-deterministic plan: %+v vs %+v", first, second)
		}
	}
	if first.Selected[0].Request.Query != "alpha" {
		t.Fatalf("tie order starts with %q", first.Selected[0].Request.Query)
	}
}

func TestBuildPlanDryRunHasNoExecutor(t *testing.T) {
	t.Parallel()

	budget, err := NewBudget(DefaultConfig(), 0)
	if err != nil {
		t.Fatal(err)
	}
	plan := BuildPlan([]Candidate{{
		Request:        provider.SearchRequest{Query: "통계학"},
		EstimatedCalls: 1,
		Priority:       1,
	}}, budget)
	if len(plan.Selected) != 1 {
		t.Fatalf("selected=%d", len(plan.Selected))
	}
	// BuildPlan intentionally has no provider or HTTP client argument. A dry-run
	// therefore cannot cause an external request.
}

func TestPriorityScoreClampsFactors(t *testing.T) {
	t.Parallel()

	score := (PriorityFactors{
		DownstreamRelevance: 2,
		RecentUserDemand:    -1,
		HistoricalYield:     1,
		FreshnessNeed:       1,
		SourceDiversity:     1,
	}).Score()
	if score != 0.75 {
		t.Fatalf("score=%v want=0.75", score)
	}
}
