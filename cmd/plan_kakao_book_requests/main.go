package main

import (
	"fmt"
	"os"
	"strings"

	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/provider"
	"statground_naver_book_go/internal/quota"
)

func main() {
	config := quota.ConfigFromEnv()
	observedCalls := envx.Int("KAKAO_OBSERVED_CALLS_TODAY", 0)
	budget, err := quota.NewBudget(config, observedCalls)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Kakao request plan rejected: invalid budget policy")
		os.Exit(1)
	}

	queries := uniqueQueries(envx.String(
		"KAKAO_PLAN_QUERIES",
		"통계학,데이터 분석,R package,외국어 학습",
	))
	candidates := make([]quota.Candidate, 0, len(queries))
	for index, query := range queries {
		candidates = append(candidates, quota.Candidate{
			Request: provider.SearchRequest{
				Query: query,
				Sort:  "accuracy",
				Page:  1,
				Size:  50,
			},
			EstimatedCalls: 1,
			Priority:       1 - float64(index)*0.01,
		})
	}

	before := budget.Snapshot()
	plan := quota.BuildPlan(candidates, budget)
	after := budget.Snapshot()
	fmt.Printf("daily_limit=%d\n", before.OfficialDailyLimit)
	fmt.Printf("auto_budget=%d\n", before.AutoDailyBudget)
	fmt.Printf("observed_calls_today=%d\n", before.ObservedCallsToday)
	fmt.Printf("remaining_auto_budget=%d\n", after.RemainingAutoCalls)
	fmt.Printf("planned_requests=%d\n", plan.PlannedCalls)
	fmt.Printf("queries_selected=%d\n", len(plan.Selected))
	fmt.Printf("queries_skipped_duplicate=%d\n", plan.SkippedDuplicate)
	fmt.Printf("queries_skipped_invalid=%d\n", plan.SkippedInvalid)
	fmt.Printf("queries_skipped_budget=%d\n", plan.SkippedOverBudget)
	fmt.Println("manual_reserve_preserved=true")
	fmt.Println("external_calls=0")
}

func uniqueQueries(raw string) []string {
	out := make([]string, 0)
	seen := make(map[string]bool)
	for _, query := range strings.Split(raw, ",") {
		query = strings.Join(strings.Fields(query), " ")
		key := strings.ToLower(query)
		if query == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, query)
	}
	return out
}
