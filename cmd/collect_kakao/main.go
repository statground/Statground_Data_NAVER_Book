package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/kakaocollector"
	"statground_naver_book_go/internal/kakaostore"
	"statground_naver_book_go/internal/provider"
	"statground_naver_book_go/internal/provider/kakao"
	"statground_naver_book_go/internal/quota"
	"statground_naver_book_go/internal/util"
)

type safeError struct {
	category string
	stage    string
	reason   string
}

func (e *safeError) Error() string {
	category := strings.TrimSpace(e.category)
	if category == "" {
		category = "unknown"
	}
	message := "kakao book collection failed category=" + category
	if stage := strings.TrimSpace(e.stage); stage != "" {
		message += " stage=" + stage
	}
	if reason := strings.TrimSpace(e.reason); reason != "" {
		message += " reason=" + reason
	}
	return message
}

type collectionSummary struct {
	candidateRequests              int
	deferredBeforePlanningRequests int
	plannedRequests                int
	processedRequests              int
	skippedDueRequests             int
	total                          kakaocollector.Result
}

func (s *collectionSummary) recordResult(result kakaocollector.Result) {
	if result.SkippedDue {
		s.skippedDueRequests++
		return
	}
	s.processedRequests++
	s.total.Calls += result.Calls
	s.total.Fetched += result.Fetched
	s.total.Inserted += result.Inserted
	s.total.NewISBN += result.NewISBN
	s.total.ChangedISBN += result.ChangedISBN
	s.total.Duplicates += result.Duplicates
}

func (s collectionSummary) String() string {
	return fmt.Sprintf(
		"provider=kakao status=completed calls=%d fetched=%d inserted=%d new_isbn=%d changed_isbn=%d duplicates=%d planned_requests=%d processed_requests=%d skipped_due_requests=%d candidate_requests=%d deferred_before_planning_requests=%d",
		s.total.Calls, s.total.Fetched, s.total.Inserted, s.total.NewISBN, s.total.ChangedISBN, s.total.Duplicates,
		s.plannedRequests, s.processedRequests, s.skippedDueRequests, s.candidateRequests, s.deferredBeforePlanningRequests,
	)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	runKind := strings.ToLower(strings.TrimSpace(envx.String("KAKAO_RUN_KIND", "manual")))
	if runKind == "scheduled" && !boolEnv("KAKAO_BOOK_SCHEDULE_ENABLED", false) {
		fmt.Println("provider=kakao status=schedule_disabled")
		return nil
	}

	timeout := durationSeconds("KAKAO_RUN_TIMEOUT_SECONDS", 30*time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if _, err := envx.Require("KAKAO_REST_API_KEY"); err != nil {
		return &safeError{category: "configuration"}
	}
	clickhouseClient, err := ch.NewFromEnv()
	if err != nil {
		return &safeError{category: "configuration"}
	}
	storeConfig := kakaostore.ConfigFromEnv()
	store, err := kakaostore.NewClickHouse(clickhouseClient, storeConfig)
	if err != nil {
		return &safeError{category: "clickhouse_contract"}
	}
	if err := store.Validate(ctx); err != nil {
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: kakaocollector.ErrorStage(err), reason: kakaocollector.ErrorReason(err)}
	}

	now := util.NowKST()
	observedCalls, err := store.ObservedCallsToday(ctx, now)
	if err != nil {
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: kakaocollector.ErrorStage(err), reason: kakaocollector.ErrorReason(err)}
	}
	stop, err := store.LatestQuotaStop(ctx)
	if err != nil {
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: kakaocollector.ErrorStage(err), reason: kakaocollector.ErrorReason(err)}
	}
	quotaHold := durationHours("KAKAO_QUOTA_EXHAUSTED_HOLD_HOURS", 24*time.Hour)
	rateLimitHold := durationMinutes("KAKAO_RATE_LIMIT_HOLD_MINUTES", 30*time.Minute)
	if !boolEnv("KAKAO_QUOTA_STOP_OVERRIDE", false) &&
		kakaostore.QuotaStopBlocked(stop, now, quotaHold, rateLimitHold) {
		fmt.Printf("provider=kakao status=quota_hold category=%s\n", stop.Category)
		return nil
	}

	queries := splitQueries(envx.String("KAKAO_QUERIES", envx.String("KAKAO_QUERY", "")))
	if len(queries) == 0 {
		return &safeError{category: "invalid_request"}
	}
	mode := strings.TrimSpace(envx.String("KAKAO_COLLECT_MODE", "manual"))
	sort := strings.TrimSpace(envx.String("KAKAO_SEARCH_SORT", "accuracy"))
	target := strings.TrimSpace(envx.String("KAKAO_SEARCH_TARGET", ""))
	startPage := envx.Int("KAKAO_START_PAGE", 1)
	pageSize := envx.Int("KAKAO_PAGE_SIZE", 50)
	pageCap := envx.Int("KAKAO_PAGE_CAP", 1)
	priority := envx.Float("KAKAO_QUERY_PRIORITY", 0.5)

	budgetConfig := quota.ConfigFromEnv()
	planningBudget, err := quota.NewBudget(budgetConfig, observedCalls)
	if err != nil {
		return &safeError{category: "configuration"}
	}
	candidates := make([]quota.Candidate, 0, len(queries))
	for _, query := range queries {
		candidates = append(candidates, quota.Candidate{
			Request: provider.SearchRequest{
				Query:  query,
				Sort:   sort,
				Target: target,
				Page:   startPage,
				Size:   pageSize,
			},
			EstimatedCalls: pageCap,
			Priority:       priority,
		})
	}
	candidateCount := len(candidates)
	respectDue := boolEnv("KAKAO_RESPECT_FRONTIER_DUE", runKind == "scheduled")
	candidates, deferred, err := frontierEligibleCandidates(ctx, candidates, store, mode, respectDue, now)
	if err != nil {
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: kakaocollector.ErrorStage(err), reason: kakaocollector.ErrorReason(err)}
	}
	plan := quota.BuildPlan(candidates, planningBudget)
	fmt.Printf(
		"provider=kakao observed_calls_today=%d planned_requests=%d planned_calls=%d skipped_invalid=%d skipped_duplicate=%d skipped_budget=%d dry_run=%t candidate_requests=%d deferred_before_planning_requests=%d\n",
		observedCalls,
		len(plan.Selected),
		plan.PlannedCalls,
		plan.SkippedInvalid,
		plan.SkippedDuplicate,
		plan.SkippedOverBudget,
		boolEnv("KAKAO_DRY_RUN", false),
		candidateCount, deferred,
	)
	if boolEnv("KAKAO_DRY_RUN", false) {
		return nil
	}
	summary := collectionSummary{plannedRequests: len(plan.Selected), candidateRequests: candidateCount, deferredBeforePlanningRequests: deferred}
	if len(plan.Selected) == 0 {
		fmt.Println(summary.String())
		return nil
	}

	searchClient, err := kakao.NewClientFromEnv()
	if err != nil {
		return &safeError{category: "configuration"}
	}
	runtimeBudget, err := quota.NewBudget(budgetConfig, observedCalls)
	if err != nil {
		return &safeError{category: "configuration"}
	}
	runUUID := util.UUIDv7()
	collector, err := kakaocollector.New(searchClient, store, runtimeBudget, runUUID)
	if err != nil {
		return &safeError{category: "contract_error"}
	}
	for _, planned := range plan.Selected {
		result, collectErr := collector.Collect(ctx, kakaocollector.Config{
			Mode:         mode,
			Request:      planned.Request,
			PageCap:      planned.EstimatedCalls,
			RespectDue:   respectDue,
			Priority:     planned.Priority,
			Source:       storeConfig.Source,
			LineageTopic: storeConfig.LineageTopic,
		})
		summary.recordResult(result)
		if result.SkippedDue {
			continue
		}
		if collectErr != nil {
			return &safeError{category: result.ErrorCategory, stage: kakaocollector.ErrorStage(collectErr), reason: kakaocollector.ErrorReason(collectErr)}
		}
		if runtimeBudget.IsExhausted() {
			break
		}
	}
	fmt.Println(summary.String())
	return nil
}

type frontierReader interface {
	LoadFrontier(context.Context, kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error)
}

// Filter before reserving planning quota. Collect still checks frontier due
// again immediately before an external request.
func frontierEligibleCandidates(ctx context.Context, candidates []quota.Candidate, store frontierReader, mode string, respectDue bool, now time.Time) ([]quota.Candidate, int, error) {
	if !respectDue {
		return candidates, 0, nil
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = "manual"
	}
	eligible := make([]quota.Candidate, 0, len(candidates))
	deferred := 0
	for _, candidate := range candidates {
		request := candidate.Request
		request.Query = strings.Join(strings.Fields(request.Query), " ")
		request, err := provider.NormalizeSearchRequest(request)
		if err != nil || candidate.EstimatedCalls <= 0 {
			// Preserve BuildPlan's invalid-candidate accounting without extra reads.
			eligible = append(eligible, candidate)
			continue
		}
		frontier, err := store.LoadFrontier(ctx, kakaostore.FrontierKey{Provider: "kakao", Mode: mode, Query: request.Query, Target: request.Target, Sort: request.Sort})
		if err != nil {
			return nil, 0, err
		}
		if frontier.Found && (!frontier.State.Active || (!frontier.NextDueAt.IsZero() && now.Before(frontier.NextDueAt))) {
			deferred++
			continue
		}
		eligible = append(eligible, candidate)
	}
	return eligible, deferred, nil
}

func splitQueries(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ';'
	})
	out := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		field = strings.Join(strings.Fields(field), " ")
		if field == "" {
			continue
		}
		key := strings.ToLower(field)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, field)
	}
	return out
}

func boolEnv(name string, fallback bool) bool {
	defaultValue := "false"
	if fallback {
		defaultValue = "true"
	}
	switch strings.ToLower(strings.TrimSpace(envx.String(name, defaultValue))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func durationSeconds(name string, fallback time.Duration) time.Duration {
	value := envx.Float(name, fallback.Seconds())
	if value <= 0 {
		return fallback
	}
	return time.Duration(value * float64(time.Second))
}

func durationHours(name string, fallback time.Duration) time.Duration {
	value := envx.Float(name, fallback.Hours())
	if value <= 0 {
		return fallback
	}
	return time.Duration(value * float64(time.Hour))
}

func durationMinutes(name string, fallback time.Duration) time.Duration {
	value := envx.Float(name, fallback.Minutes())
	if value <= 0 {
		return fallback
	}
	return time.Duration(value * float64(time.Minute))
}
