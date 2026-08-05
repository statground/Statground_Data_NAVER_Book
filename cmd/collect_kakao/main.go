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
	return message
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
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: kakaocollector.ErrorStage(err)}
	}

	now := util.NowKST()
	observedCalls, err := store.ObservedCallsToday(ctx, now)
	if err != nil {
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: kakaocollector.ErrorStage(err)}
	}
	stop, err := store.LatestQuotaStop(ctx)
	if err != nil {
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: kakaocollector.ErrorStage(err)}
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
	plan := quota.BuildPlan(candidates, planningBudget)
	fmt.Printf(
		"provider=kakao observed_calls_today=%d planned_requests=%d planned_calls=%d skipped_invalid=%d skipped_duplicate=%d skipped_budget=%d dry_run=%t\n",
		observedCalls,
		len(plan.Selected),
		plan.PlannedCalls,
		plan.SkippedInvalid,
		plan.SkippedDuplicate,
		plan.SkippedOverBudget,
		boolEnv("KAKAO_DRY_RUN", false),
	)
	if boolEnv("KAKAO_DRY_RUN", false) {
		return nil
	}
	if len(plan.Selected) == 0 {
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
	respectDue := boolEnv("KAKAO_RESPECT_FRONTIER_DUE", runKind == "scheduled")
	total := kakaocollector.Result{}
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
		if result.SkippedDue {
			continue
		}
		total.Calls += result.Calls
		total.Fetched += result.Fetched
		total.Inserted += result.Inserted
		total.NewISBN += result.NewISBN
		total.ChangedISBN += result.ChangedISBN
		total.Duplicates += result.Duplicates
		if collectErr != nil {
			return &safeError{category: result.ErrorCategory, stage: kakaocollector.ErrorStage(collectErr)}
		}
		if runtimeBudget.IsExhausted() {
			break
		}
	}
	fmt.Printf(
		"provider=kakao status=completed calls=%d fetched=%d inserted=%d new_isbn=%d changed_isbn=%d duplicates=%d\n",
		total.Calls,
		total.Fetched,
		total.Inserted,
		total.NewISBN,
		total.ChangedISBN,
		total.Duplicates,
	)
	return nil
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
