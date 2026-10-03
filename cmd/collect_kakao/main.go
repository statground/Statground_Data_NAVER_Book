package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"statground_naver_book_go/internal/bookrefreshreceipt"
	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/kakaocollector"
	"statground_naver_book_go/internal/kakaostore"
	"statground_naver_book_go/internal/lexicon"
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
	if result.LatestInsertedAt.After(s.total.LatestInsertedAt) {
		s.total.LatestInsertedAt = result.LatestInsertedAt
	}
}

func (s collectionSummary) String() string {
	return fmt.Sprintf(
		"provider=kakao status=completed calls=%d fetched=%d inserted=%d new_isbn=%d changed_isbn=%d duplicates=%d planned_requests=%d processed_requests=%d skipped_due_requests=%d candidate_requests=%d deferred_before_planning_requests=%d",
		s.total.Calls, s.total.Fetched, s.total.Inserted, s.total.NewISBN, s.total.ChangedISBN, s.total.Duplicates,
		s.plannedRequests, s.processedRequests, s.skippedDueRequests, s.candidateRequests, s.deferredBeforePlanningRequests,
	)
}

func main() {
	if err := ch.RunWriterCommand(run); err != nil {
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
	queries := splitQueries(envx.String("KAKAO_QUERIES", envx.String("KAKAO_QUERY", "")))
	if len(queries) == 0 {
		return &safeError{category: "invalid_request"}
	}
	batch, err := selectDiscoveryQueries(ctx, queries, runKind, util.NowKST(), lexicon.FromEnv)
	if err != nil {
		return &safeError{category: "configuration", stage: "discovery_selection"}
	}
	curatedCount, dictionaryCount := discoveryComposition(batch.Selections)
	fmt.Printf("provider=kakao discovery_candidates=%d curated_candidates=%d dictionary_candidates=%d discovery_degraded=%t\n", len(batch.Selections), curatedCount, dictionaryCount, batch.Degraded)
	if boolEnv("KAKAO_DISCOVERY_PLAN_ONLY", false) {
		fmt.Println("provider=kakao status=discovery_plan_only external_calls=0 collection_writes=0")
		return nil
	}

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
	dryRun := boolEnv("KAKAO_DRY_RUN", false)
	if dryRun {
		err = store.ValidateReadOnly(ctx)
	} else {
		err = store.Validate(ctx)
	}
	if err != nil {
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: kakaocollector.ErrorStage(err), reason: kakaocollector.ErrorReason(err)}
	}
	if !dryRun {
		writerCtx, writerCancel, leaseErr := clickhouseClient.WriterContext(ctx)
		if leaseErr != nil {
			return leaseErr
		}
		defer writerCancel()
		ctx = writerCtx
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
	candidates := make([]quota.Candidate, 0, len(batch.Selections))
	for _, selection := range batch.Selections {
		candidates = append(candidates, quota.Candidate{
			Request: provider.SearchRequest{
				Query:  selection.Keyword,
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
	candidates = balanceDiscoveryCandidates(candidates, batch)
	plan := balanceDiscoveryPlan(quota.BuildPlan(candidates, planningBudget), batch)
	byQuery := discoveryByQuery(batch.Selections)
	plannedSelections := make([]lexicon.Selection, 0, len(plan.Selected))
	for _, planned := range plan.Selected {
		plannedSelections = append(plannedSelections, byQuery[strings.ToLower(strings.Join(strings.Fields(planned.Request.Query), " "))])
	}
	plannedCurated, plannedDictionary := discoveryComposition(plannedSelections)
	fmt.Printf(
		"provider=kakao observed_calls_today=%d planned_requests=%d planned_calls=%d skipped_invalid=%d skipped_duplicate=%d skipped_budget=%d dry_run=%t candidate_requests=%d deferred_before_planning_requests=%d planned_curated_requests=%d planned_dictionary_requests=%d\n",
		observedCalls,
		len(plan.Selected),
		plan.PlannedCalls,
		plan.SkippedInvalid,
		plan.SkippedDuplicate,
		plan.SkippedOverBudget,
		dryRun,
		candidateCount, deferred, plannedCurated, plannedDictionary,
	)
	if dryRun {
		return nil
	}
	summary := collectionSummary{plannedRequests: len(plan.Selected), candidateRequests: candidateCount, deferredBeforePlanningRequests: deferred}
	runUUID := util.UUIDv7()
	started, err := beginCollectionReceipt(ctx, clickhouseClient, runUUID)
	if err != nil {
		return err
	}
	if len(plan.Selected) == 0 {
		if err := writeCollectionReceipt(ctx, store, runUUID, started, summary); err != nil {
			return err
		}
		fmt.Println(summary.String())
		return nil
	}
	if boolEnv("KAKAO_LEXICON_ENABLED", runKind == "scheduled") {
		if err := lexicon.Record(ctx, plannedSelections); err != nil {
			return &safeError{category: "discovery_evidence", stage: "record_selection"}
		}
	}

	searchClient, err := kakao.NewClientFromEnv()
	if err != nil {
		return &safeError{category: "configuration"}
	}
	runtimeBudget, err := quota.NewBudget(budgetConfig, observedCalls)
	if err != nil {
		return &safeError{category: "configuration"}
	}
	collector, err := kakaocollector.New(searchClient, store, runtimeBudget, runUUID)
	if err != nil {
		return &safeError{category: "contract_error"}
	}
	for _, planned := range plan.Selected {
		selection := byQuery[strings.ToLower(strings.Join(strings.Fields(planned.Request.Query), " "))]
		result, collectErr := collector.Collect(ctx, kakaocollector.Config{
			Mode:         mode,
			Request:      planned.Request,
			PageCap:      planned.EstimatedCalls,
			RespectDue:   respectDue,
			Priority:     planned.Priority,
			Source:       discoverySource(storeConfig.Source, selection),
			LineageTopic: storeConfig.LineageTopic,
		})
		summary.recordResult(result)
		if boolEnv("KAKAO_LEXICON_ENABLED", runKind == "scheduled") {
			outcome := "completed"
			if result.SkippedDue {
				outcome = "skipped_due"
			} else if collectErr != nil {
				outcome = "provider_error"
			}
			if err := lexicon.Outcome(ctx, []lexicon.Selection{selection}, outcome, result.Inserted); err != nil {
				return &safeError{category: "discovery_evidence", stage: "record_outcome"}
			}
		}
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
	if err := writeCollectionReceipt(ctx, store, runUUID, started, summary); err != nil {
		return err
	}
	fmt.Println(summary.String())
	return nil
}

func beginCollectionReceipt(ctx context.Context, client *ch.Client, runUUID string) (int64, error) {
	path := strings.TrimSpace(os.Getenv("KAKAO_COLLECTION_RECEIPT_FILE"))
	if path == "" {
		return 0, nil
	}
	rows, err := client.QueryJSONEachRowContext(ctx, "SELECT toUnixTimestamp64Milli(now64(3)) AS collection_started_millis SETTINGS max_threads=1,max_execution_time=5")
	if err != nil || len(rows) != 1 {
		return 0, &safeError{category: "publication_receipt", stage: "source_start_timestamp"}
	}
	started, valid := util.NonnegativeInteger(rows[0]["collection_started_millis"])
	if !valid || started <= 0 {
		return 0, &safeError{category: "publication_receipt", stage: "source_start_timestamp"}
	}
	if err := bookrefreshreceipt.Write(path, bookrefreshreceipt.Receipt{Version: 1, RunUUID: runUUID, State: "collecting", StartedMillis: started}); err != nil {
		return 0, &safeError{category: "publication_receipt", stage: "write_precollection_receipt"}
	}
	return started, nil
}

func writeCollectionReceipt(ctx context.Context, store *kakaostore.ClickHouseStore, runUUID string, started int64, summary collectionSummary) error {
	path := strings.TrimSpace(os.Getenv("KAKAO_COLLECTION_RECEIPT_FILE"))
	if path == "" {
		return nil
	}
	var latest int64
	if !summary.total.LatestInsertedAt.IsZero() {
		latest = summary.total.LatestInsertedAt.UnixMilli()
	}
	confirmed, completed, err := kakaostore.ConfirmCollectionSource(ctx, store.Client, store.Config.RawTable, store.Config.OutboxTable, runUUID, summary.total.Inserted, latest)
	if err != nil {
		return &safeError{category: kakaocollector.ErrorCategory(err), stage: "source_confirmation", reason: kakaocollector.ErrorReason(err)}
	}
	if err := bookrefreshreceipt.Write(path, bookrefreshreceipt.Receipt{Version: 1, RunUUID: runUUID, State: "completed", StartedMillis: started, Inserted: summary.total.Inserted, LatestInsertedMillis: confirmed, CompletedMillis: completed}); err != nil {
		return &safeError{category: "publication_receipt", stage: "write_collection_receipt"}
	}
	fmt.Printf("provider=kakao source_confirmed=true source_rows=%d source_watermark_millis=%d collection_started_millis=%d publication=unverified\n", summary.total.Inserted, confirmed, started)
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
