package kakaocollector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"statground_naver_book_go/internal/bookmodel"
	"statground_naver_book_go/internal/kakaostore"
	"statground_naver_book_go/internal/provider"
	"statground_naver_book_go/internal/provider/kakao"
	"statground_naver_book_go/internal/quota"
	"statground_naver_book_go/internal/rawkakao"
	"statground_naver_book_go/internal/util"
)

var ErrBudgetExhausted = errors.New("kakao request budget exhausted")

type Config struct {
	Mode         string
	Request      provider.SearchRequest
	PageCap      int
	RespectDue   bool
	Priority     float64
	Source       string
	LineageTopic string
}

type Result struct {
	SkippedDue      bool
	Calls           int
	Fetched         int
	Inserted        int
	NewISBN         int
	ChangedISBN     int
	Duplicates      int
	TotalCount      int
	PageableCount   int
	ErrorCategory   string
	AdjustedPageCap int
}

type Collector struct {
	Provider provider.SearchProvider
	Store    kakaostore.Store
	Budget   *quota.Budget
	RunUUID  string
	Now      func() time.Time
	NewUUID  func() string
}

func New(searchProvider provider.SearchProvider, store kakaostore.Store, budget *quota.Budget, runUUID string) (*Collector, error) {
	if searchProvider == nil {
		return nil, fmt.Errorf("Kakao search provider is required")
	}
	if store == nil {
		return nil, fmt.Errorf("Kakao store is required")
	}
	if budget == nil {
		return nil, fmt.Errorf("Kakao runtime budget is required")
	}
	if strings.TrimSpace(runUUID) == "" {
		return nil, fmt.Errorf("Kakao run UUID is required")
	}
	return &Collector{
		Provider: searchProvider,
		Store:    store,
		Budget:   budget,
		RunUUID:  runUUID,
		Now:      util.NowKST,
		NewUUID:  util.UUIDv7,
	}, nil
}

func (c *Collector) Collect(ctx context.Context, config Config) (Result, error) {
	request, err := provider.NormalizeSearchRequest(config.Request)
	if err != nil {
		return Result{ErrorCategory: "invalid_request"}, err
	}
	config.Mode = strings.TrimSpace(config.Mode)
	if config.Mode == "" {
		config.Mode = "manual"
	}
	if config.PageCap <= 0 {
		config.PageCap = 1
	}
	remainingPages := 51 - request.Page
	if config.PageCap > remainingPages {
		config.PageCap = remainingPages
	}
	if config.Priority <= 0 {
		config.Priority = 0.5
	}

	now := c.now()
	key := kakaostore.FrontierKey{
		Provider: c.Provider.Name(),
		Mode:     config.Mode,
		Query:    request.Query,
		Target:   request.Target,
		Sort:     request.Sort,
	}
	frontier, err := c.Store.LoadFrontier(ctx, key)
	if err != nil {
		return Result{ErrorCategory: storeErrorCategory(err)}, err
	}
	if config.RespectDue && frontier.Found {
		if !frontier.State.Active || (!frontier.NextDueAt.IsZero() && now.Before(frontier.NextDueAt)) {
			return Result{SkippedDue: true}, nil
		}
	}
	pageCap := quota.AdjustPageCap(frontier.State, config.PageCap)
	result := Result{AdjustedPageCap: pageCap}
	queryHash := QueryHash(request, config.Mode)
	observer := newAttemptObserver(c, config.Mode, queryHash, config.Source)
	searchCtx := kakao.WithAttemptObserver(ctx, observer)
	seen := make(map[string]struct{})

	var collectErr error
	for offset := 0; offset < pageCap; offset++ {
		pageRequest := request
		pageRequest.Page = request.Page + offset
		response, searchErr := c.Provider.Search(searchCtx, pageRequest)
		result.Calls = observer.Calls()
		if searchErr != nil {
			result.ErrorCategory = ErrorCategory(searchErr)
			collectErr = searchErr
			if kakao.IsQuotaStop(searchErr) {
				c.Budget.MarkExhausted()
			}
			break
		}

		result.TotalCount = response.TotalCount
		result.PageableCount = response.PageableCount
		result.Fetched += len(response.Documents)
		rows, stats, buildErr := c.rowsForDocuments(ctx, response.Documents, rawkakao.Evidence{
			RunUUID:       c.RunUUID,
			RequestUUID:   observer.LastSuccessfulRequestUUID(),
			Mode:          config.Mode,
			Query:         request.Query,
			Sort:          request.Sort,
			Target:        request.Target,
			Page:          pageRequest.Page,
			Size:          pageRequest.Size,
			TotalCount:    response.TotalCount,
			PageableCount: response.PageableCount,
			IsEnd:         response.IsEnd,
			Source:        config.Source,
			LineageTopic:  config.LineageTopic,
			CollectedAt:   c.now(),
		}, seen)
		if buildErr != nil {
			result.ErrorCategory = ErrorCategory(buildErr)
			collectErr = buildErr
			break
		}
		if len(rows) > 0 {
			if insertErr := c.Store.InsertRawRows(ctx, rows); insertErr != nil {
				result.ErrorCategory = storeErrorCategory(insertErr)
				collectErr = insertErr
				break
			}
		}
		result.Inserted += len(rows)
		result.NewISBN += stats.NewISBN
		result.ChangedISBN += stats.ChangedISBN
		result.Duplicates += stats.Duplicates

		if response.IsEnd || len(response.Documents) == 0 ||
			pageRequest.Page*pageRequest.Size >= response.PageableCount {
			break
		}
	}
	result.Calls = observer.Calls()

	if finishErr := c.finish(ctx, config, request, queryHash, frontier, result, collectErr); finishErr != nil {
		return result, finishErr
	}
	return result, collectErr
}

type pageStats struct {
	NewISBN     int
	ChangedISBN int
	Duplicates  int
}

func (c *Collector) rowsForDocuments(
	ctx context.Context,
	documents []bookmodel.BookDocument,
	evidence rawkakao.Evidence,
	seen map[string]struct{},
) ([]map[string]any, pageStats, error) {
	type candidate struct {
		document bookmodel.BookDocument
		hash     string
		key      string
	}
	candidates := make([]candidate, 0, len(documents))
	canonicalISBNs := make([]string, 0, len(documents))
	stats := pageStats{}

	for _, document := range documents {
		hash, err := rawkakao.ContentHash(document)
		if err != nil {
			return nil, stats, err
		}
		key := document.CanonicalISBN
		if key == "" {
			key = "invalid:" + hash
		}
		if _, exists := seen[key]; exists {
			stats.Duplicates++
			continue
		}
		seen[key] = struct{}{}
		candidates = append(candidates, candidate{document: document, hash: hash, key: key})
		if document.ISBNValid && document.CanonicalISBN != "" {
			canonicalISBNs = append(canonicalISBNs, document.CanonicalISBN)
		}
	}

	existing, err := c.Store.ExistingContentHashes(ctx, canonicalISBNs)
	if err != nil {
		return nil, stats, err
	}
	rows := make([]map[string]any, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.document.ISBNValid && candidate.document.CanonicalISBN != "" {
			existingHash, exists := existing[candidate.document.CanonicalISBN]
			switch {
			case !exists:
				stats.NewISBN++
			case existingHash == candidate.hash:
				stats.Duplicates++
				continue
			default:
				stats.ChangedISBN++
			}
		}
		row, err := rawkakao.BuildRow(candidate.document, evidence, c.NewUUID)
		if err != nil {
			return nil, stats, err
		}
		rows = append(rows, row)
	}
	return rows, stats, nil
}

func (c *Collector) finish(
	ctx context.Context,
	config Config,
	request provider.SearchRequest,
	queryHash string,
	frontier kakaostore.FrontierSnapshot,
	result Result,
	collectErr error,
) error {
	now := c.now()
	status := "success"
	category := result.ErrorCategory
	if collectErr != nil {
		status = "error"
		if category == "quota_exhausted" || category == "rate_limited" {
			status = category
		}
	}
	if category == "" {
		category = ""
	}
	version := versionAt(now)
	collectLog := kakaostore.CollectLog{
		LogUUID:          c.NewUUID(),
		RunUUID:          c.RunUUID,
		Version:          version,
		Provider:         c.Provider.Name(),
		Mode:             config.Mode,
		Query:            request.Query,
		QueryHash:        queryHash,
		Target:           request.Target,
		Sort:             request.Sort,
		RequestedPageCap: config.PageCap,
		PagesCalled:      result.Calls,
		TotalCount:       result.TotalCount,
		PageableCount:    result.PageableCount,
		FetchedCount:     result.Fetched,
		InsertedCount:    result.Inserted,
		NewISBNCount:     result.NewISBN,
		ChangedISBNCount: result.ChangedISBN,
		DuplicateCount:   result.Duplicates,
		Status:           status,
		ErrorCategory:    category,
		Source:           config.Source,
		CollectedAt:      now,
	}
	if err := c.Store.InsertCollectLog(ctx, collectLog); err != nil {
		return err
	}

	previous := frontier.State
	active := previous.Active
	if !frontier.Found {
		active = true
	}
	lastSuccessAt := previous.LastSuccessAt
	consecutiveZero := previous.ConsecutiveZeroYield
	if collectErr == nil {
		lastSuccessAt = now
		if result.NewISBN+result.ChangedISBN == 0 {
			consecutiveZero++
		} else {
			consecutiveZero = 0
		}
	}
	state := quota.FrontierState{
		LastSuccessAt:        lastSuccessAt,
		CallsLastRun:         result.Calls,
		NewISBNLastRun:       result.NewISBN,
		ChangedISBNLastRun:   result.ChangedISBN,
		DuplicateRatio:       ratio(result.Duplicates, result.Fetched),
		ConsecutiveZeroYield: consecutiveZero,
		Active:               active,
	}
	nextDue := quota.NextDueAt(state, now)
	if collectErr != nil {
		switch category {
		case "quota_exhausted":
			nextDue = now.Add(24 * time.Hour)
		case "rate_limited":
			nextDue = now.Add(30 * time.Minute)
		default:
			nextDue = now.Add(24 * time.Hour)
		}
	}
	yieldPerCall := float64(0)
	if result.Calls > 0 {
		yieldPerCall = float64(result.NewISBN+result.ChangedISBN) / float64(result.Calls)
	}
	frontierRecord := kakaostore.FrontierRecord{
		FrontierUUID:       c.NewUUID(),
		RunUUID:            c.RunUUID,
		Version:            version + 1,
		Key:                kakaostore.FrontierKey{Provider: c.Provider.Name(), Mode: config.Mode, Query: request.Query, Target: request.Target, Sort: request.Sort},
		QueryHash:          queryHash,
		LastRunAt:          now,
		LastSuccessAt:      lastSuccessAt,
		NextDueAt:          nextDue,
		LastTotalCount:     result.TotalCount,
		LastPageableCount:  result.PageableCount,
		CallsLastRun:       result.Calls,
		DocumentsLastRun:   result.Fetched,
		NewISBNLastRun:     result.NewISBN,
		ChangedISBNLastRun: result.ChangedISBN,
		DuplicateRatio:     state.DuplicateRatio,
		YieldPerCall:       yieldPerCall,
		ConsecutiveZero:    consecutiveZero,
		PriorityScore:      config.Priority,
		Active:             active,
		Source:             config.Source,
	}
	return c.Store.InsertFrontier(ctx, frontierRecord)
}

func (c *Collector) now() time.Time {
	if c.Now == nil {
		return util.NowKST()
	}
	return c.Now()
}

type attemptObserver struct {
	collector *Collector
	mode      string
	queryHash string
	source    string

	mu             sync.Mutex
	started        map[string]kakaostore.CallLog
	calls          int
	lastSuccessful string
}

func newAttemptObserver(collector *Collector, mode, queryHash, source string) *attemptObserver {
	return &attemptObserver{
		collector: collector,
		mode:      mode,
		queryHash: queryHash,
		source:    source,
		started:   make(map[string]kakaostore.CallLog),
	}
}

func (o *attemptObserver) BeforeAttempt(ctx context.Context, info kakao.AttemptInfo) (string, error) {
	if !o.collector.Budget.Reserve(1) {
		return "", ErrBudgetExhausted
	}
	requestUUID := o.collector.NewUUID()
	record := kakaostore.CallLog{
		RequestUUID:   requestUUID,
		RunUUID:       o.collector.RunUUID,
		Version:       versionAt(info.StartedAt),
		RequestedAt:   info.StartedAt,
		Mode:          o.mode,
		QueryHash:     o.queryHash,
		Target:        info.Request.Target,
		Sort:          info.Request.Sort,
		Page:          info.Request.Page,
		Size:          info.Request.Size,
		ErrorCategory: "",
		Status:        "reserved",
		Source:        o.source,
	}
	if err := o.collector.Store.InsertCallLog(ctx, record); err != nil {
		return "", err
	}
	o.mu.Lock()
	o.started[requestUUID] = record
	o.calls++
	o.mu.Unlock()
	return requestUUID, nil
}

func (o *attemptObserver) AfterAttempt(ctx context.Context, requestUUID string, result kakao.AttemptResult) error {
	o.mu.Lock()
	record, exists := o.started[requestUUID]
	o.mu.Unlock()
	if !exists {
		return &kakaostore.StoreError{Operation: "complete_call_log", Category: "clickhouse_contract"}
	}
	completedAt := result.CompletedAt
	record.CompletedAt = &completedAt
	record.Version = versionAt(completedAt)
	if record.Version <= versionAt(record.RequestedAt) {
		record.Version = versionAt(record.RequestedAt) + 1
	}
	record.HTTPStatus = result.HTTPStatus
	record.KakaoCode = result.KakaoCode
	record.Success = result.Success
	record.Documents = result.DocumentsCount
	record.ElapsedMillis = result.Elapsed.Milliseconds()
	record.ErrorCategory = result.ErrorCategory
	record.Status = "completed"
	if !result.Success {
		record.Status = "failed"
	}
	if err := o.collector.Store.InsertCallLog(ctx, record); err != nil {
		return err
	}
	o.mu.Lock()
	delete(o.started, requestUUID)
	if result.Success {
		o.lastSuccessful = requestUUID
	}
	o.mu.Unlock()
	if result.ErrorCategory == "quota_exhausted" || result.ErrorCategory == "rate_limited" {
		o.collector.Budget.MarkExhausted()
	}
	return nil
}

func (o *attemptObserver) Calls() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls
}

func (o *attemptObserver) LastSuccessfulRequestUUID() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lastSuccessful
}

func QueryHash(request provider.SearchRequest, mode string) string {
	normalized, err := provider.NormalizeSearchRequest(request)
	if err != nil {
		normalized = request
	}
	value := strings.Join([]string{
		strings.ToLower(strings.Join(strings.Fields(normalized.Query), " ")),
		strings.ToLower(strings.TrimSpace(mode)),
		normalized.Target,
		normalized.Sort,
	}, "\x1f")
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func ErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrBudgetExhausted) {
		return "budget_exhausted"
	}
	if category := kakao.ErrorCategory(err); category != kakao.ErrorUnknown {
		return category
	}
	return storeErrorCategory(err)
}

// ErrorStage returns only a closed, non-sensitive operation label. It never
// includes an endpoint, table name, SQL text, or driver response.
func ErrorStage(err error) string {
	var storeErr *kakaostore.StoreError
	if !errors.As(err, &storeErr) {
		return ""
	}
	switch storeErr.Operation {
	case "preflight_connection",
		"preflight_table",
		"preflight_grant",
		"observed_calls",
		"latest_quota_stop",
		"load_frontier",
		"insert_call_log",
		"complete_call_log",
		"existing_hashes",
		"insert_raw",
		"insert_collect_log",
		"insert_frontier":
		return storeErr.Operation
	default:
		return ""
	}
}

func storeErrorCategory(err error) string {
	var storeErr *kakaostore.StoreError
	if errors.As(err, &storeErr) {
		return storeErr.Category
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	return "unknown"
}

func versionAt(value time.Time) uint64 {
	version := value.UnixMicro()
	if version <= 0 {
		return 1
	}
	return uint64(version)
}

func ratio(numerator, denominator int) float64 {
	if numerator <= 0 || denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}
