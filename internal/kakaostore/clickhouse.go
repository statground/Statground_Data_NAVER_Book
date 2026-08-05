package kakaostore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/quota"
	"statground_naver_book_go/internal/util"
)

var tableIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

type Config struct {
	RawTable            string
	RawLocalTable       string
	CollectLogTable     string
	CollectLocalTable   string
	CallLogTable        string
	CallLocalTable      string
	FrontierTable       string
	FrontierLocal       string
	ProviderLatestTable string
	RawWriteTimeout     time.Duration
	Source              string
	LineageTopic        string
	RequireHTTPS        bool
}

func ConfigFromEnv() Config {
	return Config{
		RawTable:            envx.String("KAKAO_RAW_TABLE", "Data_Book_KAKAO_Raw.kakao_book_raw"),
		RawLocalTable:       envx.String("KAKAO_RAW_LOCAL_TABLE", "Data_Book_KAKAO_Raw.kakao_book_raw_local"),
		CollectLogTable:     envx.String("KAKAO_COLLECT_LOG_TABLE", "Data_Book_KAKAO_Log.kakao_collect_log"),
		CollectLocalTable:   envx.String("KAKAO_COLLECT_LOG_LOCAL_TABLE", "Data_Book_KAKAO_Log.kakao_collect_log_local"),
		CallLogTable:        envx.String("KAKAO_API_CALL_LOG_TABLE", "Data_Book_KAKAO_Log.kakao_api_call_log"),
		CallLocalTable:      envx.String("KAKAO_API_CALL_LOG_LOCAL_TABLE", "Data_Book_KAKAO_Log.kakao_api_call_log_local"),
		FrontierTable:       envx.String("KAKAO_QUERY_FRONTIER_TABLE", "Data_Book_KAKAO_Log.kakao_query_frontier"),
		FrontierLocal:       envx.String("KAKAO_QUERY_FRONTIER_LOCAL_TABLE", "Data_Book_KAKAO_Log.kakao_query_frontier_local"),
		ProviderLatestTable: envx.String("KAKAO_PROVIDER_LATEST_TABLE", "Data_Book_Service.book_provider_latest"),
		RawWriteTimeout:     rawWriteTimeoutFromEnv(),
		Source:              envx.String("PRODUCER_SOURCE", "github_actions"),
		LineageTopic:        envx.String("KAKAO_DIRECT_INGEST_TOPIC", "direct.statground_book.kakao_book"),
		RequireHTTPS:        boolEnv("KAKAO_REQUIRE_CLICKHOUSE_HTTPS", true),
	}
}

type ClickHouseStore struct {
	Client          *ch.Client
	RawInsertClient *ch.Client
	Config          Config
}

func NewClickHouse(client *ch.Client, config Config) (*ClickHouseStore, error) {
	if client == nil {
		return nil, fmt.Errorf("Kakao ClickHouse connection is required")
	}
	for name, table := range map[string]string{
		"raw":               config.RawTable,
		"raw local":         config.RawLocalTable,
		"collect log":       config.CollectLogTable,
		"collect log local": config.CollectLocalTable,
		"API call log":      config.CallLogTable,
		"API call local":    config.CallLocalTable,
		"frontier":          config.FrontierTable,
		"frontier local":    config.FrontierLocal,
		"provider latest":   config.ProviderLatestTable,
	} {
		if !tableIdentifierPattern.MatchString(strings.TrimSpace(table)) {
			return nil, fmt.Errorf("invalid Kakao %s table identifier", name)
		}
	}
	if config.RawWriteTimeout < 60*time.Second || config.RawWriteTimeout > 15*time.Minute {
		config.RawWriteTimeout = 660 * time.Second
	}
	rawInsertClient := *client
	if client.HTTPClient == nil {
		rawInsertClient.HTTPClient = &http.Client{Timeout: config.RawWriteTimeout}
	} else {
		rawHTTPClient := *client.HTTPClient
		rawHTTPClient.Timeout = config.RawWriteTimeout
		rawInsertClient.HTTPClient = &rawHTTPClient
	}
	return &ClickHouseStore{Client: client, RawInsertClient: &rawInsertClient, Config: config}, nil
}

type StoreError struct {
	Operation string
	Category  string
	Reason    string
}

func (e *StoreError) Error() string {
	message := fmt.Sprintf("kakao clickhouse operation failed operation=%s category=%s", e.Operation, e.Category)
	if e.Reason != "" {
		message += " reason=" + e.Reason
	}
	return message
}

func (s *ClickHouseStore) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateConnectionBoundary(); err != nil {
		return err
	}
	writableTables := []string{
		s.Config.RawTable,
		s.Config.RawLocalTable,
		s.Config.CollectLogTable,
		s.Config.CollectLocalTable,
		s.Config.CallLogTable,
		s.Config.CallLocalTable,
		s.Config.FrontierTable,
		s.Config.FrontierLocal,
	}
	for _, table := range append(writableTables, s.Config.ProviderLatestTable) {
		if err := s.tableExists(ctx, table); err != nil {
			return err
		}
	}
	for _, table := range writableTables {
		if err := s.checkGrant(ctx, "INSERT", table); err != nil {
			return err
		}
	}
	for _, table := range []string{s.Config.CallLogTable, s.Config.FrontierTable, s.Config.ProviderLatestTable} {
		if err := s.checkGrant(ctx, "SELECT", table); err != nil {
			return err
		}
	}
	return nil
}

func (s *ClickHouseStore) ObservedCallsToday(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		now = util.NowKST()
	}
	start := now.In(util.KST()).Truncate(24 * time.Hour)
	// Truncate operates on absolute duration and is not a local-midnight
	// operation, so reconstruct the date explicitly.
	start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, util.KST())
	end := start.Add(24 * time.Hour)
	sql := fmt.Sprintf(`
        SELECT uniqExact(request_uuid) AS value
        FROM %s
        WHERE requested_at >= toDateTime64(%s, 3, 'Asia/Seoul')
          AND requested_at < toDateTime64(%s, 3, 'Asia/Seoul')
    `, s.Config.CallLogTable, util.SQLString(util.FormatCHDateTime64Millis(start)), util.SQLString(util.FormatCHDateTime64Millis(end)))
	row, err := s.Client.QuerySingleRow(sql)
	if err != nil {
		return 0, sanitizeStoreError("observed_calls", err)
	}
	return int(util.ToInt64(row["value"])), nil
}

func (s *ClickHouseStore) LatestQuotaStop(ctx context.Context) (QuotaStop, error) {
	if err := ctx.Err(); err != nil {
		return QuotaStop{}, err
	}
	sql := fmt.Sprintf(`
        SELECT error_category, stopped_at
        FROM
        (
            SELECT
                request_uuid,
                argMax(error_category, version) AS error_category,
                argMax(coalesce(completed_at, requested_at), version) AS stopped_at
            FROM %s
            GROUP BY request_uuid
        )
        WHERE error_category IN ('quota_exhausted', 'rate_limited')
        ORDER BY stopped_at DESC
        LIMIT 1
    `, s.Config.CallLogTable)
	rows, err := s.Client.QueryJSONEachRow(sql)
	if err != nil {
		return QuotaStop{}, sanitizeStoreError("latest_quota_stop", err)
	}
	if len(rows) == 0 {
		return QuotaStop{}, nil
	}
	return QuotaStop{
		Found:     true,
		Category:  allowedErrorCategory(util.ToString(rows[0]["error_category"])),
		StoppedAt: parseTime(util.ToString(rows[0]["stopped_at"])),
	}, nil
}

func (s *ClickHouseStore) LoadFrontier(ctx context.Context, key FrontierKey) (FrontierSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return FrontierSnapshot{}, err
	}
	sql := fmt.Sprintf(`
        SELECT
            last_success_at,
            next_due_at,
            last_total_count,
            last_pageable_count,
            calls_last_run,
            new_isbn_last_run,
            changed_isbn_last_run,
            duplicate_ratio,
            consecutive_zero_yield,
            priority_score,
            active
        FROM %s
        WHERE provider = %s
          AND mode = %s
          AND normalized_query = %s
          AND target = %s
          AND sort = %s
        ORDER BY version DESC
        LIMIT 1
    `, s.Config.FrontierTable,
		util.SQLString(defaultString(key.Provider, "kakao")),
		util.SQLString(key.Mode),
		util.SQLString(normalizeQuery(key.Query)),
		util.SQLString(key.Target),
		util.SQLString(key.Sort),
	)
	rows, err := s.Client.QueryJSONEachRow(sql)
	if err != nil {
		return FrontierSnapshot{}, sanitizeStoreError("load_frontier", err)
	}
	if len(rows) == 0 {
		return FrontierSnapshot{
			State: quota.FrontierState{Active: true},
		}, nil
	}
	row := rows[0]
	return FrontierSnapshot{
		Found: true,
		State: quota.FrontierState{
			LastSuccessAt:        parseTime(util.ToString(row["last_success_at"])),
			CallsLastRun:         int(util.ToInt64(row["calls_last_run"])),
			NewISBNLastRun:       int(util.ToInt64(row["new_isbn_last_run"])),
			ChangedISBNLastRun:   int(util.ToInt64(row["changed_isbn_last_run"])),
			DuplicateRatio:       toFloat64(row["duplicate_ratio"]),
			ConsecutiveZeroYield: int(util.ToInt64(row["consecutive_zero_yield"])),
			Active:               toBool(row["active"]),
		},
		NextDueAt:         parseTime(util.ToString(row["next_due_at"])),
		LastTotalCount:    int(util.ToInt64(row["last_total_count"])),
		LastPageableCount: int(util.ToInt64(row["last_pageable_count"])),
		PriorityScore:     toFloat64(row["priority_score"]),
	}, nil
}

func (s *ClickHouseStore) ExistingContentHashes(ctx context.Context, canonicalISBNs []string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cleaned := cleanStrings(canonicalISBNs)
	if len(cleaned) == 0 {
		return map[string]string{}, nil
	}
	const lookupBatchSize = 500
	out := make(map[string]string, len(cleaned))
	for offset := 0; offset < len(cleaned); offset += lookupBatchSize {
		end := min(offset+lookupBatchSize, len(cleaned))
		sql := fmt.Sprintf(`
			SELECT
				canonical_isbn,
				argMax(content_hash, tuple(version, updated_at, ingested_at, uuid)) AS content_hash
			FROM %s
			WHERE provider = 'kakao'
			  AND isbn_valid = 1
			  AND notEmpty(canonical_isbn)
			  AND canonical_isbn IN (%s)
			GROUP BY canonical_isbn
			SETTINGS
				optimize_skip_unused_shards = 1,
				max_threads = 1,
				max_execution_time = 10
		`, s.Config.ProviderLatestTable, util.QuoteStringList(cleaned[offset:end]))
		rows, err := s.queryJSONEachRowReadOnly(ctx, sql)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			return nil, sanitizeStoreError("existing_hashes", err)
		}
		for _, row := range rows {
			isbn := strings.TrimSpace(util.ToString(row["canonical_isbn"]))
			hash := strings.TrimSpace(util.ToString(row["content_hash"]))
			if isbn != "" && hash != "" {
				out[isbn] = hash
			}
		}
	}
	return out, nil
}

// queryJSONEachRowReadOnly provides a read-specific retry boundary. Insert
// paths retain their separate deterministic deduplication-token policy.
func (s *ClickHouseStore) queryJSONEachRowReadOnly(ctx context.Context, sql string) ([]map[string]any, error) {
	for attempt := 1; attempt <= 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := s.Client.QueryJSONEachRow(sql)
		if err == nil {
			return rows, nil
		}
		if !retryableStoreError(err) || attempt == 3 {
			return nil, err
		}
		timer := time.NewTimer(time.Duration(attempt) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("read-only query retry exhausted")
}

func (s *ClickHouseStore) InsertCallLog(ctx context.Context, record CallLog) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var completedAt any
	if record.CompletedAt != nil {
		completedAt = util.FormatCHDateTime64Millis(*record.CompletedAt)
	}
	row := map[string]any{
		"request_uuid":     record.RequestUUID,
		"run_uuid":         record.RunUUID,
		"version":          record.Version,
		"requested_at":     util.FormatCHDateTime64Millis(record.RequestedAt),
		"completed_at":     completedAt,
		"mode":             record.Mode,
		"query_hash":       record.QueryHash,
		"target":           record.Target,
		"sort":             record.Sort,
		"page":             nonNegative(record.Page),
		"size":             nonNegative(record.Size),
		"http_status":      nonNegative(record.HTTPStatus),
		"kakao_error_code": record.KakaoCode,
		"success":          boolUInt8(record.Success),
		"documents_count":  nonNegative(record.Documents),
		"elapsed_ms":       nonNegative64(record.ElapsedMillis),
		"error_category":   allowedErrorCategory(record.ErrorCategory),
		"status":           defaultString(record.Status, "completed"),
		"source":           defaultString(record.Source, s.Config.Source),
		"created_at":       util.FormatCHDateTime64Millis(record.RequestedAt),
	}
	if err := s.Client.InsertJSONEachRow(s.Config.CallLogTable, []map[string]any{row}); err != nil {
		return sanitizeStoreError("insert_call_log", err)
	}
	return nil
}

func (s *ClickHouseStore) InsertRawRows(ctx context.Context, rows []map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.RawInsertClient.InsertJSONEachRow(s.Config.RawTable, rows); err != nil {
		return sanitizeStoreError("insert_raw", err)
	}
	return nil
}

func (s *ClickHouseStore) InsertCollectLog(ctx context.Context, record CollectLog) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	row := map[string]any{
		"log_uuid":           record.LogUUID,
		"run_uuid":           record.RunUUID,
		"version":            record.Version,
		"provider":           defaultString(record.Provider, "kakao"),
		"search_mode":        record.Mode,
		"search_query":       normalizeQuery(record.Query),
		"query_hash":         record.QueryHash,
		"search_target":      record.Target,
		"search_sort":        record.Sort,
		"requested_page_cap": nonNegative(record.RequestedPageCap),
		"pages_called":       nonNegative(record.PagesCalled),
		"api_total_count":    nonNegative(record.TotalCount),
		"api_pageable_count": nonNegative(record.PageableCount),
		"fetched_count":      nonNegative(record.FetchedCount),
		"inserted_count":     nonNegative(record.InsertedCount),
		"new_isbn_count":     nonNegative(record.NewISBNCount),
		"changed_isbn_count": nonNegative(record.ChangedISBNCount),
		"duplicate_count":    nonNegative(record.DuplicateCount),
		"status":             record.Status,
		"error_category":     allowedErrorCategory(record.ErrorCategory),
		"source":             defaultString(record.Source, s.Config.Source),
		"collected_at":       util.FormatCHDateTime64Millis(record.CollectedAt),
	}
	if err := s.Client.InsertJSONEachRow(s.Config.CollectLogTable, []map[string]any{row}); err != nil {
		return sanitizeStoreError("insert_collect_log", err)
	}
	return nil
}

func (s *ClickHouseStore) InsertFrontier(ctx context.Context, record FrontierRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	row := map[string]any{
		"frontier_uuid":          record.FrontierUUID,
		"run_uuid":               record.RunUUID,
		"version":                record.Version,
		"provider":               defaultString(record.Key.Provider, "kakao"),
		"mode":                   record.Key.Mode,
		"normalized_query":       normalizeQuery(record.Key.Query),
		"query_hash":             record.QueryHash,
		"target":                 record.Key.Target,
		"sort":                   record.Key.Sort,
		"last_run_at":            util.FormatCHDateTime64Millis(record.LastRunAt),
		"last_success_at":        nullableTime(record.LastSuccessAt),
		"next_due_at":            util.FormatCHDateTime64Millis(record.NextDueAt),
		"last_total_count":       nonNegative(record.LastTotalCount),
		"last_pageable_count":    nonNegative(record.LastPageableCount),
		"calls_last_run":         nonNegative(record.CallsLastRun),
		"documents_last_run":     nonNegative(record.DocumentsLastRun),
		"new_isbn_last_run":      nonNegative(record.NewISBNLastRun),
		"changed_isbn_last_run":  nonNegative(record.ChangedISBNLastRun),
		"duplicate_ratio":        clamp01(record.DuplicateRatio),
		"yield_per_call":         maxFloat(record.YieldPerCall, 0),
		"consecutive_zero_yield": nonNegative(record.ConsecutiveZero),
		"priority_score":         clamp01(record.PriorityScore),
		"active":                 boolUInt8(record.Active),
		"source":                 defaultString(record.Source, s.Config.Source),
		"created_at":             util.FormatCHDateTime64Millis(record.LastRunAt),
	}
	if err := s.Client.InsertJSONEachRow(s.Config.FrontierTable, []map[string]any{row}); err != nil {
		return sanitizeStoreError("insert_frontier", err)
	}
	return nil
}

func (s *ClickHouseStore) tableExists(ctx context.Context, table string) error {
	for attempt := 1; attempt <= 3; attempt++ {
		exists, err := s.Client.TableExists(table)
		if err == nil && exists {
			return nil
		}
		if err == nil {
			return &StoreError{Operation: "preflight_table", Category: "clickhouse_contract"}
		}
		if !retryableStoreError(err) || attempt == 3 {
			return sanitizeStoreError("preflight_table", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	return &StoreError{Operation: "preflight_table", Category: "unknown"}
}

func (s *ClickHouseStore) checkGrant(ctx context.Context, privilege, table string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.Client.Exec(fmt.Sprintf("CHECK GRANT %s ON %s", privilege, table)); err != nil {
		return sanitizeStoreError("preflight_grant", err)
	}
	return nil
}

func (s *ClickHouseStore) validateConnectionBoundary() error {
	host := strings.TrimSpace(s.Client.Host)
	parsedHost := host
	protocol := strings.ToLower(strings.TrimSpace(s.Client.Protocol))
	if parsed, err := url.Parse(host); err == nil && parsed.Host != "" {
		parsedHost = parsed.Hostname()
		protocol = strings.ToLower(parsed.Scheme)
	} else if splitHost, _, err := net.SplitHostPort(host); err == nil {
		parsedHost = splitHost
	}
	parsedHost = strings.Trim(parsedHost, "[]")
	if parsedHost == "localhost" || parsedHost == "0.0.0.0" || parsedHost == "::" {
		return &StoreError{Operation: "preflight_connection", Category: "clickhouse_contract"}
	}
	if ip := net.ParseIP(parsedHost); ip != nil && ip.IsLoopback() {
		return &StoreError{Operation: "preflight_connection", Category: "clickhouse_contract"}
	}
	if s.Config.RequireHTTPS && protocol != "https" {
		return &StoreError{Operation: "preflight_connection", Category: "clickhouse_contract"}
	}
	return nil
}

func sanitizeStoreError(operation string, err error) error {
	category := "clickhouse_contract"
	if retryableStoreError(err) {
		category = "clickhouse_transient"
	}
	return &StoreError{Operation: operation, Category: category, Reason: safeStoreErrorReason(err)}
}

func safeStoreErrorReason(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToLower(err.Error())
	for marker, reason := range map[string]string{
		"http status=400": "query_rejected",
		"http status=401": "auth_or_permission",
		"http status=403": "auth_or_permission",
		"http status=404": "object_unavailable",
		"http status=408": "read_timeout",
		"http status=429": "query_admission",
		"http status=500": "server_unavailable",
		"http status=502": "server_unavailable",
		"http status=503": "server_unavailable",
		"http status=504": "server_unavailable",
	} {
		if strings.Contains(message, marker) {
			return reason
		}
	}
	for _, marker := range []string{"timeout", "deadline"} {
		if strings.Contains(message, marker) {
			return "transport_timeout"
		}
	}
	for _, marker := range []string{"connection reset", "connection refused", "broken pipe", "unexpected eof"} {
		if strings.Contains(message, marker) {
			return "transport_interrupted"
		}
	}
	return "request_failed"
}

func retryableStoreError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary()) {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"timeout", "deadline", "connection reset", "connection refused",
		"connection aborted", "broken pipe", "unexpected eof",
		"not initialized", "keeper", "coordination", "readonly",
		"http status=408", "http status=429", "http status=500", "http status=502",
		"http status=503", "http status=504",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func allowedErrorCategory(category string) string {
	category = strings.TrimSpace(category)
	switch category {
	case "":
		return ""
	case "auth_failed", "invalid_request", "permission_denied",
		"quota_exhausted", "rate_limited", "timeout", "network",
		"unavailable", "contract_error", "clickhouse_transient",
		"clickhouse_contract", "budget_exhausted", "unknown":
		return category
	default:
		return "unknown"
	}
}

func normalizeQuery(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func rawWriteTimeoutFromEnv() time.Duration {
	seconds := envx.Int("KAKAO_CLICKHOUSE_RAW_WRITE_TIMEOUT_SECONDS", 660)
	if seconds < 60 || seconds > 900 {
		seconds = 660
	}
	return time.Duration(seconds) * time.Second
}

func cleanStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func parseTime(value string) time.Time {
	value = strings.TrimSpace(value)
	for _, layout := range []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	} {
		if parsed, err := time.ParseInLocation(layout, value, util.KST()); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func toFloat64(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint64:
		return float64(typed)
	case string:
		parsed, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed
	default:
		return 0
	}
}

func toBool(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes":
			return true
		}
	default:
		return util.ToInt64(value) != 0
	}
	return false
}

func boolUInt8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func nonNegative(value int) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

func nonNegative64(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return util.FormatCHDateTime64Millis(value)
}

func clamp01(value float64) float64 {
	return math.Min(1, math.Max(0, value))
}

func maxFloat(value, minimum float64) float64 {
	return math.Max(value, minimum)
}

func defaultString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
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
