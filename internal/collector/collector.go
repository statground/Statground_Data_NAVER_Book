package collector

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/dbingest"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/rawnaver"
	"statground_naver_book_go/internal/terms"
	"statground_naver_book_go/internal/util"
)

type SearchMeta struct {
	Mode    string
	Query   string
	Sort    string
	Start   int
	Display int
	Total   int
}

type Collector struct {
	Client  *ch.Client
	Table   string
	Columns map[string]bool
	Keys    []naver.APIKey
	Rand    *rand.Rand
	Ingest  *dbingest.Writer

	existingLookupDisabled      bool
	existingLookupDisableLogged bool
}

type RPackageCandidate struct {
	PackageName   string
	Repository    string
	LatestVersion string
	Title         string
}

var defaultRPackageFallbackNames = []string{
	"dplyr",
	"ggplot2",
	"data.table",
	"shiny",
	"tidyr",
	"readr",
	"stringr",
	"purrr",
	"lubridate",
	"caret",
	"xgboost",
	"randomForest",
	"lme4",
	"survival",
	"sf",
	"plotly",
	"rmarkdown",
	"knitr",
	"httr",
	"jsonlite",
}

func New(client *ch.Client, table string, keys []naver.APIKey, seed int64) (*Collector, error) {
	if table == "" {
		table = "naver_book_raw"
	}

	columns := map[string]bool{}
	if client != nil && existingISBNLookupEnabled() {
		if cols, err := client.QueryColumnNames(table); err == nil {
			columns = cols
		} else {
			fmt.Printf("[warn] ClickHouse column lookup skipped table=raw error=%s\n", ShortOperationalError(err))
		}
	}

	ingest, err := dbingest.NewFromEnv(client, table)
	if err != nil {
		return nil, err
	}

	return &Collector{
		Client:  client,
		Table:   table,
		Columns: columns,
		Keys:    keys,
		Rand:    rand.New(rand.NewSource(seed)),
		Ingest:  ingest,
	}, nil
}

func (c *Collector) ValidateIngest(ctx context.Context) error {
	return c.Ingest.Validate(ctx)
}

func (c *Collector) SampleRows(limit int) ([]map[string]any, error) {
	if c.Client == nil {
		return []map[string]any{}, nil
	}
	sampleTable := strings.TrimSpace(envx.String("BOOK_SAMPLE_TABLE", "Data_Book_NAVER_Service.naver_book_latest"))
	if sampleTable == "" {
		sampleTable = c.Table
	}
	rows, err := rawnaver.SampleTitleAuthorPublisher(c.Client, sampleTable, limit)
	if err != nil && sampleTable != c.Table {
		fmt.Printf("[warn] existing book sample skipped table=sample error=%s; retrying raw table\n", ShortOperationalError(err))
		rows, err = rawnaver.SampleTitleAuthorPublisher(c.Client, c.Table, limit)
	}
	if err != nil {
		fmt.Printf("[warn] existing book sample unavailable error=%s\n", ShortOperationalError(err))
		return []map[string]any{}, nil
	}
	return rows, nil
}

func (c *Collector) SampleRPackages(limit int) ([]RPackageCandidate, error) {
	if limit <= 0 {
		limit = 10
	}
	if c.Client == nil {
		out := fallbackRPackageCandidates(limit)
		fmt.Printf("[warn] r_package source catalog unavailable; using %d fallback R package candidates\n", len(out))
		return out, nil
	}
	sql := fmt.Sprintf(`
        SELECT package_name, repository, latest_version, title
        FROM Data_R_Package_Mart.v_package_catalog_latest
        WHERE notEmpty(package_name)
        ORDER BY rand()
        LIMIT %d
        SETTINGS distributed_product_mode = 'global'
    `, limit)
	rows, err := c.Client.QueryJSONEachRow(sql)
	if err != nil {
		sql = fmt.Sprintf(`
            SELECT package_name, repository, latest_version, title
            FROM
	            (
	                SELECT package_key,
	                       any(package_name) AS package_name,
	                       argMax(repository, identity_sort_key) AS repository,
	                       argMax(latest_version, identity_sort_key) AS latest_version,
	                       argMax(title, identity_sort_key) AS title
	                  FROM
	                (
	                    SELECT lowerUTF8(package_name) AS package_key,
	                           package_name,
	                           repository,
	                           latest_version,
	                           title,
	                           (
	                               multiIf(repository = 'CRAN', 3, repository = 'Bioconductor', 2, repository = 'R-universe', 1, 0),
	                               version
	                           ) AS identity_sort_key
	                      FROM Data_R_Package_Service.package_current
	                     WHERE notEmpty(package_name)
	                )
	                 GROUP BY package_key
	            )
            ORDER BY rand()
            LIMIT %d
            SETTINGS distributed_product_mode = 'global'
        `, limit)
		rows, err = c.Client.QueryJSONEachRow(sql)
		if err != nil {
			return nil, err
		}
	}
	out := make([]RPackageCandidate, 0, len(rows))
	for _, row := range rows {
		packageName := strings.TrimSpace(util.ToString(row["package_name"]))
		if packageName == "" {
			continue
		}
		out = append(out, RPackageCandidate{
			PackageName:   packageName,
			Repository:    strings.TrimSpace(util.ToString(row["repository"])),
			LatestVersion: strings.TrimSpace(util.ToString(row["latest_version"])),
			Title:         strings.TrimSpace(util.ToString(row["title"])),
		})
	}
	return out, nil
}

func fallbackRPackageCandidates(limit int) []RPackageCandidate {
	rawNames := envx.String("R_PACKAGE_FALLBACK_PACKAGES", "")
	names := make([]string, 0)
	for _, name := range strings.Split(rawNames, ",") {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		names = defaultRPackageFallbackNames
	}
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}
	out := make([]RPackageCandidate, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, RPackageCandidate{
			PackageName: name,
			Repository:  "fallback",
		})
	}
	return out
}

func (c *Collector) CollectTerm(term, mode string, reqsPerTerm, display int) error {
	term = terms.SanitizeKeyword(term, c.Rand)
	if display <= 0 {
		display = 100
	}
	sorts := []string{"sim", "date"}
	if reqsPerTerm <= 1 {
		sorts = []string{sorts[c.Rand.Intn(len(sorts))]}
	}
	for _, sort := range sorts {
		total, items, err := naver.FetchItems(term, sort, 1, display, c.Keys, c.Rand)
		meta := SearchMeta{Mode: mode, Query: term, Sort: sort, Start: 1, Display: display, Total: total}
		if err != nil {
			_ = c.publishSearchLogBestEffort(meta, "ERROR", 0, err.Error(), fmt.Sprintf("auto_search_error|mode=%s|term=%s|sort=%s", mode, term, sort))
			return err
		}
		if len(items) == 0 {
			if err := c.publishSearchLogBestEffort(meta, "OK", len(items), "", fmt.Sprintf("auto_search|mode=%s|term=%s|sort=%s", mode, term, sort)); err != nil {
				return err
			}
			continue
		}
		if err := c.upsertItems(items, fmt.Sprintf("auto_upsert|mode=%s|term=%s|sort=%s", mode, term, sort), "github_actions_auto", false, meta); err != nil {
			return err
		}
		if err := c.publishSearchLogBestEffort(meta, "OK", len(items), "", fmt.Sprintf("auto_search|mode=%s|term=%s|sort=%s", mode, term, sort)); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) CollectRPackageBooks(sampleSize, reqsPerTerm, display int) error {
	if sampleSize <= 0 {
		sampleSize = 10
	}
	packages, err := c.SampleRPackages(sampleSize)
	if err != nil {
		return err
	}
	for _, pkg := range packages {
		query := c.RPackageBookQuery(pkg)
		if query == "" {
			continue
		}
		if err := c.CollectTerm(query, "r_package", reqsPerTerm, display); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) RPackageBookQuery(pkg RPackageCandidate) string {
	name := strings.TrimSpace(pkg.PackageName)
	if name == "" {
		return ""
	}
	templates := []string{
		"%s with R",
		"%s R package",
		"%s R 패키지",
	}
	if c.Rand == nil {
		return fmt.Sprintf(templates[0], name)
	}
	return fmt.Sprintf(templates[c.Rand.Intn(len(templates))], name)
}

func (c *Collector) CollectManual(keyword string) error {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return fmt.Errorf("MANUAL_KEYWORD is empty")
	}
	for _, sort := range []string{"sim", "date"} {
		for start := 1; start <= 1000; start += 100 {
			total, items, err := naver.FetchItems(keyword, sort, start, 100, c.Keys, c.Rand)
			meta := SearchMeta{Mode: "manual", Query: keyword, Sort: sort, Start: start, Display: 100, Total: total}
			if err != nil {
				_ = c.publishSearchLogBestEffort(meta, "ERROR", 0, err.Error(), fmt.Sprintf("manual_search_error|keyword=%s|sort=%s|start=%d", keyword, sort, start))
				return err
			}
			if len(items) == 0 {
				if err := c.publishSearchLogBestEffort(meta, "OK", len(items), "", fmt.Sprintf("manual_search|keyword=%s|sort=%s|start=%d", keyword, sort, start)); err != nil {
					return err
				}
				break
			}
			if err := c.upsertItems(items, fmt.Sprintf("manual_upsert|keyword=%s|sort=%s|start=%d", keyword, sort, start), "github_actions_manual", true, meta); err != nil {
				return err
			}
			if err := c.publishSearchLogBestEffort(meta, "OK", len(items), "", fmt.Sprintf("manual_search|keyword=%s|sort=%s|start=%d", keyword, sort, start)); err != nil {
				return err
			}
			if len(items) < 100 {
				break
			}
		}
	}
	return nil
}

func (c *Collector) CollectPublisherAllPages(publisher string, reqsPerTerm, display int, sleepMin, sleepMax float64, maxTotal int) error {
	publisher = strings.TrimSpace(publisher)
	if publisher == "" {
		return nil
	}
	if display <= 0 {
		display = 100
	}
	time.Sleep(randomDuration(c.Rand, sleepMin, sleepMax))
	sorts := []string{"date"}
	if reqsPerTerm > 1 {
		sorts = []string{"sim", "date"}
	}
	for _, sort := range sorts {
		start := 1
		total := 0
		fetched := 0
		for {
			if start > 1000 {
				break
			}
			t, items, err := naver.FetchItems(publisher, sort, start, display, c.Keys, c.Rand)
			if total == 0 {
				total = t
			}
			meta := SearchMeta{Mode: "aladin_publisher_seed", Query: publisher, Sort: sort, Start: start, Display: display, Total: t}
			if err != nil {
				_ = c.publishSearchLogBestEffort(meta, "ERROR", 0, err.Error(), fmt.Sprintf("aladin_publisher_seed_error|publisher=%s|sort=%s|start=%d", publisher, sort, start))
				return err
			}
			if start == 1 && maxTotal > 0 && t > maxTotal {
				if err := c.publishSearchLogBestEffort(meta, "SKIP", 0, "", fmt.Sprintf("aladin_publisher_seed_skip|publisher=%s|sort=%s|total=%d|max_total=%d", publisher, sort, t, maxTotal)); err != nil {
					return err
				}
				fmt.Printf("[SKIP] publisher=%q sort=%s total=%d max_total=%d\n", publisher, sort, t, maxTotal)
				break
			}
			if len(items) == 0 {
				if err := c.publishSearchLogBestEffort(meta, "OK", len(items), "", fmt.Sprintf("aladin_publisher_seed_search|publisher=%s|sort=%s|start=%d", publisher, sort, start)); err != nil {
					return err
				}
				break
			}
			if err := c.upsertItems(items, fmt.Sprintf("aladin_publisher_seed|publisher=%s|sort=%s|start=%d", publisher, sort, start), "github_actions_auto", false, meta); err != nil {
				return err
			}
			if err := c.publishSearchLogBestEffort(meta, "OK", len(items), "", fmt.Sprintf("aladin_publisher_seed_search|publisher=%s|sort=%s|start=%d", publisher, sort, start)); err != nil {
				return err
			}
			fetched += len(items)
			start += display
			if total > 0 && start > total {
				break
			}
			time.Sleep(randomDuration(c.Rand, sleepMin, sleepMax))
		}
		fmt.Printf("[NAVER] publisher=%q sort=%s total=%d fetched=%d\n", publisher, sort, total, fetched)
	}
	return nil
}

func randomDuration(r *rand.Rand, minV, maxV float64) time.Duration {
	if maxV <= 0 {
		return 0
	}
	if minV < 0 {
		minV = 0
	}
	if maxV < minV {
		maxV = minV
	}
	seconds := minV
	if maxV > minV {
		if r == nil {
			seconds += rand.Float64() * (maxV - minV)
		} else {
			seconds += r.Float64() * (maxV - minV)
		}
	}
	return time.Duration(seconds * float64(time.Second))
}

func (c *Collector) upsertItems(items []naver.BookItem, updatedLog, defaultCreatedLog string, preserveCreated bool, meta SearchMeta) error {
	now := util.NowKST()
	nowStr := util.FormatCHDateTime64Millis(now)
	version := uint64(now.UnixMilli())

	isbns := make([]string, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.ISBN) != "" {
			isbns = append(isbns, item.ISBN)
		}
	}

	existingMap := map[string]rawnaver.ExistingInfo{}
	if c.shouldLookupExistingISBNs() {
		var err error
		existingMap, err = rawnaver.BuildExistingMap(c.Client, c.Table, c.Columns, isbns)
		if err != nil {
			category := ShortOperationalError(err)
			fmt.Printf("[warn] existing ISBN lookup skipped table=raw error=%s\n", category)
			if shouldDisableExistingLookup(err) {
				c.existingLookupDisabled = true
				if !c.existingLookupDisableLogged {
					c.existingLookupDisableLogged = true
					fmt.Printf("[warn] existing ISBN lookup disabled for this run after error=%s\n", category)
				}
			}
			existingMap = map[string]rawnaver.ExistingInfo{}
		}
	}

	uuidMap := map[string]string{}
	if preserveCreated && c.Client != nil {
		var err error
		uuidMap, err = rawnaver.BuildUUIDMap(c.Client, c.Table, isbns)
		if err != nil {
			fmt.Printf("[warn] UUID lookup skipped table=raw error=%s\n", ShortOperationalError(err))
			uuidMap = map[string]string{}
		}
	}

	rows := make([]map[string]any, 0, len(items))
	for _, item := range items {
		isbn := strings.TrimSpace(item.ISBN)
		if isbn == "" {
			continue
		}

		uuid := ""
		createdAt := nowStr
		createdLog := defaultCreatedLog
		if preserveCreated {
			if existingUUID := strings.TrimSpace(uuidMap[isbn]); existingUUID != "" {
				uuid = existingUUID
			}
		}
		if existing, ok := existingMap[isbn]; ok {
			if strings.TrimSpace(existing.UUID) != "" {
				uuid = existing.UUID
			}
			if strings.TrimSpace(existing.CreatedAt) != "" {
				createdAt = existing.CreatedAt
			}
			if strings.TrimSpace(existing.CreatedLog) != "" {
				createdLog = existing.CreatedLog
			}
		}
		if uuid == "" {
			uuid = util.UUIDv7()
		}

		row := rawnaver.BuildRawNaverRow(item, nowStr, int64(version), createdAt, createdLog, updatedLog, uuid)
		row["provider"] = "naver"
		row["search_mode"] = meta.Mode
		row["search_query"] = meta.Query
		row["search_sort"] = meta.Sort
		row["search_start"] = meta.Start
		row["search_display"] = meta.Display
		row["api_total"] = meta.Total
		row["source"] = c.Ingest.Cfg.ProducerSource
		row["collected_at"] = nowStr

		sourceURL := item.Link
		if strings.TrimSpace(sourceURL) == "" {
			sourceURL = naverSearchURL(meta)
		}
		ev, err := c.Ingest.NewEvent("book.naver.raw.v1", "", sourceURL, nowStr, row)
		if err != nil {
			return err
		}
		row["event_uuid"] = ev.EventUUID
		row["kafka_topic"] = c.Ingest.Cfg.DirectTopic
		row["kafka_partition"] = 0
		row["kafka_offset"] = 0
		row["payload"] = ev.Payload
		row["ingested_at"] = nowStr
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return nil
	}

	if err := c.Ingest.InsertRawRows(rows); err != nil {
		return err
	}
	fmt.Printf("[db] inserted raw rows=%d table=%s mode=%s query=%q sort=%s start=%d\n", len(rows), c.Ingest.Cfg.RawTable, meta.Mode, meta.Query, meta.Sort, meta.Start)
	return nil
}

func (c *Collector) publishSearchLog(meta SearchMeta, status string, fetchedCount int, errText, collectLog string) error {
	return c.publishSearchLogWithTimeout(meta, status, fetchedCount, errText, collectLog, publishTimeoutDuration("DB_LOG_WRITE_TIMEOUT_SECONDS", 45*time.Second))
}

func (c *Collector) publishSearchLogWithTimeout(meta SearchMeta, status string, fetchedCount int, errText, collectLog string, timeout time.Duration) error {
	_ = timeout
	nowStr := util.FormatCHDateTime64Millis(util.NowKST())
	payload := map[string]any{
		"log_uuid":       util.UUIDv7(),
		"provider":       "naver",
		"search_mode":    meta.Mode,
		"search_query":   meta.Query,
		"search_sort":    meta.Sort,
		"search_start":   meta.Start,
		"search_display": meta.Display,
		"api_total":      meta.Total,
		"fetched_count":  fetchedCount,
		"status":         status,
		"error":          errText,
		"collect_log":    collectLog,
		"source":         c.Ingest.Cfg.ProducerSource,
		"created_at":     nowStr,
	}
	sourceURL := naverSearchURL(meta)
	ev, err := c.Ingest.NewEvent("book.naver.search_log.v1", "", sourceURL, nowStr, payload)
	if err != nil {
		return err
	}
	row := map[string]any{
		"log_uuid":       payload["log_uuid"],
		"event_uuid":     ev.EventUUID,
		"provider":       "naver",
		"source":         ev.Source,
		"host":           ev.Host,
		"ip":             ev.IP,
		"search_mode":    meta.Mode,
		"search_query":   meta.Query,
		"search_sort":    meta.Sort,
		"search_start":   meta.Start,
		"search_display": meta.Display,
		"api_total":      meta.Total,
		"fetched_count":  fetchedCount,
		"status":         status,
		"error":          errText,
		"collect_log":    collectLog,
		"url":            sourceURL,
		"payload":        ev.Payload,
		"created_at":     nowStr,
		"ingested_at":    nowStr,
	}
	return c.Ingest.WithTimeout(timeout).InsertCollectLogRows([]map[string]any{row})
}

func (c *Collector) publishSearchLogBestEffort(meta SearchMeta, status string, fetchedCount int, errText, collectLog string) error {
	required := searchLogRequired()
	timeout := publishTimeoutDuration("DB_LOG_WRITE_TIMEOUT_SECONDS", 45*time.Second)
	if !required {
		timeout = searchLogBestEffortTimeout()
	}
	err := c.publishSearchLogWithTimeout(meta, status, fetchedCount, errText, collectLog, timeout)
	if err == nil || required {
		return err
	}
	fmt.Printf("[warn] search log publish skipped mode=%s query=%q sort=%s start=%d status=%s error=%s\n", meta.Mode, meta.Query, meta.Sort, meta.Start, status, ShortOperationalError(err))
	return nil
}

func publishTimeoutDuration(envName string, fallback time.Duration) time.Duration {
	seconds := envx.Float(envName, fallback.Seconds())
	if seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds * float64(time.Second))
}

func (c *Collector) shouldLookupExistingISBNs() bool {
	return c.Client != nil && len(c.Columns) > 0 && existingISBNLookupEnabled() && !c.existingLookupDisabled
}

func existingISBNLookupEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(envx.String("EXISTING_ISBN_LOOKUP_ENABLED", "true")))
	return value != "0" && value != "false" && value != "no" && value != "off"
}

func IngestPreflightRequired() bool {
	value := strings.ToLower(strings.TrimSpace(envx.String("DB_PREFLIGHT_REQUIRED", envx.String("INGEST_PREFLIGHT_REQUIRED", "true"))))
	return value != "0" && value != "false" && value != "no" && value != "off"
}

func ShouldSkipIngestPreflightError(err error) bool {
	return err != nil && !IngestPreflightRequired() && IsRetryableOperationalError(err)
}

func searchLogRequired() bool {
	value := strings.ToLower(strings.TrimSpace(envx.String("SEARCH_LOG_REQUIRED", "true")))
	return value != "0" && value != "false" && value != "no" && value != "off"
}

func searchLogBestEffortTimeout() time.Duration {
	fallback := publishTimeoutDuration("DB_LOG_WRITE_TIMEOUT_SECONDS", 45*time.Second)
	if fallback > 8*time.Second {
		fallback = 8 * time.Second
	}
	return publishTimeoutDuration("DB_LOG_BEST_EFFORT_TIMEOUT_SECONDS", fallback)
}

func shouldDisableExistingLookup(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "too_many_simultaneous_queries") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "client.timeout exceeded") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "timeout")
}

func IsRetryableOperationalError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "too_many_simultaneous_queries") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "client.timeout exceeded") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "temporary") ||
		strings.Contains(msg, "naver api http 429") ||
		strings.Contains(msg, "naver api http 500") ||
		strings.Contains(msg, "naver api http 502") ||
		strings.Contains(msg, "naver api http 503") ||
		strings.Contains(msg, "naver api http 504")
}

func ShortOperationalError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "too_many_simultaneous_queries"):
		return "clickhouse_too_many_queries"
	case strings.Contains(msg, "access_denied"), strings.Contains(msg, "not enough privileges"):
		return "clickhouse_access_denied"
	case strings.Contains(msg, "context deadline exceeded"),
		strings.Contains(msg, "client.timeout exceeded"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "naver api http 429"):
		return "naver_rate_limited"
	case strings.Contains(msg, "naver api http 500"),
		strings.Contains(msg, "naver api http 502"),
		strings.Contains(msg, "naver api http 503"),
		strings.Contains(msg, "naver api http 504"):
		return "naver_unavailable"
	case strings.Contains(msg, "connection reset"), strings.Contains(msg, "broken pipe"):
		return "network"
	default:
		return "operational_error"
	}
}

func naverSearchURL(meta SearchMeta) string {
	q := url.Values{}
	q.Set("query", meta.Query)
	q.Set("display", fmt.Sprintf("%d", meta.Display))
	q.Set("start", fmt.Sprintf("%d", meta.Start))
	q.Set("sort", meta.Sort)
	return naver.BookSearchURL + "?" + q.Encode()
}
