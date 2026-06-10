package collector

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/kafkaingest"
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
	Client    *ch.Client
	Table     string
	Columns   map[string]bool
	Keys      []naver.APIKey
	Rand      *rand.Rand
	Publisher *kafkaingest.Publisher
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
	if client != nil {
		if cols, err := client.QueryColumnNames(table); err == nil {
			columns = cols
		} else {
			fmt.Printf("[warn] ClickHouse column lookup skipped table=%s.%s error=%v\n", client.Database, table, err)
		}
	}

	publisher, err := kafkaingest.NewFromEnv()
	if err != nil {
		return nil, err
	}

	return &Collector{
		Client:    client,
		Table:     table,
		Columns:   columns,
		Keys:      keys,
		Rand:      rand.New(rand.NewSource(seed)),
		Publisher: publisher,
	}, nil
}

func (c *Collector) ValidateIngest(ctx context.Context) error {
	return c.Publisher.Validate(ctx)
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
		fmt.Printf("[warn] existing book sample skipped table=%s error=%v; retrying raw table=%s\n", sampleTable, err, c.Table)
		rows, err = rawnaver.SampleTitleAuthorPublisher(c.Client, c.Table, limit)
	}
	if err != nil {
		fmt.Printf("[warn] existing book sample unavailable error=%v\n", err)
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
		fmt.Printf("[warn] r_package source catalog unavailable; using %d fallback R package candidates for Kafka-only run\n", len(out))
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
			_ = c.publishSearchLog(meta, "ERROR", 0, err.Error(), fmt.Sprintf("auto_search_error|mode=%s|term=%s|sort=%s", mode, term, sort))
			return err
		}
		if err := c.publishSearchLog(meta, "OK", len(items), "", fmt.Sprintf("auto_search|mode=%s|term=%s|sort=%s", mode, term, sort)); err != nil {
			return err
		}
		if len(items) == 0 {
			continue
		}
		if err := c.upsertItems(items, fmt.Sprintf("auto_upsert|mode=%s|term=%s|sort=%s", mode, term, sort), "github_actions_auto", false, meta); err != nil {
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
		query := c.rPackageBookQuery(pkg)
		if query == "" {
			continue
		}
		if err := c.CollectTerm(query, "r_package", reqsPerTerm, display); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) rPackageBookQuery(pkg RPackageCandidate) string {
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
				_ = c.publishSearchLog(meta, "ERROR", 0, err.Error(), fmt.Sprintf("manual_search_error|keyword=%s|sort=%s|start=%d", keyword, sort, start))
				return err
			}
			if err := c.publishSearchLog(meta, "OK", len(items), "", fmt.Sprintf("manual_search|keyword=%s|sort=%s|start=%d", keyword, sort, start)); err != nil {
				return err
			}
			if len(items) == 0 {
				break
			}
			if err := c.upsertItems(items, fmt.Sprintf("manual_upsert|keyword=%s|sort=%s|start=%d", keyword, sort, start), "github_actions_manual", true, meta); err != nil {
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
				_ = c.publishSearchLog(meta, "ERROR", 0, err.Error(), fmt.Sprintf("aladin_publisher_seed_error|publisher=%s|sort=%s|start=%d", publisher, sort, start))
				return err
			}
			if start == 1 && maxTotal > 0 && t > maxTotal {
				if err := c.publishSearchLog(meta, "SKIP", 0, "", fmt.Sprintf("aladin_publisher_seed_skip|publisher=%s|sort=%s|total=%d|max_total=%d", publisher, sort, t, maxTotal)); err != nil {
					return err
				}
				fmt.Printf("[SKIP] publisher=%q sort=%s total=%d max_total=%d\n", publisher, sort, t, maxTotal)
				break
			}
			if err := c.publishSearchLog(meta, "OK", len(items), "", fmt.Sprintf("aladin_publisher_seed_search|publisher=%s|sort=%s|start=%d", publisher, sort, start)); err != nil {
				return err
			}
			if len(items) == 0 {
				break
			}
			if err := c.upsertItems(items, fmt.Sprintf("aladin_publisher_seed|publisher=%s|sort=%s|start=%d", publisher, sort, start), "github_actions_auto", false, meta); err != nil {
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
	if c.Client != nil && len(c.Columns) > 0 {
		var err error
		existingMap, err = rawnaver.BuildExistingMap(c.Client, c.Table, c.Columns, isbns)
		if err != nil {
			fmt.Printf("[warn] existing ISBN lookup skipped table=%s.%s error=%v\n", c.Client.Database, c.Table, err)
			existingMap = map[string]rawnaver.ExistingInfo{}
		}
	}

	uuidMap := map[string]string{}
	if preserveCreated && c.Client != nil {
		var err error
		uuidMap, err = rawnaver.BuildUUIDMap(c.Client, c.Table, isbns)
		if err != nil {
			fmt.Printf("[warn] UUID lookup skipped table=%s.%s error=%v\n", c.Client.Database, c.Table, err)
			uuidMap = map[string]string{}
		}
	}

	events := make([]kafkaingest.Event, 0, len(items))
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
		row["source"] = c.Publisher.Cfg.ProducerSource
		row["collected_at"] = nowStr

		sourceURL := item.Link
		if strings.TrimSpace(sourceURL) == "" {
			sourceURL = naverSearchURL(meta)
		}
		ev, err := c.Publisher.NewEvent("book.naver.raw.v1", "", sourceURL, nowStr, row)
		if err != nil {
			return err
		}
		events = append(events, ev)
	}

	if len(events) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.Publisher.Publish(ctx, events); err != nil {
		return err
	}
	fmt.Printf("[kafka] published raw events=%d topic=%s mode=%s query=%q sort=%s start=%d\n", len(events), c.Publisher.Cfg.KafkaTopic, meta.Mode, meta.Query, meta.Sort, meta.Start)
	return nil
}

func (c *Collector) publishSearchLog(meta SearchMeta, status string, fetchedCount int, errText, collectLog string) error {
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
		"source":         c.Publisher.Cfg.ProducerSource,
		"created_at":     nowStr,
	}
	sourceURL := naverSearchURL(meta)
	ev, err := c.Publisher.NewEvent("book.naver.search_log.v1", "", sourceURL, nowStr, payload)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.Publisher.Publish(ctx, []kafkaingest.Event{ev})
}

func naverSearchURL(meta SearchMeta) string {
	q := url.Values{}
	q.Set("query", meta.Query)
	q.Set("display", fmt.Sprintf("%d", meta.Display))
	q.Set("start", fmt.Sprintf("%d", meta.Start))
	q.Set("sort", meta.Sort)
	return naver.BookSearchURL + "?" + q.Encode()
}
