package stats

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/charts"
	"statground_naver_book_go/internal/util"
)

type Series struct {
	Labels []string
	Values []int64
}

const (
	ColorBooks      = "#1f77b4"
	ColorAuthors    = "#2ca02c"
	ColorPublishers = "#ff7f0e"
)

func queryInt(client *ch.Client, sql string) (int64, error) {
	row, err := client.QuerySingleRow(sql)
	if err != nil {
		return 0, err
	}
	if v, ok := row["value"]; ok {
		return util.ToInt64(v), nil
	}
	for _, v := range row {
		return util.ToInt64(v), nil
	}
	return 0, nil
}

func queryString(client *ch.Client, sql string) (string, error) {
	row, err := client.QuerySingleRow(sql)
	if err != nil {
		return "", err
	}
	if v, ok := row["value"]; ok {
		return strings.TrimSpace(util.ToString(v)), nil
	}
	for _, v := range row {
		return strings.TrimSpace(util.ToString(v)), nil
	}
	return "", nil
}

func querySeries(client *ch.Client, sql string) (Series, error) {
	rows, err := client.QueryJSONEachRow(sql)
	if err != nil {
		return Series{}, err
	}
	series := Series{Labels: make([]string, 0, len(rows)), Values: make([]int64, 0, len(rows))}
	for _, row := range rows {
		series.Labels = append(series.Labels, util.ToString(row["label"]))
		series.Values = append(series.Values, util.ToInt64(row["c"]))
	}
	return series, nil
}

func fmtCount(v int64) string {
	s := fmt.Sprintf("%d", v)
	if len(s) <= 3 {
		return s
	}
	rem := len(s) % 3
	if rem == 0 {
		rem = 3
	}
	var b strings.Builder
	b.WriteString(s[:rem])
	for i := rem; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func firstSeenSource(table, mode string) (sourceFrom, groupExpr, where string) {
	baseIsbn := "isbn IS NOT NULL AND length(trim(isbn)) > 0"
	basePub := "publisher IS NOT NULL AND length(trim(publisher)) > 0"
	switch mode {
	case "books":
		return table, "isbn", baseIsbn
	case "publishers":
		return table, "publisher", basePub
	case "authors":
		return table + " ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one", "trim(author_one)", "length(trim(author_one)) > 0"
	default:
		panic("unsupported mode: " + mode)
	}
}

func buildFirstSeenSeriesSQL(table, mode, granularity string) string {
	sourceFrom, groupExpr, whereExpr := firstSeenSource(table, mode)
	switch granularity {
	case "year":
		return fmt.Sprintf(`
            WITH source AS (
                SELECT %s AS grp, created_at
                FROM %s
                WHERE %s
            ),
            first_seen AS (
                SELECT grp, min(created_at) AS first_at
                FROM source
                GROUP BY grp
            ),
            params AS (
                SELECT
                    toStartOfYear(min(first_at)) AS min_y,
                    dateDiff('year', toStartOfYear(min(first_at)), toStartOfYear(max(first_at))) AS diff_y
                FROM first_seen
                HAVING count() > 0
            ),
            counts AS (
                SELECT toYear(first_at) AS y, count() AS c
                FROM first_seen
                GROUP BY y
            ),
            timeline AS (
                SELECT toYear(addYears(p.min_y, n)) AS y
                FROM params p
                ARRAY JOIN range(p.diff_y + 1) AS n
            )
            SELECT toString(t.y) AS label, ifNull(c.c, 0) AS c
            FROM timeline t
            LEFT JOIN counts c ON c.y = t.y
            ORDER BY t.y
        `, groupExpr, sourceFrom, whereExpr)
	case "month":
		return fmt.Sprintf(`
            WITH source AS (
                SELECT %s AS grp, created_at
                FROM %s
                WHERE %s
            ),
            first_seen AS (
                SELECT grp, min(created_at) AS first_at
                FROM source
                GROUP BY grp
            ),
            params AS (
                SELECT
                    toStartOfMonth(min(first_at)) AS min_m,
                    dateDiff('month', toStartOfMonth(min(first_at)), toStartOfMonth(max(first_at))) AS diff_m
                FROM first_seen
                HAVING count() > 0
            ),
            counts AS (
                SELECT toStartOfMonth(first_at) AS m, count() AS c
                FROM first_seen
                GROUP BY m
            ),
            timeline AS (
                SELECT addMonths(p.min_m, n) AS m
                FROM params p
                ARRAY JOIN range(p.diff_m + 1) AS n
            )
            SELECT toString(toYYYYMM(t.m)) AS label, ifNull(c.c, 0) AS c
            FROM timeline t
            LEFT JOIN counts c ON c.m = t.m
            ORDER BY label
        `, groupExpr, sourceFrom, whereExpr)
	case "day":
		return fmt.Sprintf(`
            WITH source AS (
                SELECT %s AS grp, created_at
                FROM %s
                WHERE %s
            ),
            first_seen AS (
                SELECT grp, min(created_at) AS first_at
                FROM source
                GROUP BY grp
            ),
            params AS (
                SELECT
                    toDate(min(first_at)) AS min_d,
                    dateDiff('day', toDate(min(first_at)), toDate(max(first_at))) AS diff_d
                FROM first_seen
                HAVING count() > 0
            ),
            counts AS (
                SELECT toDate(first_at) AS d, count() AS c
                FROM first_seen
                GROUP BY d
            ),
            timeline AS (
                SELECT addDays(p.min_d, n) AS d
                FROM params p
                ARRAY JOIN range(p.diff_d + 1) AS n
            )
            SELECT toString(t.d) AS label, ifNull(c.c, 0) AS c
            FROM timeline t
            LEFT JOIN counts c ON c.d = t.d
            ORDER BY label
        `, groupExpr, sourceFrom, whereExpr)
	case "hour":
		return fmt.Sprintf(`
            WITH source AS (
                SELECT %s AS grp, created_at
                FROM %s
                WHERE %s
            ),
            first_seen AS (
                SELECT grp, min(created_at) AS first_at
                FROM source
                GROUP BY grp
            ),
            params AS (
                SELECT
                    toStartOfHour(min(first_at)) AS min_t,
                    dateDiff('hour', toStartOfHour(min(first_at)), toStartOfHour(max(first_at))) AS diff_h
                FROM first_seen
                HAVING count() > 0
            ),
            counts AS (
                SELECT toStartOfHour(first_at) AS t, count() AS c
                FROM first_seen
                GROUP BY t
            ),
            timeline AS (
                SELECT addHours(p.min_t, n) AS t
                FROM params p
                ARRAY JOIN range(p.diff_h + 1) AS n
            )
            SELECT formatDateTime(t.t, '%%Y-%%m-%%d %%H', 'Asia/Seoul') AS label, ifNull(c.c, 0) AS c
            FROM timeline t
            LEFT JOIN counts c ON c.t = t.t
            ORDER BY label
        `, groupExpr, sourceFrom, whereExpr)
	default:
		panic("unsupported granularity: " + granularity)
	}
}

type pubDateParts struct {
	BaseIsbn      string
	BasePub       string
	PubDigitsExpr string
	PubLenExpr    string
	PubYearExpr   string
	PubMonthExpr  string
	PubDayExpr    string
	PubValidYear  string
	PubValidMonth string
	PubValidDay   string
	PubMonthStart string
	PubDayDate    string
}

func buildPubDateParts() pubDateParts {
	digits := "replaceRegexpAll(trim(pubdate), '[^0-9]', '')"
	length := fmt.Sprintf("length(%s)", digits)
	year := fmt.Sprintf("toUInt16OrZero(substring(%s, 1, 4))", digits)
	month := fmt.Sprintf("toUInt8OrZero(substring(%s, 5, 2))", digits)
	day := fmt.Sprintf("toUInt8OrZero(substring(%s, 7, 2))", digits)
	validYear := fmt.Sprintf("(%s >= 4) AND (%s BETWEEN 1000 AND 2100)", length, year)
	validMonth := fmt.Sprintf("(%s >= 6) AND %s AND (%s BETWEEN 1 AND 12)", length, validYear, month)
	validDay := fmt.Sprintf("(%s >= 8) AND %s AND (%s BETWEEN 1 AND 31)", length, validMonth, day)
	monthStart := fmt.Sprintf("toDateOrNull(concat(toString(%s), '-', lpad(toString(%s), 2, '0'), '-01'))", year, month)
	dayDate := fmt.Sprintf("toDateOrNull(concat(toString(%s), '-', lpad(toString(%s), 2, '0'), '-', lpad(toString(%s), 2, '0')))", year, month, day)
	return pubDateParts{
		BaseIsbn:      "isbn IS NOT NULL AND length(trim(isbn)) > 0",
		BasePub:       "publisher IS NOT NULL AND length(trim(publisher)) > 0",
		PubDigitsExpr: digits,
		PubLenExpr:    length,
		PubYearExpr:   year,
		PubMonthExpr:  month,
		PubDayExpr:    day,
		PubValidYear:  validYear,
		PubValidMonth: validMonth,
		PubValidDay:   validDay,
		PubMonthStart: monthStart,
		PubDayDate:    dayDate,
	}
}

func pubDateSource(table, mode string, p pubDateParts) (sourceFrom, groupExpr, whereBase string) {
	switch mode {
	case "books":
		return table, "isbn", p.BaseIsbn
	case "publishers":
		return table, "publisher", p.BasePub
	case "authors":
		return table + " ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one", "trim(author_one)", "length(trim(author_one)) > 0"
	default:
		panic("unsupported mode: " + mode)
	}
}

func buildPubDateSeriesSQL(table, mode, granularity string, p pubDateParts) string {
	sourceFrom, groupExpr, whereBase := pubDateSource(table, mode, p)
	switch granularity {
	case "year":
		whereExpr := whereBase + " AND " + p.PubValidYear
		return fmt.Sprintf(`
            WITH source AS (
                SELECT DISTINCT %s AS grp, %s AS y
                FROM %s
                WHERE %s
            ),
            params AS (
                SELECT toStartOfYear(toDate(min(y) * 10000 + 101)) AS min_y,
                       dateDiff('year', toStartOfYear(toDate(min(y) * 10000 + 101)), toStartOfYear(toDate(max(y) * 10000 + 101))) AS diff_y
                FROM source
                HAVING count() > 0
            ),
            counts AS (
                SELECT y, count() AS c
                FROM source
                GROUP BY y
            ),
            timeline AS (
                SELECT toYear(addYears(p.min_y, n)) AS y
                FROM params p
                ARRAY JOIN range(p.diff_y + 1) AS n
            )
            SELECT toString(t.y) AS label, ifNull(c.c, 0) AS c
            FROM timeline t
            LEFT JOIN counts c ON c.y = t.y
            ORDER BY label
        `, groupExpr, p.PubYearExpr, sourceFrom, whereExpr)
	case "month":
		whereExpr := whereBase + " AND " + p.PubValidMonth
		return fmt.Sprintf(`
            WITH source AS (
                SELECT DISTINCT %s AS grp, %s AS m
                FROM %s
                WHERE %s
            ),
            params AS (
                SELECT toStartOfMonth(min(m)) AS min_m,
                       dateDiff('month', toStartOfMonth(min(m)), toStartOfMonth(max(m))) AS diff_m
                FROM source
                HAVING count() > 0
            ),
            counts AS (
                SELECT toYYYYMM(m) AS ym, count() AS c
                FROM source
                GROUP BY ym
            ),
            timeline AS (
                SELECT toYYYYMM(addMonths(p.min_m, n)) AS ym
                FROM params p
                ARRAY JOIN range(p.diff_m + 1) AS n
            )
            SELECT toString(t.ym) AS label, ifNull(c.c, 0) AS c
            FROM timeline t
            LEFT JOIN counts c ON c.ym = t.ym
            ORDER BY label
        `, groupExpr, p.PubMonthStart, sourceFrom, whereExpr)
	case "day":
		whereExpr := whereBase + " AND " + p.PubValidDay
		return fmt.Sprintf(`
            WITH source AS (
                SELECT DISTINCT %s AS grp, %s AS d
                FROM %s
                WHERE %s
            ),
            params AS (
                SELECT min(d) AS min_d,
                       dateDiff('day', min(d), max(d)) AS diff_d
                FROM source
                HAVING count() > 0
            ),
            counts AS (
                SELECT toDate(d) AS d, count() AS c
                FROM source
                GROUP BY d
            ),
            timeline AS (
                SELECT addDays(p.min_d, n) AS d
                FROM params p
                ARRAY JOIN range(p.diff_d + 1) AS n
            )
            SELECT toString(t.d) AS label, ifNull(c.c, 0) AS c
            FROM timeline t
            LEFT JOIN counts c ON c.d = t.d
            ORDER BY label
        `, groupExpr, p.PubDayDate, sourceFrom, whereExpr)
	default:
		panic("unsupported granularity: " + granularity)
	}
}

func querySeriesBatch(client *ch.Client, tasks map[string]string) (map[string]Series, error) {
	keys := make([]string, 0, len(tasks))
	for key := range tasks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	results := make(map[string]Series, len(tasks))
	for _, key := range keys {
		series, err := querySeries(client, tasks[key])
		if err != nil {
			return nil, fmt.Errorf("query %s failed: %w", key, err)
		}
		results[key] = series
	}
	return results, nil
}

func querySeriesBatchWarn(client *ch.Client, tasks map[string]string) map[string]Series {
	keys := make([]string, 0, len(tasks))
	for key := range tasks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	results := make(map[string]Series, len(tasks))
	for _, key := range keys {
		series, err := querySeries(client, tasks[key])
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] stats query failed: %s err=%v\n", key, err)
			results[key] = Series{}
			continue
		}
		results[key] = series
	}
	return results
}

func Generate(client *ch.Client, table, outDir string) error {
	if table == "" {
		table = "raw_naver"
	}
	if outDir == "" {
		outDir = "stats"
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	dataUpdatedAt, err := queryString(client, fmt.Sprintf(`
        SELECT ifNull(formatDateTime(max(updated_at), '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul'), '') AS value
        FROM %s
    `, table))
	if err != nil {
		return err
	}

	p := buildPubDateParts()
	totalBooks, err := queryInt(client, fmt.Sprintf(`SELECT uniqExact(isbn) AS value FROM %s WHERE %s`, table, p.BaseIsbn))
	if err != nil {
		return err
	}
	totalPublishers, err := queryInt(client, fmt.Sprintf(`SELECT countDistinct(publisher) AS value FROM %s WHERE %s`, table, p.BasePub))
	if err != nil {
		return err
	}
	totalAuthors, err := queryInt(client, fmt.Sprintf(`
        SELECT countDistinct(author_one) AS value
        FROM (
            SELECT trim(author_one) AS author_one
            FROM %s
            ARRAY JOIN splitByChar('^', ifNull(author, '')) AS author_one
            WHERE length(trim(author_one)) > 0
        )
    `, table))
	if err != nil {
		return err
	}

	inflowTasks := map[string]string{
		"y_books": buildFirstSeenSeriesSQL(table, "books", "year"),
		"m_books": buildFirstSeenSeriesSQL(table, "books", "month"),
		"d_books": buildFirstSeenSeriesSQL(table, "books", "day"),
		"h_books": buildFirstSeenSeriesSQL(table, "books", "hour"),
		"y_pubs":  buildFirstSeenSeriesSQL(table, "publishers", "year"),
		"m_pubs":  buildFirstSeenSeriesSQL(table, "publishers", "month"),
		"d_pubs":  buildFirstSeenSeriesSQL(table, "publishers", "day"),
		"h_pubs":  buildFirstSeenSeriesSQL(table, "publishers", "hour"),
		"y_auth":  buildFirstSeenSeriesSQL(table, "authors", "year"),
		"m_auth":  buildFirstSeenSeriesSQL(table, "authors", "month"),
		"d_auth":  buildFirstSeenSeriesSQL(table, "authors", "day"),
		"h_auth":  buildFirstSeenSeriesSQL(table, "authors", "hour"),
	}
	inflow, err := querySeriesBatch(client, inflowTasks)
	if err != nil {
		return err
	}

	totalPubYear, err := queryInt(client, fmt.Sprintf(`SELECT uniqExact(isbn) AS value FROM %s WHERE %s AND %s`, table, p.BaseIsbn, p.PubValidYear))
	if err != nil {
		return err
	}
	totalPubYearOnly, err := queryInt(client, fmt.Sprintf(`SELECT uniqExact(isbn) AS value FROM %s WHERE %s AND %s AND %s = 4`, table, p.BaseIsbn, p.PubValidYear, p.PubLenExpr))
	if err != nil {
		return err
	}
	totalPubYM, err := queryInt(client, fmt.Sprintf(`SELECT uniqExact(isbn) AS value FROM %s WHERE %s AND %s AND %s = 6`, table, p.BaseIsbn, p.PubValidYear, p.PubLenExpr))
	if err != nil {
		return err
	}
	totalPubYMD, err := queryInt(client, fmt.Sprintf(`SELECT uniqExact(isbn) AS value FROM %s WHERE %s AND %s AND %s >= 8`, table, p.BaseIsbn, p.PubValidYear, p.PubLenExpr))
	if err != nil {
		return err
	}
	totalPubMissing, err := queryInt(client, fmt.Sprintf(`SELECT uniqExact(isbn) AS value FROM %s WHERE %s AND NOT (%s)`, table, p.BaseIsbn, p.PubValidYear))
	if err != nil {
		return err
	}

	pubTasks := map[string]string{
		"y_pub_books": buildPubDateSeriesSQL(table, "books", "year", p),
		"y_pub_pubs":  buildPubDateSeriesSQL(table, "publishers", "year", p),
		"y_pub_auth":  buildPubDateSeriesSQL(table, "authors", "year", p),
		"m_pub_books": buildPubDateSeriesSQL(table, "books", "month", p),
		"m_pub_pubs":  buildPubDateSeriesSQL(table, "publishers", "month", p),
		"m_pub_auth":  buildPubDateSeriesSQL(table, "authors", "month", p),
		"d_pub_books": buildPubDateSeriesSQL(table, "books", "day", p),
		"d_pub_pubs":  buildPubDateSeriesSQL(table, "publishers", "day", p),
		"d_pub_auth":  buildPubDateSeriesSQL(table, "authors", "day", p),
	}
	pubResults := querySeriesBatchWarn(client, pubTasks)

	unknownMonthBooks, _ := querySeries(client, fmt.Sprintf(`
        SELECT toString(%s) AS label, uniqExact(isbn) AS c
        FROM %s
        WHERE %s AND %s AND %s = 4
        GROUP BY label
        ORDER BY label
    `, p.PubYearExpr, table, p.BaseIsbn, p.PubValidYear, p.PubLenExpr))
	unknownDayBooks, _ := querySeries(client, fmt.Sprintf(`
        SELECT concat(toString(%s), '-', lpad(toString(%s), 2, '0')) AS label,
               uniqExact(isbn) AS c
        FROM %s
        WHERE %s AND %s AND %s = 6
        GROUP BY label
        ORDER BY label
    `, p.PubYearExpr, p.PubMonthExpr, table, p.BaseIsbn, p.PubValidMonth, p.PubLenExpr))

	colorBooks := charts.MustHex(ColorBooks)
	colorAuthors := charts.MustHex(ColorAuthors)
	colorPublishers := charts.MustHex(ColorPublishers)

	if err := charts.PlotTotals("RAW NAVER TOTALS", []string{"BOOKS", "AUTHORS", "PUBLISHERS"}, []int64{totalBooks, totalAuthors, totalPublishers}, filepath.Join(outDir, "raw_naver_totals.png"), []charts.Color{colorBooks, colorAuthors, colorPublishers}); err != nil {
		return err
	}

	maybePlot := func(series Series, title, filename, hex string) error {
		if len(series.Values) == 0 {
			return nil
		}
		return charts.PlotSeriesWithCumulative(title, series.Labels, series.Values, filepath.Join(outDir, filename), charts.MustHex(hex), 24)
	}

	if err := maybePlot(inflow["y_books"], "YEARLY (BOOKS: NEW + CUMULATIVE)", "raw_naver_by_year.png", ColorBooks); err != nil {
		return err
	}
	if err := maybePlot(inflow["y_auth"], "YEARLY (AUTHORS: NEW + CUMULATIVE)", "raw_naver_by_year_authors.png", ColorAuthors); err != nil {
		return err
	}
	if err := maybePlot(inflow["y_pubs"], "YEARLY (PUBLISHERS: NEW + CUMULATIVE)", "raw_naver_by_year_publishers.png", ColorPublishers); err != nil {
		return err
	}

	if err := maybePlot(inflow["m_books"], "MONTHLY (BOOKS: NEW + CUMULATIVE)", "raw_naver_by_month.png", ColorBooks); err != nil {
		return err
	}
	if err := maybePlot(inflow["m_auth"], "MONTHLY (AUTHORS: NEW + CUMULATIVE)", "raw_naver_by_month_authors.png", ColorAuthors); err != nil {
		return err
	}
	if err := maybePlot(inflow["m_pubs"], "MONTHLY (PUBLISHERS: NEW + CUMULATIVE)", "raw_naver_by_month_publishers.png", ColorPublishers); err != nil {
		return err
	}

	if err := maybePlot(inflow["d_books"], "DAILY (BOOKS: NEW + CUMULATIVE)", "raw_naver_by_day.png", ColorBooks); err != nil {
		return err
	}
	if err := maybePlot(inflow["d_auth"], "DAILY (AUTHORS: NEW + CUMULATIVE)", "raw_naver_by_day_authors.png", ColorAuthors); err != nil {
		return err
	}
	if err := maybePlot(inflow["d_pubs"], "DAILY (PUBLISHERS: NEW + CUMULATIVE)", "raw_naver_by_day_publishers.png", ColorPublishers); err != nil {
		return err
	}

	if err := maybePlot(inflow["h_books"], "HOURLY (BOOKS: NEW + CUMULATIVE)", "raw_naver_by_hour.png", ColorBooks); err != nil {
		return err
	}
	if err := maybePlot(inflow["h_auth"], "HOURLY (AUTHORS: NEW + CUMULATIVE)", "raw_naver_by_hour_authors.png", ColorAuthors); err != nil {
		return err
	}
	if err := maybePlot(inflow["h_pubs"], "HOURLY (PUBLISHERS: NEW + CUMULATIVE)", "raw_naver_by_hour_publishers.png", ColorPublishers); err != nil {
		return err
	}

	if err := maybePlot(pubResults["y_pub_books"], "YEARLY (BOOKS BY PUBLISHED DATE)", "raw_naver_pub_by_year_books.png", ColorBooks); err != nil {
		return err
	}
	if err := maybePlot(pubResults["m_pub_books"], "MONTHLY (BOOKS BY PUBLISHED DATE)", "raw_naver_pub_by_month_books.png", ColorBooks); err != nil {
		return err
	}
	if err := maybePlot(pubResults["d_pub_books"], "DAILY (BOOKS BY PUBLISHED DATE)", "raw_naver_pub_by_day_books.png", ColorBooks); err != nil {
		return err
	}

	if err := maybePlot(pubResults["y_pub_pubs"], "YEARLY (PUBLISHERS BY PUBLISHED DATE)", "raw_naver_pub_by_year_publishers.png", ColorPublishers); err != nil {
		return err
	}
	if err := maybePlot(pubResults["m_pub_pubs"], "MONTHLY (PUBLISHERS BY PUBLISHED DATE)", "raw_naver_pub_by_month_publishers.png", ColorPublishers); err != nil {
		return err
	}
	if err := maybePlot(pubResults["d_pub_pubs"], "DAILY (PUBLISHERS BY PUBLISHED DATE)", "raw_naver_pub_by_day_publishers.png", ColorPublishers); err != nil {
		return err
	}

	if err := maybePlot(pubResults["y_pub_auth"], "YEARLY (AUTHORS BY PUBLISHED DATE)", "raw_naver_pub_by_year_authors.png", ColorAuthors); err != nil {
		return err
	}
	if err := maybePlot(pubResults["m_pub_auth"], "MONTHLY (AUTHORS BY PUBLISHED DATE)", "raw_naver_pub_by_month_authors.png", ColorAuthors); err != nil {
		return err
	}
	if err := maybePlot(pubResults["d_pub_auth"], "DAILY (AUTHORS BY PUBLISHED DATE)", "raw_naver_pub_by_day_authors.png", ColorAuthors); err != nil {
		return err
	}

	lines := make([]string, 0, 256)
	lines = append(lines, "# 수집 데이터 집계", "")
	if dataUpdatedAt != "" {
		lines = append(lines, fmt.Sprintf("- 데이터 기준 최종 수정 시각(KST): %s", dataUpdatedAt))
	} else {
		lines = append(lines, "- 데이터 기준 최종 수정 시각(KST): N/A")
	}
	lines = append(lines, "", "## 전체", "")
	lines = append(lines, fmt.Sprintf("- 총 고유 ISBN 수: **%s**", fmtCount(totalBooks)))
	lines = append(lines, fmt.Sprintf("- 저자 수: **%s**", fmtCount(totalAuthors)))
	lines = append(lines, fmt.Sprintf("- 출판사 수: **%s**", fmtCount(totalPublishers)))
	lines = append(lines, "", "![Totals](raw_naver_totals.png)", "")

	lines = append(lines, "## 출간일(pubdate) 기준 통계", "")
	lines = append(lines, fmt.Sprintf("- 출간연도(YYYY 이상) 파싱 가능 ISBN: **%s**", fmtCount(totalPubYear)))
	lines = append(lines, fmt.Sprintf("  - 연도만(YYYY): **%s**", fmtCount(totalPubYearOnly)))
	lines = append(lines, fmt.Sprintf("  - 연/월(YYYYMM): **%s**", fmtCount(totalPubYM)))
	lines = append(lines, fmt.Sprintf("  - 연/월/일(YYYYMMDD+): **%s**", fmtCount(totalPubYMD)))
	lines = append(lines, fmt.Sprintf("- 출간일 파싱 불가/없음 ISBN: **%s**", fmtCount(totalPubMissing)), "")
	if len(pubResults["y_pub_books"].Values) > 0 {
		lines = append(lines, "### Books (Published Date)", "![Books Published Year](raw_naver_pub_by_year_books.png)", "")
	}
	if len(pubResults["m_pub_books"].Values) > 0 {
		lines = append(lines, "![Books Published Month](raw_naver_pub_by_month_books.png)", "")
	}
	if len(pubResults["d_pub_books"].Values) > 0 {
		lines = append(lines, "![Books Published Day](raw_naver_pub_by_day_books.png)", "")
	}
	if len(unknownMonthBooks.Values) > 0 {
		lines = append(lines, "### Books (Published Date) - UNKNOWN month (year-only)", "", "| Year | ISBN Count |", "|---:|---:|")
		for i, label := range unknownMonthBooks.Labels {
			lines = append(lines, fmt.Sprintf("| %s | %s |", label, fmtCount(unknownMonthBooks.Values[i])))
		}
		lines = append(lines, "")
	}
	if len(unknownDayBooks.Values) > 0 {
		lines = append(lines, "### Books (Published Date) - UNKNOWN day (year-month only)", "", "| Year-Month | ISBN Count |", "|---:|---:|")
		for i, label := range unknownDayBooks.Labels {
			lines = append(lines, fmt.Sprintf("| %s | %s |", label, fmtCount(unknownDayBooks.Values[i])))
		}
		lines = append(lines, "")
	}
	lines = append(lines,
		"<details>",
		"<summary>📚 Published Date Details (Authors/Publishers)</summary>",
		"",
		"### Authors",
		"![Authors Published Year](raw_naver_pub_by_year_authors.png)",
		"",
		"![Authors Published Month](raw_naver_pub_by_month_authors.png)",
		"",
		"![Authors Published Day](raw_naver_pub_by_day_authors.png)",
		"",
		"### Publishers",
		"![Publishers Published Year](raw_naver_pub_by_year_publishers.png)",
		"",
		"![Publishers Published Month](raw_naver_pub_by_month_publishers.png)",
		"",
		"![Publishers Published Day](raw_naver_pub_by_day_publishers.png)",
		"",
		"</details>",
		"",
		"## 📊 Monthly Overview (New + Cumulative)",
		"",
		"### Books",
		"![Books Month](raw_naver_by_month.png)",
		"",
		"### Authors",
		"![Authors Month](raw_naver_by_month_authors.png)",
		"",
		"### Publishers",
		"![Publishers Month](raw_naver_by_month_publishers.png)",
		"",
		"<details>",
		"<summary>📅 Yearly Details</summary>",
		"",
		"### Books",
		"![Books Year](raw_naver_by_year.png)",
		"",
		"### Authors",
		"![Authors Year](raw_naver_by_year_authors.png)",
		"",
		"### Publishers",
		"![Publishers Year](raw_naver_by_year_publishers.png)",
		"",
		"</details>",
		"",
		"<details>",
		"<summary>📆 Daily Details</summary>",
		"",
		"### Books",
		"![Books Day](raw_naver_by_day.png)",
		"",
		"### Authors",
		"![Authors Day](raw_naver_by_day_authors.png)",
		"",
		"### Publishers",
		"![Publishers Day](raw_naver_by_day_publishers.png)",
		"",
		"</details>",
		"",
		"<details>",
		"<summary>⏱ Hourly Details</summary>",
		"",
		"### Books",
		"![Books Hour](raw_naver_by_hour.png)",
		"",
		"### Authors",
		"![Authors Hour](raw_naver_by_hour_authors.png)",
		"",
		"### Publishers",
		"![Publishers Hour](raw_naver_by_hour_publishers.png)",
		"",
		"</details>",
		"",
		"> 시계열은 각 항목(ISBN/저자/출판사)의 '최초 등장 시각' 기준 신규 유입을 집계합니다. (빈 구간은 0으로 채움)",
		"> 모든 시계열 차트는 **신규 유입(막대) + 누적(선)** 을 함께 표시합니다.",
		"",
	)

	mdPath := filepath.Join(outDir, "raw_naver_stats.md")
	return os.WriteFile(mdPath, []byte(strings.Join(lines, "\n")), 0o644)

}
