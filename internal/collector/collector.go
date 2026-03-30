package collector

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/rawnaver"
	"statground_naver_book_go/internal/terms"
	"statground_naver_book_go/internal/util"
)

type Collector struct {
	Client  *ch.Client
	Table   string
	Columns map[string]bool
	Keys    []naver.APIKey
	Rand    *rand.Rand
}

func New(client *ch.Client, table string, keys []naver.APIKey, seed int64) (*Collector, error) {
	if table == "" {
		table = "raw_naver"
	}
	columns, err := client.QueryColumnNames(table)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("cannot read columns for %s.%s", client.Database, table)
	}
	return &Collector{
		Client:  client,
		Table:   table,
		Columns: columns,
		Keys:    keys,
		Rand:    rand.New(rand.NewSource(seed)),
	}, nil
}

func (c *Collector) SampleRows(limit int) ([]map[string]any, error) {
	return rawnaver.SampleTitleAuthorPublisher(c.Client, c.Table, limit)
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
		_, items, err := naver.FetchItems(term, sort, 1, display, c.Keys, c.Rand)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			continue
		}
		if err := c.upsertItems(items, fmt.Sprintf("auto_upsert|mode=%s|term=%s|sort=%s", mode, term, sort), "github_actions_auto", false); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) CollectManual(keyword string) error {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return fmt.Errorf("MANUAL_KEYWORD is empty")
	}
	for _, sort := range []string{"sim", "date"} {
		for start := 1; start <= 1000; start += 100 {
			_, items, err := naver.FetchItems(keyword, sort, start, 100, c.Keys, c.Rand)
			if err != nil {
				return err
			}
			if len(items) == 0 {
				break
			}
			if err := c.upsertItems(items, fmt.Sprintf("manual_upsert|keyword=%s|sort=%s", keyword, sort), "github_actions_manual", true); err != nil {
				return err
			}
			if len(items) < 100 {
				break
			}
		}
	}
	return nil
}

func (c *Collector) CollectPublisherAllPages(publisher string, reqsPerTerm, display int, sleepMin, sleepMax float64) error {
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
			if err != nil {
				return err
			}
			if total == 0 {
				total = t
			}
			if len(items) == 0 {
				break
			}
			if err := c.upsertItems(items, fmt.Sprintf("aladin_publisher_seed|publisher=%s|sort=%s|start=%d", publisher, sort, start), "github_actions_auto", false); err != nil {
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

func (c *Collector) upsertItems(items []naver.BookItem, updatedLog, defaultCreatedLog string, preserveCreated bool) error {
	now := util.NowKST()
	nowStr := util.FormatCHDateTime64Millis(now)
	version := now.Unix()

	isbns := make([]string, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.ISBN) != "" {
			isbns = append(isbns, item.ISBN)
		}
	}

	existingMap, err := rawnaver.BuildExistingMap(c.Client, c.Table, c.Columns, isbns)
	if err != nil {
		return err
	}
	uuidMap := map[string]string{}
	if preserveCreated {
		uuidMap, err = rawnaver.BuildUUIDMap(c.Client, c.Table, isbns)
		if err != nil {
			return err
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
		row := rawnaver.BuildRawNaverRow(item, nowStr, version, createdAt, createdLog, updatedLog, uuid)
		rows = append(rows, rawnaver.FilterRowByColumns(row, c.Columns))
	}
	if len(rows) == 0 {
		return nil
	}
	return c.Client.InsertJSONEachRow(c.Table, rows)
}
