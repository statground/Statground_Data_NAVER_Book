package main

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"statground_naver_book_go/internal/aladin"
	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/collector"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/util"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func ensureAladinCacheTable(client *ch.Client, table string) error {
	ddl := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s
        (
          publisher String COMMENT '알라딘 출판사명(원문)',
          collected_at DateTime64(3, 'Asia/Seoul') COMMENT '수집 시각 (Asia/Seoul)',
          detected_last_page UInt32 COMMENT '수집 당시 알라딘 최대 페이지',
          run_uuid UUID COMMENT '배치 실행 UUID v7 (OLAP 전용, SSOT 아님)',
          source LowCardinality(String) COMMENT '수집 출처 (aladin)'
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(collected_at)
        ORDER BY (collected_at, detected_last_page, publisher)
        COMMENT '알라딘 출판사 목록 캐시/로그 (OLAP 전용, SSOT 아님). 배치 효율 목적 캐시';
    `, table)
	return client.Exec(ddl)
}

func loadCachedPublishersIfFresh(client *ch.Client, cacheTable string, currentLastPage int) ([]string, error) {
	row, err := client.QuerySingleRow(fmt.Sprintf(`
        SELECT ifNull(max(detected_last_page), 0) AS last_page
        FROM %s
    `, cacheTable))
	if err != nil {
		return nil, nil
	}
	cachedLastPage := int(util.ToInt64(row["last_page"]))
	if cachedLastPage != currentLastPage {
		return nil, nil
	}
	rows, err := client.QueryJSONEachRow(fmt.Sprintf(`
        SELECT DISTINCT publisher
        FROM %s
        WHERE detected_last_page = %d
    `, cacheTable, currentLastPage))
	if err != nil {
		return nil, nil
	}
	pubs := make([]string, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		pub := strings.TrimSpace(util.ToString(row["publisher"]))
		if pub == "" {
			continue
		}
		if _, ok := seen[pub]; ok {
			continue
		}
		seen[pub] = struct{}{}
		pubs = append(pubs, pub)
	}
	sort.Strings(pubs)
	return pubs, nil
}

func savePublishersCache(client *ch.Client, cacheTable string, publishers []string, detectedLastPage int, runUUID string) error {
	now := util.FormatCHDateTime64Millis(util.NowKST())
	rows := make([]map[string]any, 0, len(publishers))
	for _, publisher := range publishers {
		publisher = strings.TrimSpace(publisher)
		if publisher == "" {
			continue
		}
		rows = append(rows, map[string]any{
			"publisher":          publisher,
			"collected_at":       now,
			"detected_last_page": detectedLastPage,
			"run_uuid":           runUUID,
			"source":             "aladin",
		})
	}
	return client.InsertJSONEachRow(cacheTable, rows)
}

func samplePublishers(items []string, n int, r *rand.Rand) []string {
	if n <= 0 || len(items) == 0 {
		return nil
	}
	copyItems := make([]string, len(items))
	copy(copyItems, items)
	if r == nil {
		rand.Shuffle(len(copyItems), func(i, j int) { copyItems[i], copyItems[j] = copyItems[j], copyItems[i] })
	} else {
		r.Shuffle(len(copyItems), func(i, j int) { copyItems[i], copyItems[j] = copyItems[j], copyItems[i] })
	}
	if n > len(copyItems) {
		n = len(copyItems)
	}
	return copyItems[:n]
}

func run() error {
	client, err := ch.NewFromEnv()
	if err != nil {
		return err
	}
	keys, err := naver.LoadAPIKeysFromEnv()
	if err != nil {
		return err
	}

	rawNaverTable := envx.String("RAW_NAVER_TABLE", "raw_naver")
	aladinURL := envx.String("ALADIN_PUBLISHER_LIST_URL", "https://www.aladin.co.kr/aladdin/PublisherList.aspx")
	aladinMaxWorkers := envx.Int("ALADIN_MAX_WORKERS", 6)
	aladinSleepMin := envx.Float("ALADIN_SLEEP_MIN", 0.05)
	aladinSleepMax := envx.Float("ALADIN_SLEEP_MAX", 0.20)
	cacheTable := envx.String("ALADIN_CACHE_TABLE", "raw_aladin_publisher_cache")
	publisherSampleN := envx.Int("PUBLISHER_SAMPLE_N", 100)
	display := envx.Int("NAVER_DISPLAY", 100)
	naverMaxWorkers := envx.Int("NAVER_MAX_WORKERS", 10)
	naverSleepMin := envx.Float("NAVER_SLEEP_MIN", 0.05)
	naverSleepMax := envx.Float("NAVER_SLEEP_MAX", 0.20)
	reqsPerTerm := envx.Int("REQS_PER_TERM", 1)

	runUUID := util.UUIDv7()
	if err := ensureAladinCacheTable(client, cacheTable); err != nil {
		return err
	}

	_, currentLastPage, err := aladin.DetectCntAndLastPage(aladinURL)
	if err != nil {
		return err
	}

	publishers, err := loadCachedPublishersIfFresh(client, cacheTable, currentLastPage)
	if err != nil {
		return err
	}
	if len(publishers) > 0 {
		fmt.Printf("[CACHE] use cached publishers for last_page=%d: %d\n", currentLastPage, len(publishers))
	} else {
		rr := rand.New(rand.NewSource(time.Now().UnixNano()))
		publishers, _, err = aladin.CrawlPublishersDynamic(aladinURL, aladinMaxWorkers, aladinSleepMin, aladinSleepMax, rr)
		if err != nil {
			return err
		}
		if err := savePublishersCache(client, cacheTable, publishers, currentLastPage, runUUID); err != nil {
			return err
		}
		fmt.Printf("[CACHE] saved publishers: %d (last_page=%d)\n", len(publishers), currentLastPage)
	}
	if len(publishers) == 0 {
		return fmt.Errorf("no publishers collected from Aladin")
	}

	sampled := samplePublishers(publishers, publisherSampleN, rand.New(rand.NewSource(time.Now().UnixNano())))
	fmt.Printf("[SAMPLE] picked %d publishers\n", len(sampled))

	baseCollector, err := collector.New(client, rawNaverTable, keys, time.Now().UnixNano())
	if err != nil {
		return err
	}
	_ = baseCollector

	if naverMaxWorkers <= 0 {
		naverMaxWorkers = 1
	}
	jobs := make(chan string)
	errs := make(chan error, len(sampled))
	var wg sync.WaitGroup
	progress := struct {
		sync.Mutex
		done int
	}{}

	for i := 0; i < naverMaxWorkers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			coll, err := collector.New(client, rawNaverTable, keys, time.Now().UnixNano()+int64(worker+1))
			if err != nil {
				errs <- err
				return
			}
			for publisher := range jobs {
				if err := coll.CollectPublisherAllPages(publisher, reqsPerTerm, display, naverSleepMin, naverSleepMax); err != nil {
					errs <- err
					continue
				}
				progress.Lock()
				progress.done++
				done := progress.done
				progress.Unlock()
				if done%10 == 0 {
					fmt.Printf("[DONE] completed publishers: %d/%d\n", done, len(sampled))
				}
			}
		}(i)
	}

	for _, publisher := range sampled {
		jobs <- publisher
	}
	close(jobs)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}
	fmt.Printf("[FINISH] completed publishers: %d/%d\n", progress.done, len(sampled))
	return nil
}
