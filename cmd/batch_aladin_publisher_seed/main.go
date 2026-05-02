package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"statground_naver_book_go/internal/aladin"
	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/collector"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/kafkaingest"
	"statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/util"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func loadCachedPublishersIfFresh(client *ch.Client, cacheTable string, currentLastPage int) ([]string, error) {
	if client == nil || strings.TrimSpace(cacheTable) == "" || currentLastPage <= 0 {
		return nil, nil
	}
	row, err := client.QuerySingleRow(fmt.Sprintf(`
        SELECT ifNull(max(detected_last_page), 0) AS last_page
        FROM %s
    `, cacheTable))
	if err != nil {
		fmt.Printf("[CACHE] load skipped table=%s error=%v\n", cacheTable, err)
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
		fmt.Printf("[CACHE] publisher query skipped table=%s error=%v\n", cacheTable, err)
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

func publishPublishersCache(pub *kafkaingest.Publisher, sourceURL string, publishers []string, detectedLastPage int, runUUID string) error {
	if pub == nil || len(publishers) == 0 || detectedLastPage <= 0 {
		return nil
	}
	nowStr := util.FormatCHDateTime64Millis(util.NowKST())
	const chunkSize = 1000
	events := make([]kafkaingest.Event, 0, chunkSize)
	flush := func() error {
		if len(events) == 0 {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := pub.Publish(ctx, events); err != nil {
			return err
		}
		events = events[:0]
		return nil
	}

	seen := map[string]struct{}{}
	for _, publisher := range publishers {
		publisher = strings.TrimSpace(publisher)
		if publisher == "" {
			continue
		}
		if _, ok := seen[publisher]; ok {
			continue
		}
		seen[publisher] = struct{}{}
		eventURL := sourceURL
		if strings.Contains(eventURL, "?") {
			eventURL += "&publisher=" + url.QueryEscape(publisher)
		} else {
			eventURL += "?publisher=" + url.QueryEscape(publisher)
		}
		payload := map[string]any{
			"publisher":          publisher,
			"collected_at":       nowStr,
			"detected_last_page": detectedLastPage,
			"run_uuid":           runUUID,
			"source":             "aladin",
		}
		ev, err := pub.NewEvent("book.aladin.publisher_cache.v1", "", eventURL, nowStr, payload)
		if err != nil {
			return err
		}
		events = append(events, ev)
		if len(events) >= chunkSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	fmt.Printf("[CACHE] published aladin publisher cache events=%d last_page=%d topic=%s\n", len(seen), detectedLastPage, pub.Cfg.KafkaTopic)
	return nil
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
	client, err := ch.NewOptionalFromEnv()
	if err != nil {
		return err
	}
	keys, err := naver.LoadAPIKeysFromEnv()
	if err != nil {
		return err
	}

	rawNaverTable := envx.String("RAW_NAVER_TABLE", "naver_book_raw")
	aladinURL := envx.String("ALADIN_PUBLISHER_LIST_URL", "https://www.aladin.co.kr/aladdin/PublisherList.aspx")
	aladinMaxWorkers := envx.Int("ALADIN_MAX_WORKERS", 6)
	aladinSleepMin := envx.Float("ALADIN_SLEEP_MIN", 0.05)
	aladinSleepMax := envx.Float("ALADIN_SLEEP_MAX", 0.20)
	cacheTable := envx.String("ALADIN_CACHE_TABLE", "Data_Book_NAVER_Log.aladin_publisher_cache")
	publisherSampleN := envx.Int("PUBLISHER_SAMPLE_N", 100)
	display := envx.Int("NAVER_DISPLAY", 100)
	naverMaxWorkers := envx.Int("NAVER_MAX_WORKERS", 10)
	naverSleepMin := envx.Float("NAVER_SLEEP_MIN", 0.05)
	naverSleepMax := envx.Float("NAVER_SLEEP_MAX", 0.20)
	reqsPerTerm := envx.Int("REQS_PER_TERM", 1)

	baseCollector, err := collector.New(client, rawNaverTable, keys, time.Now().UnixNano())
	if err != nil {
		return err
	}
	if err := baseCollector.ValidateIngest(context.Background()); err != nil {
		return err
	}

	runUUID := util.UUIDv7()
	_, currentLastPage, err := aladin.DetectCntAndLastPage(aladinURL)
	if err != nil {
		return err
	}

	publishers, err := loadCachedPublishersIfFresh(client, cacheTable, currentLastPage)
	if err != nil {
		return err
	}
	if len(publishers) > 0 {
		fmt.Printf("[CACHE] use cached publishers table=%s last_page=%d publishers=%d\n", cacheTable, currentLastPage, len(publishers))
	} else {
		rr := rand.New(rand.NewSource(time.Now().UnixNano()))
		publishers, _, err = aladin.CrawlPublishersDynamic(aladinURL, aladinMaxWorkers, aladinSleepMin, aladinSleepMax, rr)
		if err != nil {
			return err
		}
		cleaned := make([]string, 0, len(publishers))
		seen := map[string]struct{}{}
		for _, publisher := range publishers {
			publisher = strings.TrimSpace(publisher)
			if publisher == "" {
				continue
			}
			if _, ok := seen[publisher]; ok {
				continue
			}
			seen[publisher] = struct{}{}
			cleaned = append(cleaned, publisher)
		}
		publishers = cleaned
		fmt.Printf("[ALADIN] crawled publishers=%d last_page=%d\n", len(publishers), currentLastPage)
		if err := publishPublishersCache(baseCollector.Publisher, aladinURL, publishers, currentLastPage, runUUID); err != nil {
			return err
		}
	}
	if len(publishers) == 0 {
		return fmt.Errorf("no publishers collected from Aladin")
	}

	sampled := samplePublishers(publishers, publisherSampleN, rand.New(rand.NewSource(time.Now().UnixNano())))
	fmt.Printf("[SAMPLE] picked %d publishers\n", len(sampled))

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
