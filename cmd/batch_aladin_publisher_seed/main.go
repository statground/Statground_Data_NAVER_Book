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
	"statground_naver_book_go/internal/terms"
	"statground_naver_book_go/internal/util"
)

type publisherCacheSnapshot struct {
	RunUUID  string
	LastPage int
}

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
	snapshot, err := latestPublisherCacheSnapshot(client, cacheTable)
	if err != nil {
		fmt.Printf("[CACHE] load skipped table=%s error=%s\n", cacheTable, shortOperationalError(err))
		return nil, nil
	}
	if snapshot.LastPage != currentLastPage {
		return nil, nil
	}
	return loadCachedPublishersForSnapshot(client, cacheTable, snapshot)
}

func loadCachedPublishersLatest(client *ch.Client, cacheTable string) ([]string, int, error) {
	if client == nil || strings.TrimSpace(cacheTable) == "" {
		return nil, 0, nil
	}
	snapshot, err := latestPublisherCacheSnapshot(client, cacheTable)
	if err != nil {
		return nil, 0, err
	}
	if snapshot.LastPage <= 0 {
		return nil, 0, nil
	}
	publishers, err := loadCachedPublishersForSnapshot(client, cacheTable, snapshot)
	return publishers, snapshot.LastPage, err
}

func latestPublisherCacheSnapshot(client *ch.Client, cacheTable string) (publisherCacheSnapshot, error) {
	row, err := client.QuerySingleRow(fmt.Sprintf(`
        SELECT toString(run_uuid) AS run_uuid, detected_last_page
        FROM %s
        ORDER BY collected_at DESC
        LIMIT 1
        SETTINGS max_threads = 1
    `, cacheTable))
	if err != nil {
		return publisherCacheSnapshot{}, err
	}
	return publisherCacheSnapshot{
		RunUUID:  strings.TrimSpace(util.ToString(row["run_uuid"])),
		LastPage: int(util.ToInt64(row["detected_last_page"])),
	}, nil
}

func loadCachedPublishersForSnapshot(client *ch.Client, cacheTable string, snapshot publisherCacheSnapshot) ([]string, error) {
	if client == nil || strings.TrimSpace(cacheTable) == "" || snapshot.LastPage <= 0 || strings.TrimSpace(snapshot.RunUUID) == "" {
		return nil, nil
	}
	rows, err := client.QueryJSONEachRow(fmt.Sprintf(`
        SELECT DISTINCT publisher
        FROM %s
        WHERE run_uuid = toUUID(%s)
          AND detected_last_page = %d
        ORDER BY publisher
        SETTINGS max_threads = 1
    `, cacheTable, util.SQLString(snapshot.RunUUID), snapshot.LastPage))
	if err != nil {
		fmt.Printf("[CACHE] publisher query skipped table=%s error=%s\n", cacheTable, shortOperationalError(err))
		return nil, nil
	}
	pubs := make([]string, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		pub := terms.NormalizePublisher(util.ToString(row["publisher"]))
		if !terms.IsPublisherSearchCandidate(pub) {
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

func aladinPublisherSeedRequired() bool {
	return boolEnv("ALADIN_PUBLISHER_SEED_REQUIRED", false)
}

func aladinPublisherCacheRequired() bool {
	return boolEnv("ALADIN_PUBLISHER_CACHE_REQUIRED", false)
}

func boolEnv(name string, fallback bool) bool {
	defaultValue := "false"
	if fallback {
		defaultValue = "true"
	}
	switch strings.ToLower(strings.TrimSpace(envx.String(name, defaultValue))) {
	case "1", "true", "yes", "y", "required":
		return true
	case "0", "false", "no", "n", "optional":
		return false
	default:
		return fallback
	}
}

func shouldSkipAladinPublisherSeed(err error) bool {
	return err != nil && !aladinPublisherSeedRequired() && isTemporaryAladinError(err)
}

func isTemporaryAladinError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "aladin http 429") ||
		strings.Contains(msg, "aladin http 500") ||
		strings.Contains(msg, "aladin http 502") ||
		strings.Contains(msg, "aladin http 503") ||
		strings.Contains(msg, "aladin http 504") ||
		strings.Contains(msg, "service unavailable") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof")
}

func fallbackCachedPublishers(client *ch.Client, cacheTable string) ([]string, int) {
	publishers, lastPage, err := loadCachedPublishersLatest(client, cacheTable)
	if err != nil {
		fmt.Printf("[CACHE] latest fallback skipped table=%s error=%s\n", cacheTable, shortOperationalError(err))
		return nil, 0
	}
	if len(publishers) > 0 {
		fmt.Printf("[CACHE] use latest cached publishers table=%s last_page=%d publishers=%d\n", cacheTable, lastPage, len(publishers))
	}
	return publishers, lastPage
}

func shortOperationalError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "access_denied") || strings.Contains(msg, "not enough privileges"):
		return "access_denied"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "context deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "eof"):
		return "eof"
	default:
		out := strings.Join(strings.Fields(err.Error()), " ")
		const maxLen = 220
		if len(out) > maxLen {
			out = strings.TrimSpace(out[:maxLen]) + "..."
		}
		return out
	}
}

func cleanPublisherSeeds(publishers []string) ([]string, int) {
	cleaned := make([]string, 0, len(publishers))
	seen := map[string]struct{}{}
	skipped := 0
	for _, publisher := range publishers {
		publisher = terms.NormalizePublisher(publisher)
		if !terms.IsPublisherSearchCandidate(publisher) {
			skipped++
			continue
		}
		if _, ok := seen[publisher]; ok {
			skipped++
			continue
		}
		seen[publisher] = struct{}{}
		cleaned = append(cleaned, publisher)
	}
	return cleaned, skipped
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
		timeout := envx.Int("ALADIN_CACHE_PUBLISH_TIMEOUT_SECONDS", 20)
		if timeout <= 0 {
			timeout = 20
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
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

func publishPublishersCacheBestEffort(pub *kafkaingest.Publisher, sourceURL string, publishers []string, detectedLastPage int, runUUID string) error {
	err := publishPublishersCache(pub, sourceURL, publishers, detectedLastPage, runUUID)
	if err == nil || aladinPublisherCacheRequired() {
		return err
	}
	fmt.Printf("[CACHE] publisher cache publish skipped after error=%s; continuing with sampled NAVER collection\n", shortOperationalError(err))
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
	publisherMaxTotal := envx.Int("ALADIN_PUBLISHER_MAX_TOTAL", 5000)
	publisherSeedRequired := aladinPublisherSeedRequired()

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
		if !shouldSkipAladinPublisherSeed(err) {
			return err
		}
		fmt.Printf("[ALADIN] publisher list unavailable; using cache fallback or skipping seed step error=%v\n", err)
		publishers, cachedLastPage := fallbackCachedPublishers(client, cacheTable)
		if len(publishers) == 0 {
			if publisherSeedRequired {
				return fmt.Errorf("no publishers collected from Aladin")
			}
			fmt.Printf("[SKIP] aladin publisher seed skipped because Aladin is temporarily unavailable and no cached publishers are available\n")
			return nil
		}
		currentLastPage = cachedLastPage
		sampled := samplePublishers(publishers, publisherSampleN, rand.New(rand.NewSource(time.Now().UnixNano())))
		return collectSampledPublishers(client, rawNaverTable, keys, sampled, reqsPerTerm, display, naverMaxWorkers, naverSleepMin, naverSleepMax, publisherMaxTotal)
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
			if !shouldSkipAladinPublisherSeed(err) {
				return err
			}
			fmt.Printf("[ALADIN] publisher crawl unavailable; using cache fallback or skipping seed step error=%v\n", err)
			publishers, currentLastPage = fallbackCachedPublishers(client, cacheTable)
			if len(publishers) == 0 {
				if publisherSeedRequired {
					return fmt.Errorf("no publishers collected from Aladin")
				}
				fmt.Printf("[SKIP] aladin publisher seed skipped because Aladin is temporarily unavailable and no cached publishers are available\n")
				return nil
			}
		} else {
			var skipped int
			publishers, skipped = cleanPublisherSeeds(publishers)
			fmt.Printf("[ALADIN] crawled publishers=%d skipped=%d last_page=%d\n", len(publishers), skipped, currentLastPage)
			if err := publishPublishersCacheBestEffort(baseCollector.Publisher, aladinURL, publishers, currentLastPage, runUUID); err != nil {
				return err
			}
		}
	}
	if len(publishers) == 0 {
		if !publisherSeedRequired {
			fmt.Printf("[SKIP] aladin publisher seed skipped because no publishers are available\n")
			return nil
		}
		return fmt.Errorf("no publishers collected from Aladin")
	}

	sampled := samplePublishers(publishers, publisherSampleN, rand.New(rand.NewSource(time.Now().UnixNano())))
	return collectSampledPublishers(client, rawNaverTable, keys, sampled, reqsPerTerm, display, naverMaxWorkers, naverSleepMin, naverSleepMax, publisherMaxTotal)
}

func collectSampledPublishers(client *ch.Client, rawNaverTable string, keys []naver.APIKey, sampled []string, reqsPerTerm, display, naverMaxWorkers int, naverSleepMin, naverSleepMax float64, publisherMaxTotal int) error {
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
				if err := coll.CollectPublisherAllPages(publisher, reqsPerTerm, display, naverSleepMin, naverSleepMax, publisherMaxTotal); err != nil {
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
