package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/collector"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/terms"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
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

	mode := envx.String("COLLECT_MODE", "mixed")
	batchSize := envx.Int("BATCH_SIZE", 1000)
	sampleRows := envx.Int("SAMPLE_ROWS", 8000)
	display := envx.Int("NAVER_DISPLAY", 100)
	reqsPerTerm := envx.Int("REQS_PER_TERM", 1)
	rPackageSampleSize := envx.Int("R_PACKAGE_SAMPLE_SIZE", 10)

	rawNaverTable := envx.String("RAW_NAVER_TABLE", "naver_book_raw")

	c, err := collector.New(client, rawNaverTable, keys, time.Now().UnixNano())
	if err != nil {
		return err
	}
	if err := c.ValidateIngest(context.Background()); err != nil {
		return err
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	if mode == "r_package" {
		packages, err := c.SampleRPackages(rPackageSampleSize)
		if err != nil {
			return err
		}
		queries := make([]string, 0, len(packages))
		for _, pkg := range packages {
			if query := c.RPackageBookQuery(pkg); query != "" {
				queries = append(queries, query)
			}
		}
		return runTermCollection(mode, queries, r, func(term string) error {
			return c.CollectTerm(term, mode, reqsPerTerm, display)
		})
	}
	if mode == "fixed_keyword" {
		keywords := splitFixedKeywords(envx.String("FIXED_KEYWORDS", ""))
		if len(keywords) == 0 {
			return fmt.Errorf("FIXED_KEYWORDS is required for fixed_keyword mode")
		}
		return runTermCollection(mode, keywords, r, func(term string) error {
			return c.CollectTerm(term, "keyword", reqsPerTerm, display)
		})
	}
	if mode == "mixed" {
		sample, err := c.SampleRows(100)
		if err != nil {
			return err
		}
		term := terms.GenerateKeyword(sample, r)
		return runTermCollection(mode, []string{term}, r, func(term string) error {
			return c.CollectTerm(term, mode, reqsPerTerm, display)
		})
	}

	sample, err := c.SampleRows(sampleRows)
	if err != nil {
		return err
	}
	picked := terms.PickUniqueTerms(mode, batchSize, sample, r)
	if len(picked) == 0 {
		fmt.Printf("[warn] %s collection skipped because existing book sample terms are unavailable; configure optional CH_* or CLICKHOUSE_* secrets and BOOK_SAMPLE_TABLE to avoid static fallback bias\n", mode)
		return nil
	}
	fmt.Printf("[sample] mode=%s sample_rows=%d picked_terms=%d sample_terms=%s\n", mode, len(sample), len(picked), strings.Join(sampleStrings(picked, 12), ", "))
	return runTermCollection(mode, picked, r, func(term string) error {
		return c.CollectTerm(term, mode, reqsPerTerm, display)
	})
}

func runTermCollection(mode string, picked []string, r *rand.Rand, collect func(string) error) error {
	required := collectTermRequired()
	maxConsecutiveFailures := envx.Int("COLLECT_MAX_CONSECUTIVE_FAILURES", 20)
	sleepMin := envx.Float("COLLECT_SLEEP_MIN", envx.Float("NAVER_SLEEP_MIN", 0))
	sleepMax := envx.Float("COLLECT_SLEEP_MAX", envx.Float("NAVER_SLEEP_MAX", 0))

	successes := 0
	failures := 0
	consecutiveFailures := 0
	var firstErr error

	for _, term := range picked {
		if err := collect(term); err != nil {
			if required || !collector.IsRetryableOperationalError(err) {
				return err
			}
			if firstErr == nil {
				firstErr = err
			}
			failures++
			consecutiveFailures++
			fmt.Printf("[warn] %s term skipped term=%q error=%s consecutive_failures=%d\n", mode, term, collector.ShortOperationalError(err), consecutiveFailures)
			if maxConsecutiveFailures > 0 && consecutiveFailures >= maxConsecutiveFailures {
				if successes == 0 {
					return fmt.Errorf("%s collection stopped after %d consecutive retryable failures with no successful terms; first_error=%s", mode, consecutiveFailures, collector.ShortOperationalError(firstErr))
				}
				fmt.Printf("[warn] %s collection stopped early after %d consecutive retryable failures successes=%d failures=%d\n", mode, consecutiveFailures, successes, failures)
				break
			}
			continue
		}
		successes++
		consecutiveFailures = 0
		sleepBetweenTerms(r, sleepMin, sleepMax)
	}

	if len(picked) > 0 && successes == 0 && failures > 0 {
		return fmt.Errorf("%s collection had no successful terms failures=%d first_error=%s", mode, failures, collector.ShortOperationalError(firstErr))
	}
	if failures > 0 {
		fmt.Printf("[warn] %s collection completed with retryable failures=%d successes=%d\n", mode, failures, successes)
	}
	return nil
}

func collectTermRequired() bool {
	value := strings.ToLower(strings.TrimSpace(envx.String("COLLECT_TERM_REQUIRED", "true")))
	return value != "0" && value != "false" && value != "no" && value != "off"
}

func sleepBetweenTerms(r *rand.Rand, minV, maxV float64) {
	if maxV <= 0 {
		return
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
	time.Sleep(time.Duration(seconds * float64(time.Second)))
}

func sampleStrings(values []string, limit int) []string {
	if limit <= 0 || len(values) <= limit {
		return values
	}
	out := make([]string, limit)
	copy(out, values[:limit])
	return out
}

func splitFixedKeywords(raw string) []string {
	seen := map[string]struct{}{}
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ';'
	})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		key := strings.ToLower(field)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, field)
	}
	return out
}
