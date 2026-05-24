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
		return c.CollectRPackageBooks(rPackageSampleSize, reqsPerTerm, display)
	}
	if mode == "fixed_keyword" {
		keywords := splitFixedKeywords(envx.String("FIXED_KEYWORDS", ""))
		if len(keywords) == 0 {
			return fmt.Errorf("FIXED_KEYWORDS is required for fixed_keyword mode")
		}
		for _, term := range keywords {
			if err := c.CollectTerm(term, "keyword", reqsPerTerm, display); err != nil {
				return err
			}
		}
		return nil
	}
	if mode == "mixed" {
		sample, err := c.SampleRows(100)
		if err != nil {
			return err
		}
		term := terms.GenerateKeyword(sample, r)
		return c.CollectTerm(term, mode, reqsPerTerm, display)
	}

	sample, err := c.SampleRows(sampleRows)
	if err != nil {
		return err
	}
	picked := terms.PickUniqueTerms(mode, batchSize, sample, r)
	for _, term := range picked {
		if err := c.CollectTerm(term, mode, reqsPerTerm, display); err != nil {
			return err
		}
	}
	return nil
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
