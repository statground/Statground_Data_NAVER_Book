package main

import (
	"fmt"
	"math/rand"
	"os"
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
	client, err := ch.NewFromEnv()
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

	c, err := collector.New(client, "raw_naver", keys, time.Now().UnixNano())
	if err != nil {
		return err
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
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
