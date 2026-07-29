package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/nlkbackfill"
	"statground_naver_book_go/internal/nlkstore"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("backfill_nlk_service", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	snapshotRaw := flags.String(
		"snapshot-date",
		envx.String("NLK_SERVICE_BACKFILL_SNAPSHOT_DATE", ""),
		"required NLK snapshot date in YYYY-MM-DD",
	)
	rangeSize := flags.Uint64(
		"range-size",
		uint64(positiveEnvInt("NLK_SERVICE_BACKFILL_RANGE_SIZE", int(nlkbackfill.DefaultRangeSize))),
		"contiguous source_record_index range size",
	)
	projectionsRaw := flags.String(
		"projections",
		"",
		"comma-separated allowlisted projections; empty uses the safe default order",
	)
	transformVersion := flags.String(
		"transform-version",
		envx.String("NLK_SERVICE_BACKFILL_TRANSFORM_VERSION", nlkbackfill.DefaultTransformVersion),
		"allowlisted transform version label",
	)
	concurrency := flags.Int("concurrency", 1, "projection concurrency; only 1 is accepted")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return &nlkbackfill.SafeError{Category: "invalid_flags"}
	}
	if *concurrency != 1 {
		return &nlkbackfill.SafeError{Category: "invalid_concurrency"}
	}
	snapshot, err := time.ParseInLocation(
		"2006-01-02",
		strings.TrimSpace(*snapshotRaw),
		time.FixedZone("Asia/Seoul", 9*60*60),
	)
	if err != nil {
		return &nlkbackfill.SafeError{Category: "invalid_snapshot_date"}
	}
	projections, err := nlkbackfill.NormalizeProjections(splitList(*projectionsRaw))
	if err != nil {
		return err
	}

	client, err := ch.NewFromEnv()
	if err != nil {
		return &nlkbackfill.SafeError{Category: "configuration"}
	}
	timeoutSeconds := positiveEnvInt("NLK_SERVICE_BACKFILL_TIMEOUT_SECONDS", 1810)
	client.HTTPClient.Timeout = time.Duration(timeoutSeconds) * time.Second
	store, err := nlkstore.NewClickHouse(client, nlkstore.ConfigFromEnv())
	if err != nil {
		return &nlkbackfill.SafeError{Category: "configuration"}
	}
	result, err := (nlkbackfill.Runner{Store: store}).Run(ctx, nlkbackfill.Config{
		SnapshotDate:     snapshot,
		RangeSize:        *rangeSize,
		Projections:      projections,
		TransformVersion: strings.TrimSpace(*transformVersion),
	})
	if err != nil {
		return err
	}
	fmt.Printf(
		"provider=nlk status=completed snapshot=%s concurrency=1 entries=%d projections=%d skipped=%d ranges=%d records=%d\n",
		snapshot.Format("2006-01-02"),
		result.EntriesTotal,
		result.ProjectionsHandled,
		result.ProjectionsSkipped,
		result.RangesCompleted,
		result.RecordsCovered,
	)
	return nil
}

func splitList(raw string) []string {
	return strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r'
	})
}

func positiveEnvInt(name string, fallback int) int {
	value := envx.Int(name, fallback)
	if value < 1 {
		return fallback
	}
	return value
}
