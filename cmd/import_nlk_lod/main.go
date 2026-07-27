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
	"statground_naver_book_go/internal/nlkimport"
	"statground_naver_book_go/internal/nlkstore"
)

const defaultDatasets = "book,concept,person,library"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("import_nlk_lod", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	inputDir := flags.String("input-dir", "", "directory containing NLK LOD ZIP archives")
	datasetsRaw := flags.String("datasets", defaultDatasets, "comma-separated NLK datasets")
	snapshotRaw := flags.String("snapshot-date", "2026-05-29", "NLK snapshot date in YYYY-MM-DD")
	batchSize := flags.Int("batch-size", 20000, "maximum resources per ClickHouse JSONEachRow batch")
	batchBytes := flags.Uint64("batch-bytes", 64*1024*1024, "estimated in-memory raw batch byte limit")
	resume := flags.Bool("resume", true, "resume from durable per-entry checkpoints")
	dryRun := flags.Bool("dry-run", false, "parse and validate without ClickHouse writes")
	maxRecords := flags.Uint64("max-records", 0, "maximum resources to process; zero is unlimited")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return &nlkimport.SafeError{Category: "invalid_flags"}
	}

	snapshot, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(*snapshotRaw), time.FixedZone("Asia/Seoul", 9*60*60))
	if err != nil {
		return &nlkimport.SafeError{Category: "invalid_snapshot_date"}
	}
	datasets, err := nlkimport.NormalizeDatasets(splitDatasets(*datasetsRaw))
	if err != nil {
		return err
	}

	var store nlkimport.Store
	if !*dryRun {
		client, err := ch.NewFromEnv()
		if err != nil {
			return &nlkimport.SafeError{Category: "configuration"}
		}
		timeoutSeconds := envx.Int("NLK_CLICKHOUSE_TIMEOUT_SECONDS", 660)
		if timeoutSeconds < 1 {
			timeoutSeconds = 660
		}
		client.HTTPClient.Timeout = time.Duration(timeoutSeconds) * time.Second
		clickhouseStore, err := nlkstore.NewClickHouse(client, nlkstore.ConfigFromEnv())
		if err != nil {
			return &nlkimport.SafeError{Category: "configuration"}
		}
		store = clickhouseStore
	}

	importer := nlkimport.Importer{Store: store}
	result, err := importer.Run(ctx, nlkimport.Config{
		InputDir:        strings.TrimSpace(*inputDir),
		Datasets:        datasets,
		SnapshotDate:    snapshot,
		BatchSize:       *batchSize,
		BatchByteLimit:  *batchBytes,
		Resume:          *resume,
		DryRun:          *dryRun,
		MaxRecords:      *maxRecords,
		ImporterVersion: envx.String("NLK_IMPORTER_VERSION", "nlk_lod_importer_v2"),
		Source:          envx.String("PRODUCER_SOURCE", "controlled_local_import"),
	})
	if err != nil {
		return err
	}
	fmt.Printf(
		"provider=nlk status=completed dry_run=%t limited=%t archives=%d/%d entries=%d/%d parsed=%d inserted=%d rejected=%d\n",
		*dryRun,
		result.Limited,
		result.ArchivesCompleted,
		result.ArchivesTotal,
		result.EntriesCompleted,
		result.EntriesTotal,
		result.RecordsParsed,
		result.RecordsInserted,
		result.RecordsRejected,
	)
	return nil
}

func splitDatasets(raw string) []string {
	return strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r'
	})
}
