package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	manifestPath := flags.String("manifest", "", "verified extracted-RDF inventory JSON; input-dir is an optional local RDF root")
	driveFolder := flags.String("drive-folder-id", "", "Google Drive folder containing the complete eleven dataset folders")
	expectedFiles := flags.Int("expected-files", 208, "required complete manifest file count")
	expectedBytes := flags.Uint64("expected-bytes", 88736746306, "required manifest uncompressed bytes; zero disables byte total assertion")
	planOnly := flags.Bool("plan-only", false, "validate source inventory and report coverage without parsing or database access")
	manifestOutput := flags.String("manifest-out", "", "write verified inventory JSON outside the repository for reuse by import and publication")
	datasetsRaw := flags.String("datasets", defaultDatasets, "comma-separated NLK datasets")
	snapshotRaw := flags.String("snapshot-date", "2026-05-29", "NLK snapshot date in YYYY-MM-DD")
	batchSize := flags.Int("batch-size", 20000, "maximum resources per ClickHouse JSONEachRow batch")
	batchBytes := flags.Uint64("batch-bytes", 64*1024*1024, "estimated in-memory raw batch byte limit")
	entryShardCount := flags.Int("entry-shard-count", 1, "number of deterministic ZIP-entry shards")
	entryShardIndex := flags.Int("entry-shard-index", 0, "zero-based ZIP-entry shard index")
	resume := flags.Bool("resume", true, "resume from durable per-entry checkpoints")
	dryRun := flags.Bool("dry-run", false, "parse and validate without ClickHouse writes")
	maxRecords := flags.Uint64("max-records", 0, "maximum resources to process; zero is unlimited")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return &nlkimport.SafeError{Category: "invalid_flags"}
	}
	if (*manifestPath != "" && *driveFolder != "") || (*driveFolder != "" && *inputDir != "") || ((*planOnly || *manifestOutput != "") && *manifestPath == "" && *driveFolder == "") {
		return &nlkimport.SafeError{Category: "invalid_flags"}
	}
	if *entryShardCount < 1 || *entryShardCount > nlkimport.MaxEntryShardCount ||
		*entryShardIndex < 0 || *entryShardIndex >= *entryShardCount {
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
	var manifest *nlkimport.Manifest
	var driveSource *nlkimport.DriveSource
	if *manifestPath != "" || *driveFolder != "" {
		if *driveFolder != "" || (!*planOnly && strings.TrimSpace(*inputDir) == "") {
			driveSource, err = nlkimport.NewDriveSourceFromEnv(ctx)
			if err != nil {
				return err
			}
		}
		var inventory nlkimport.Manifest
		if *manifestPath != "" {
			inventory, err = nlkimport.LoadManifest(*manifestPath, *expectedFiles, *expectedBytes)
		} else {
			inventory, err = driveSource.Discover(ctx, strings.TrimSpace(*driveFolder), *expectedFiles, *expectedBytes)
		}
		if err != nil {
			return err
		}
		if *manifestOutput != "" && (driveSource != nil || strings.TrimSpace(*inputDir) != "") {
			inventory, err = nlkimport.VerifyManifest(ctx, nlkimport.Config{Manifest: &inventory, InputDir: strings.TrimSpace(*inputDir), DriveSource: driveSource, SnapshotDate: snapshot})
			if err != nil {
				return err
			}
		}
		manifest = &inventory
		if *manifestOutput != "" {
			body, encodeErr := json.Marshal(inventory)
			if encodeErr != nil || os.WriteFile(*manifestOutput, append(body, '\n'), 0600) != nil {
				return &nlkimport.SafeError{Category: "manifest_output_failed"}
			}
		}
		if *planOnly {
			var bytes uint64
			folders := map[string]bool{}
			for _, file := range inventory.Files {
				bytes += file.Size
				folders[file.Folder] = true
			}
			fmt.Printf("provider=nlk status=inventory_verified files=%d datasets=%d bytes=%d database_writes=false\n", len(inventory.Files), len(folders), bytes)
			return nil
		}
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
	if !*dryRun && strings.EqualFold(strings.TrimSpace(os.Getenv("NLK_PRESSURE_GATE_ENABLED")), "true") {
		lastGate := time.Time{}
		importer.BeforeBatch = func(ctx context.Context) error {
			if !lastGate.IsZero() && time.Since(lastGate) < time.Minute {
				return nil
			}
			if exec.CommandContext(ctx, "python3", "scripts/clickhouse_pressure_gate.py").Run() != nil {
				return &nlkimport.SafeError{Category: "pressure_gate_failed"}
			}
			lastGate = time.Now()
			return nil
		}
	}
	result, err := importer.Run(ctx, nlkimport.Config{
		InputDir:        strings.TrimSpace(*inputDir),
		Manifest:        manifest,
		DriveSource:     driveSource,
		Datasets:        datasets,
		SnapshotDate:    snapshot,
		BatchSize:       *batchSize,
		BatchByteLimit:  *batchBytes,
		EntryShardCount: *entryShardCount,
		EntryShardIndex: *entryShardIndex,
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
		"provider=nlk status=completed dry_run=%t limited=%t entry_shard_index=%d entry_shard_count=%d archives=%d/%d entries=%d/%d parsed=%d inserted=%d rejected=%d\n",
		*dryRun,
		result.Limited,
		*entryShardIndex,
		*entryShardCount,
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
