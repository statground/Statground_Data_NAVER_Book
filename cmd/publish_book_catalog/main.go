package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"statground_naver_book_go/internal/bookcatalogpublish"
	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/nlkbackfill"
	"statground_naver_book_go/internal/nlkimport"
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
	flags := flag.NewFlagSet("publish_book_catalog", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	manifestPath := flags.String("manifest", "", "verified full NLK inventory JSON")
	snapshotRaw := flags.String("snapshot-date", "2026-05-29", "NLK snapshot YYYY-MM-DD")
	statePath := flags.String("state-file", "", "durable publish journal; defaults alongside manifest")
	chunkSize := flags.Int("chunk-size", bookcatalogpublish.DefaultChunkSize, "maximum rows per insert chunk (1–100000)")
	transform := flags.String("transform-version", nlkbackfill.DefaultTransformVersion, "completed NLK service projection version")
	planOnly := flags.Bool("plan-only", false, "validate inventory without database access or writes")
	if flags.Parse(args) != nil || flags.NArg() != 0 || strings.TrimSpace(*manifestPath) == "" || *chunkSize < 1 || *chunkSize > bookcatalogpublish.MaxChunkSize {
		return &bookcatalogpublish.SafeError{Category: "invalid_flags"}
	}
	snapshot, err := time.ParseInLocation("2006-01-02", *snapshotRaw, time.FixedZone("Asia/Seoul", 9*60*60))
	if err != nil {
		return &bookcatalogpublish.SafeError{Category: "snapshot_date"}
	}
	manifest, err := nlkimport.LoadManifest(*manifestPath, bookcatalogpublish.ExpectedEntries, bookcatalogpublish.ExpectedBytes)
	if err != nil {
		return &bookcatalogpublish.SafeError{Category: "manifest_invalid"}
	}
	if *planOnly {
		fmt.Printf("book_catalog status=inventory_verified files=%d database_writes=false\n", len(manifest.Files))
		return nil
	}
	client, err := ch.NewFromEnv()
	if err != nil {
		return &bookcatalogpublish.SafeError{Category: "configuration"}
	}
	client.HTTPClient.Timeout = 660 * time.Second
	endpoint := strings.TrimSpace(os.Getenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME"))
	if *statePath == "" {
		*statePath = filepath.Join(filepath.Dir(*manifestPath), "book-catalog-publish-state.json")
	}
	gate := pressureGate()
	state, err := (bookcatalogpublish.Runner{Store: &bookcatalogpublish.ClickHouse{Client: client, Endpoint: endpoint}, BeforeWrite: gate}).Run(ctx, bookcatalogpublish.Config{Manifest: manifest, SnapshotDate: snapshot, TransformVersion: strings.TrimSpace(*transform), StateFile: *statePath, Endpoint: endpoint, ChunkSize: *chunkSize})
	if err != nil {
		return err
	}
	fmt.Printf("book_catalog status=published complete=%t rows=%d isbn=%d bibliography=%d\n", state.Complete, state.Before.Rows, state.Before.ISBN, state.Before.Bibliography)
	return nil
}

// Every executable publisher uses the shared fail-closed pressure gate. Its
// target list is fixed to these endpoint-local writes, never inherited broadly.
func pressureGate() func(context.Context) error {
	last := time.Time{}
	return func(ctx context.Context) error {
		if !last.IsZero() && time.Since(last) < time.Minute {
			return nil
		}
		command := exec.CommandContext(ctx, "python3", "scripts/clickhouse_pressure_gate.py")
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "CLICKHOUSE_PRESSURE_GATE_TARGETS=") {
				command.Env = append(command.Env, entry)
			}
		}
		command.Env = append(command.Env, "CLICKHOUSE_PRESSURE_GATE_TARGETS=local:"+bookcatalogpublish.SnapshotTable+",local:"+bookcatalogpublish.MarkerTable)
		if command.Run() != nil {
			return &bookcatalogpublish.SafeError{Category: "pressure_gate"}
		}
		last = time.Now()
		return nil
	}
}
