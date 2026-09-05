package nlkstore

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/nlkimport"
	"statground_naver_book_go/internal/nlklod"
	"statground_naver_book_go/internal/util"
)

// Opt-in deployment proof: this test only reads the verified RDF source and
// executes SELECTs. No importer, checkpoint, or INSERT method is invoked.
func TestRawResumeReadOnlySourceProof(t *testing.T) {
	fixturePath := os.Getenv("NLK_TEST_RAW_READ_ONLY_FIXTURE")
	if fixturePath == "" {
		t.Skip("read-only source/replica proof not requested")
	}
	var fixture struct {
		SourcePath, MD5, SnapshotDate, DatasetName, Archive, Entry, Endpoint string
		Size                                                                 int64
		ParsedRows, ExistingRows                                             uint64
	}
	body, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal("source fixture unavailable")
	}
	if json.Unmarshal(body, &fixture) != nil || fixture.ParsedRows == 0 || fixture.ParsedRows > 5000 || fixture.ExistingRows > fixture.ParsedRows || fixture.Size <= 0 {
		t.Fatal("invalid bounded source fixture")
	}
	snapshot, err := time.Parse("2006-01-02", fixture.SnapshotDate)
	if err != nil {
		t.Fatal("invalid snapshot")
	}
	file, err := os.Open(fixture.SourcePath)
	if err != nil {
		t.Fatal("source unavailable")
	}
	defer file.Close()
	hash := md5.New()
	sourceSHA := sha256.New()
	size, err := io.Copy(io.MultiWriter(hash, sourceSHA), file)
	if err != nil || size != fixture.Size || fmt.Sprintf("%x", hash.Sum(nil)) != fixture.MD5 {
		t.Fatal("source file hash/size mismatch")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal("source seek failed")
	}
	lineage := nlkimport.RawLineage{SnapshotDate: snapshot, DatasetName: fixture.DatasetName, Archive: fixture.Archive, Entry: fixture.Entry}
	rows := make([]map[string]any, 0, fixture.ParsedRows)
	stop := errors.New("bounded source limit")
	err = nlklod.StreamResources(file, func(index uint64, resource nlklod.Resource) error {
		if index >= fixture.ParsedRows {
			return stop
		}
		row, err := nlklod.BuildRow(resource, nlklod.Evidence{RunUUID: "00000000-0000-4000-8000-000000000001", DatasetName: lineage.DatasetName, SnapshotDate: snapshot, SourceArchive: lineage.Archive, SourceEntry: lineage.Entry, RecordIndex: index, ImportedAt: time.Unix(1, 0)})
		if err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	})
	if !errors.Is(err, stop) || uint64(len(rows)) != fixture.ParsedRows {
		t.Fatal("bounded source parsing failed")
	}
	client, err := ch.NewFromEnv()
	if err != nil {
		t.Fatal("read-only connection configuration failed")
	}
	client.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal("store configuration failed")
	}
	if err := store.validateConnectionBoundary(); err != nil {
		t.Fatal("private/HTTPS connection boundary failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := client.ValidateDirectEndpointHostnameContext(ctx, fixture.Endpoint); err != nil {
		t.Fatal("physical endpoint mismatch")
	}
	_, indexes, _ := rawExpectedRows(lineage, rows)
	query := rawReplicaReadbackSQL(lineage, indexes)
	if _, err := client.QueryJSONEachRowContext(ctx, "SELECT formatQuery("+util.SQLString(query)+") AS formatted SETTINGS max_execution_time=5"); err != nil {
		t.Fatal("deployed read-only SQL syntax check failed")
	}
	accepted, err := store.VerifiedRawRecordIndexes(ctx, lineage, rows)
	if err != nil || uint64(len(accepted)) != fixture.ExistingRows {
		t.Fatalf("read-only source proof accepted=%d expected=%d error=%v", len(accepted), fixture.ExistingRows, err)
	}
	if err := client.ValidateDirectEndpointHostnameContext(ctx, fixture.Endpoint); err != nil {
		t.Fatal("physical endpoint changed")
	}
	if dir := os.Getenv("NLK_TEST_RAW_PROOF_DIR"); dir != "" {
		report := map[string]any{"read_only": true, "at_utc": time.Now().UTC().Format(time.RFC3339), "endpoint": fixture.Endpoint, "source_md5": fixture.MD5, "source_sha256": fmt.Sprintf("%x", sourceSHA.Sum(nil)), "source_bytes": size, "source_entry": fixture.Entry, "parsed_rows": len(rows), "verified_both_replica_rows": len(accepted), "deployed_format_query": true, "database_writes": false}
		encoded, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "raw-resume-source-proof.json"), append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("read-only source proof: parsed=%d verified_on_both_replicas=%d", len(rows), len(accepted))
}
