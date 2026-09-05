package nlkimport

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const manifestRoot = "verified_drive_root"
const manifestFileID = "verified_drive_file"
const manifestRDF = `<?xml version="1.0"?><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:dcterms="http://purl.org/dc/terms/"><rdf:Description rdf:about="http://lod.nl.go.kr/resource/NO_ISBN_1"><dcterms:title>ISBN 없는 자료</dcterms:title></rdf:Description></rdf:RDF>`

func manifestFixture(t *testing.T) (string, Manifest) {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "book"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "book", "book_0.rdf"), []byte(manifestRDF), 0600); err != nil {
		t.Fatal(err)
	}
	return root, Manifest{Root: manifestRoot, Files: []ManifestFile{{Folder: "book", ID: manifestFileID, Name: "book_0.rdf", Size: uint64(len(manifestRDF))}}}
}

func TestManifestLocalImportPreservesOriginalZIPLineageAndNonISBN(t *testing.T) {
	root, manifest := manifestFixture(t)
	store := &fakeStore{}
	result, err := (&Importer{Store: store, IDGenerator: incrementingID()}).Run(context.Background(), Config{InputDir: root, Manifest: &manifest, SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC), Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.EntriesTotal != 1 || result.EntriesCompleted != 1 || result.RecordsInserted != 1 {
		t.Fatalf("result=%+v", result)
	}
	row := store.rawBatches[0][0]
	if row["source_archive"] != "book_rdf_20260529.zip" || row["source_entry"] != "book_rdf_20260529/book_0.rdf" || row["resource_id"] != "http://lod.nl.go.kr/resource/NO_ISBN_1" {
		t.Fatalf("lineage=%v", row)
	}
	last := store.checkpoints[len(store.checkpoints)-1]
	if !strings.HasPrefix(last.SourceRevision, "local:") || last.ContentHash == "" || last.EntryCRC32 == "" || last.Status != "succeeded" {
		t.Fatalf("checkpoint=%+v", last)
	}
}

func TestManifestAdoptsVerifiedLegacyCompletedCheckpointWithoutReinsert(t *testing.T) {
	root, manifest := manifestFixture(t)
	sum := sha256.Sum256([]byte(manifestRDF))
	store := &fakeStore{hasCheckpoint: true, checkpoint: Checkpoint{DatasetName: "book", Status: "succeeded", NextRecordIndex: 1, RecordsInserted: 1, EntryCRC32: fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(manifestRDF))), EntryUncompressed: uint64(len(manifestRDF)), ContentHash: hex.EncodeToString(sum[:])}}
	result, err := (&Importer{Store: store, IDGenerator: incrementingID()}).Run(context.Background(), Config{InputDir: root, Manifest: &manifest, SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC), Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.EntriesCompleted != 1 || result.RecordsInserted != 0 || len(store.rawBatches) != 0 || len(store.checkpoints) != 1 || store.checkpoints[0].SourceRevision == "" {
		t.Fatalf("resume=%+v checkpoints=%+v", result, store.checkpoints)
	}
	store.checkpoint.ContentHash = strings.Repeat("a", 64)
	_, err = (&Importer{Store: store, IDGenerator: incrementingID()}).Run(context.Background(), Config{InputDir: root, Manifest: &manifest, SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC), Resume: true})
	if ErrorCategory(err) != "checkpoint_content_mismatch" {
		t.Fatalf("changed prior content accepted: %v", err)
	}
}

func TestManifestCoverageAndTraversalFailBeforeDatabase(t *testing.T) {
	root, manifest := manifestFixture(t)
	if err := manifest.Validate(208, 88736746306); ErrorCategory(err) != "manifest_coverage" {
		t.Fatalf("partial inventory accepted: %v", err)
	}
	duplicate := manifest
	duplicate.Files = append(append([]ManifestFile(nil), manifest.Files...), manifest.Files[0])
	if err := duplicate.Validate(2, 0); ErrorCategory(err) != "manifest_duplicate" {
		t.Fatal(err)
	}
	manifest.Files[0].Name = "../book_0.rdf"
	store := &fakeStore{}
	_, err := (&Importer{Store: store}).Run(context.Background(), Config{InputDir: root, Manifest: &manifest, SnapshotDate: time.Now()})
	if err == nil || store.validateCalls != 0 || len(store.rawBatches) != 0 {
		t.Fatalf("unsafe manifest reached database: %v", err)
	}
}

func TestDriveManifestMetadataAndMediaIntegrity(t *testing.T) {
	sum := md5.Sum([]byte(manifestRDF))
	checksum := hex.EncodeToString(sum[:])
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("alt") == "media" {
			_, _ = io.WriteString(w, manifestRDF)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": manifestFileID, "name": "book_0.rdf", "size": fmt.Sprint(len(manifestRDF)), "md5Checksum": checksum, "version": "7", "modifiedTime": "2026-05-29T00:00:00Z"})
	}))
	defer server.Close()
	source := &DriveSource{client: server.Client(), base: server.URL}
	manifest := Manifest{Root: manifestRoot, Files: []ManifestFile{{Folder: "book", ID: manifestFileID, Name: "book_0.rdf", Size: uint64(len(manifestRDF))}}}
	store := &fakeStore{}
	result, err := (&Importer{Store: store, IDGenerator: incrementingID()}).Run(context.Background(), Config{Manifest: &manifest, DriveSource: source, SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC), Resume: true})
	if err != nil || result.EntriesCompleted != 1 || requests != 3 {
		t.Fatalf("drive import result=%+v requests=%d err=%v", result, requests, err)
	}
	if !strings.Contains(store.checkpoints[len(store.checkpoints)-1].SourceRevision, checksum) {
		t.Fatal("remote checksum missing from revision")
	}
	manifest.Files[0].MD5 = strings.Repeat("0", 32)
	before := len(store.rawBatches)
	_, err = (&Importer{Store: store}).Run(context.Background(), Config{Manifest: &manifest, DriveSource: source, SnapshotDate: time.Now()})
	if ErrorCategory(err) != "drive_source_changed" || len(store.rawBatches) != before {
		t.Fatalf("changed Drive revision accepted: %v", err)
	}
}

func TestDriveCredentialsAndErrorsDoNotExposeSecrets(t *testing.T) {
	t.Setenv("NLK_GOOGLE_DRIVE_ACCESS_TOKEN", "")
	t.Setenv("NLK_GOOGLE_SERVICE_ACCOUNT_FILE", "")
	t.Setenv("NLK_GOOGLE_SERVICE_ACCOUNT_JSON", `{"type":"service_account","token_uri":"https://attacker.example/secret"}`)
	if _, err := NewDriveSourceFromEnv(context.Background()); ErrorCategory(err) != "drive_credentials" || strings.Contains(err.Error(), "secret") {
		t.Fatalf("credential boundary=%v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "secret-token")
	}))
	defer server.Close()
	source := &DriveSource{client: server.Client(), base: server.URL}
	_, err := source.metadata(context.Background(), ManifestFile{ID: manifestFileID})
	if err == nil || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("unsafe HTTP error=%v", err)
	}
}

func TestVerifiedEntryRejectsCorruptSameLengthBody(t *testing.T) {
	sum := md5.Sum([]byte("good"))
	stream := newVerifiedEntryReader(io.NopCloser(strings.NewReader("evil")), 4, hex.EncodeToString(sum[:]))
	_, _ = io.Copy(io.Discard, stream)
	if ErrorCategory(stream.Verify()) != "entry_checksum_failed" {
		t.Fatal("corrupt body accepted")
	}
}

func TestDriveCorruptionNeverAdvancesRawCheckpointAndRemovesCache(t *testing.T) {
	sum := md5.Sum([]byte(manifestRDF))
	checksum := hex.EncodeToString(sum[:])
	cache := t.TempDir()
	t.Setenv("NLK_RDF_CACHE_DIR", cache)
	changed := strings.Replace(manifestRDF, "NO_ISBN_1", "NO_ISBN_2", 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("alt") == "media" {
			_, _ = io.WriteString(w, changed)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": manifestFileID, "name": "book_0.rdf", "size": fmt.Sprint(len(manifestRDF)), "md5Checksum": checksum, "version": "7"})
	}))
	defer server.Close()
	source := &DriveSource{client: server.Client(), base: server.URL}
	manifest := Manifest{Root: manifestRoot, Files: []ManifestFile{{Folder: "book", ID: manifestFileID, Name: "book_0.rdf", Size: uint64(len(manifestRDF))}}}
	store := &fakeStore{}
	_, err := (&Importer{Store: store, IDGenerator: incrementingID()}).Run(context.Background(), Config{Manifest: &manifest, DriveSource: source, SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC), Resume: true})
	if err == nil || len(store.rawBatches) != 0 {
		t.Fatalf("corrupt source reached raw table: %v", err)
	}
	for _, checkpoint := range store.checkpoints {
		if checkpoint.NextRecordIndex != 0 || checkpoint.Status == "succeeded" {
			t.Fatalf("corrupt checkpoint advanced: %+v", checkpoint)
		}
	}
	files, err := os.ReadDir(cache)
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary source retained files=%d err=%v", len(files), err)
	}
}

func TestManifestPressureGateFailurePreservesRawProgress(t *testing.T) {
	root, manifest := manifestFixture(t)
	store := &fakeStore{}
	importer := Importer{Store: store, IDGenerator: incrementingID(), BeforeBatch: func(context.Context) error { return fmt.Errorf("pressure") }}
	_, err := importer.Run(context.Background(), Config{InputDir: root, Manifest: &manifest, SnapshotDate: time.Now()})
	if ErrorCategory(err) != "pressure_gate_failed" || len(store.rawBatches) != 0 {
		t.Fatalf("gate bypassed err=%v", err)
	}
	if checkpoint := store.checkpoints[len(store.checkpoints)-1]; checkpoint.NextRecordIndex != 0 {
		t.Fatalf("gate advanced checkpoint %+v", checkpoint)
	}
}
