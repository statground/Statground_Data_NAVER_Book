package bookcatalogpublish

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/nlkimport"
)

type item struct {
	isbn        bool
	fingerprint uint64
}
type fakeStore struct {
	source, target                                         map[string]item
	inserts, publishes, fullReads, latestReads             int
	coverageErr, insertErr, publishErr                     bool
	partial, missingMarker, sourceChanges, newerGeneration bool
	marker                                                 bool
}

func fixtureStore() *fakeStore {
	return &fakeStore{source: map[string]item{"isbn:1": {true, math.MaxUint64}, "isbn:2": {true, 7}, "record-a": {false, 11}}, target: map[string]item{}}
}
func (st *fakeStore) Validate(context.Context) error { return nil }
func (st *fakeStore) Coverage(context.Context, Config) error {
	if st.coverageErr {
		return errors.New("private source error")
	}
	return nil
}
func stats(items map[string]item, after, through string) Stats {
	var s Stats
	for key, row := range items {
		if key <= after || (through != "" && key > through) {
			continue
		}
		s.Rows++
		s.Unique++
		if row.isbn {
			s.ISBN++
		} else {
			s.Bibliography++
		}
		s.Sum += row.fingerprint
		s.Xor ^= row.fingerprint
	}
	return s
}
func (st *fakeStore) SourceStats(_ context.Context, after, through string) (Stats, error) {
	s := stats(st.source, after, through)
	if after == "" && through == "" {
		st.fullReads++
		if st.sourceChanges && st.fullReads > 1 {
			s.Sum++
		}
	}
	return s, nil
}
func (st *fakeStore) TargetStats(_ context.Context, _ State, after, through string) (Stats, error) {
	return stats(st.target, after, through), nil
}
func (st *fakeStore) NextKey(_ context.Context, after string, limit int) (string, error) {
	var keys []string
	for key := range st.source {
		if key > after {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return "", nil
	}
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys[len(keys)-1], nil
}
func (st *fakeStore) InsertChunk(_ context.Context, _ State, span Span) error {
	st.inserts++
	for key, row := range st.source {
		if key > span.After && key <= span.Through {
			st.target[key] = row
			if st.partial {
				break
			}
		}
	}
	if st.insertErr {
		return errors.New("private insert response")
	}
	return nil
}
func (st *fakeStore) Latest(context.Context) (uint64, string, error) {
	st.latestReads++
	if st.newerGeneration && st.latestReads > 1 {
		return math.MaxUint64, "other", nil
	}
	return 0, "", nil
}
func (st *fakeStore) MarkerExists(context.Context, State) (bool, error) { return st.marker, nil }
func (st *fakeStore) Publish(context.Context, State) error {
	st.publishes++
	st.marker = !st.missingMarker
	if st.publishErr {
		return errors.New("private marker response")
	}
	return nil
}
func manifestFixture(t *testing.T) nlkimport.Manifest {
	t.Helper()
	m := nlkimport.Manifest{Root: "test_manifest_root"}
	folders := []struct {
		folder, stem string
		count        int
	}{{"book", "book", 20}, {"Concept", "Concept", 3}, {"Person", "Person", 10}, {"Library", "Library", 1}, {"Organization", "Organization", 1}, {"Offline", "Offline", 36}, {"Online", "Online", 110}, {"audiovisual", "audiovisual", 10}, {"government", "govermentpublication", 4}, {"serial", "serial", 2}, {"thesis", "thesis", 11}}
	base := ExpectedBytes / ExpectedEntries
	var total uint64
	for _, f := range folders {
		for index := 0; index < f.count; index++ {
			size := base
			if len(m.Files) == ExpectedEntries-1 {
				size = ExpectedBytes - total
			}
			total += size
			m.Files = append(m.Files, nlkimport.ManifestFile{Folder: f.folder, Name: fmt.Sprintf("%s_%d.rdf", f.stem, index), ID: fmt.Sprintf("manifest_file_%04d", len(m.Files)), Size: size, Revision: "revision-verified", MD5: strings.Repeat("a", 32)})
		}
	}
	if err := m.Validate(ExpectedEntries, ExpectedBytes); err != nil {
		t.Fatal(err)
	}
	return m
}
func configFixture(t *testing.T) Config {
	return Config{Manifest: manifestFixture(t), SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC), TransformVersion: "nlk_service_projection_v1", StateFile: filepath.Join(t.TempDir(), "publish.json"), Endpoint: "Clickhouse_1", ChunkSize: 2}
}
func runnerFixture(st *fakeStore) Runner {
	return Runner{Store: st, BeforeWrite: func(context.Context) error { return nil }, Now: func() time.Time { return time.Unix(1788652800, 123) }}
}
func TestPublisherPublishesCompleteMixedCatalogLast(t *testing.T) {
	st := fixtureStore()
	cfg := configFixture(t)
	result, err := runnerFixture(st).Run(context.Background(), cfg)
	if err != nil || !result.Complete || st.inserts != 2 || st.publishes != 1 || result.Before.Rows != 3 || result.Before.ISBN != 2 || result.Before.Bibliography != 1 || result.Pending != nil {
		t.Fatalf("result=%+v error=%v inserts=%d markers=%d", result, err, st.inserts, st.publishes)
	}
	persisted, found, err := readState(cfg.StateFile)
	if err != nil || !found || !persisted.Complete {
		t.Fatal("completion journal missing")
	}
	_, err = runnerFixture(st).Run(context.Background(), cfg)
	if err != nil || st.inserts != 2 || st.publishes != 1 {
		t.Fatal("completed resume reinserted or republished")
	}
}
func TestPublisherRetainsLastGoodOnIncompleteOrChangedSource(t *testing.T) {
	for _, kind := range []string{"coverage", "fingerprint", "newer", "pressure", "zero"} {
		t.Run(kind, func(t *testing.T) {
			st := fixtureStore()
			r := runnerFixture(st)
			switch kind {
			case "coverage":
				st.coverageErr = true
			case "fingerprint":
				st.sourceChanges = true
			case "newer":
				st.newerGeneration = true
			case "pressure":
				r.BeforeWrite = func(context.Context) error { return errors.New("private endpoint") }
			case "zero":
				st.source = map[string]item{}
			}
			_, err := r.Run(context.Background(), configFixture(t))
			if err == nil || st.publishes != 0 || st.marker {
				t.Fatalf("incomplete source published: %v", err)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("raw failure leaked")
			}
		})
	}
}
func TestPublisherReconcilesAmbiguousAcceptedChunkAndMarker(t *testing.T) {
	st := fixtureStore()
	st.insertErr = true
	st.publishErr = true
	state, err := runnerFixture(st).Run(context.Background(), configFixture(t))
	if err != nil || !state.Complete || st.inserts != 2 || st.publishes != 1 {
		t.Fatalf("ack readback failed: %v", err)
	}
}
func TestPublisherDoesNotReplayAmbiguousPartialChunk(t *testing.T) {
	st := fixtureStore()
	st.partial = true
	st.insertErr = true
	cfg := configFixture(t)
	r := runnerFixture(st)
	state, err := r.Run(context.Background(), cfg)
	if SafeCategory(err) != "chunk_delivery_unresolved" || state.Pending == nil || st.publishes != 0 {
		t.Fatalf("partial insert=%+v %v", state, err)
	}
	_, err = r.Run(context.Background(), cfg)
	if SafeCategory(err) != "chunk_delivery_unresolved" || st.inserts != 1 || st.publishes != 0 {
		t.Fatal("ambiguous intent was blindly replayed")
	}
	// A later authoritative readback may reconcile a server-completed insertion.
	for key, row := range st.source {
		if key > state.Pending.After && key <= state.Pending.Through {
			st.target[key] = row
		}
	}
	st.partial = false
	st.insertErr = false
	state, err = r.Run(context.Background(), cfg)
	if err != nil || !state.Complete || st.inserts != 2 {
		t.Fatalf("reconciled resume: %+v %v", state, err)
	}
}
func TestPublisherDoesNotReplayAmbiguousMarker(t *testing.T) {
	st := fixtureStore()
	st.publishErr = true
	st.missingMarker = true
	cfg := configFixture(t)
	r := runnerFixture(st)
	_, err := r.Run(context.Background(), cfg)
	if SafeCategory(err) != "marker_delivery_unresolved" {
		t.Fatal(err)
	}
	_, err = r.Run(context.Background(), cfg)
	if SafeCategory(err) != "marker_delivery_unresolved" || st.publishes != 1 {
		t.Fatal("ambiguous marker was replayed")
	}
	st.marker = true
	state, err := r.Run(context.Background(), cfg)
	if err != nil || !state.Complete || st.publishes != 1 {
		t.Fatal("marker reconciliation failed")
	}
}
func TestPublisherRefusesForeignJournalAndConcurrentOwner(t *testing.T) {
	st := fixtureStore()
	cfg := configFixture(t)
	lock, err := lockState(cfg.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runnerFixture(st).Run(context.Background(), cfg)
	lock.Close()
	if SafeCategory(err) != "publisher_already_running" {
		t.Fatal(err)
	}
	if _, err = runnerFixture(st).Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Endpoint = "Clickhouse_2"
	_, err = runnerFixture(st).Run(context.Background(), cfg)
	if SafeCategory(err) != "state_identity_mismatch" {
		t.Fatal("foreign endpoint journal accepted")
	}
}
func TestExactFingerprintIntegerContract(t *testing.T) {
	v, err := exactUint("18446744073709551615")
	if err != nil || v != math.MaxUint64 {
		t.Fatal("UInt64 fingerprint rounded")
	}
	for _, bad := range []any{float64(math.MaxUint64), "-1", "18446744073709551616", nil} {
		if _, err := exactUint(bad); err == nil {
			t.Fatalf("unsafe UInt64 accepted: %T", bad)
		}
	}
}
