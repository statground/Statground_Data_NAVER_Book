package bookcatalogpublish

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"statground_naver_book_go/internal/nlkimport"
	"statground_naver_book_go/internal/util"
)

const MaxChunkSize = 100000
const DefaultChunkSize = 50000
const ExpectedEntries = 208
const ExpectedBytes uint64 = 88736746306

// Stats preserves UInt64 fingerprints exactly; never round-trip through float64.
type Stats struct {
	Rows         uint64 `json:"rows"`
	Unique       uint64 `json:"unique"`
	ISBN         uint64 `json:"isbn"`
	Bibliography uint64 `json:"bibliography"`
	Sum          uint64 `json:"sum"`
	Xor          uint64 `json:"xor"`
}

func (s Stats) valid() bool {
	return s.Rows > 0 && s.Rows == s.Unique && s.ISBN <= s.Rows && s.Bibliography == s.Rows-s.ISBN
}

type Span struct {
	After, Through string
	Expected       Stats
}
type State struct {
	Version                                                  int
	Endpoint, ManifestSHA256, SnapshotDate, TransformVersion string
	BatchUUID                                                string
	Generation                                               uint64
	Before                                                   Stats
	LastKey                                                  string
	Pending                                                  *Span
	MarkerPending, Complete                                  bool
	PublishedAt                                              string
}
type Config struct {
	Manifest                              nlkimport.Manifest
	SnapshotDate                          time.Time
	TransformVersion, StateFile, Endpoint string
	ChunkSize                             int
}
type Store interface {
	Validate(context.Context) error
	Coverage(context.Context, Config) error
	SourceStats(context.Context, string, string) (Stats, error)
	NextKey(context.Context, string, int) (string, error)
	TargetStats(context.Context, State, string, string) (Stats, error)
	InsertChunk(context.Context, State, Span) error
	Latest(context.Context) (uint64, string, error)
	MarkerExists(context.Context, State) (bool, error)
	Publish(context.Context, State) error
}
type Runner struct {
	Store       Store
	BeforeWrite func(context.Context) error
	Now         func() time.Time
}
type SafeError struct{ Category string }

func (e *SafeError) Error() string { return "book catalog publish failed category=" + e.Category }
func fail(category string) error   { return &SafeError{Category: category} }
func SafeCategory(err error) string {
	if e, ok := err.(*SafeError); ok {
		return e.Category
	}
	return "operation_failed"
}

var uuidPattern = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var crcPattern = regexp.MustCompile(`^[a-f0-9]{8}$`)
var versionPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

func (r Runner) Run(ctx context.Context, c Config) (State, error) {
	if r.Store == nil || r.BeforeWrite == nil || c.StateFile == "" || c.Endpoint == "" || c.SnapshotDate.IsZero() || c.ChunkSize < 1 || c.ChunkSize > MaxChunkSize || !versionPattern.MatchString(c.TransformVersion) {
		return State{}, fail("configuration")
	}
	if err := c.Manifest.Validate(ExpectedEntries, ExpectedBytes); err != nil {
		return State{}, fail("manifest_coverage")
	}
	manifestHash, err := c.Manifest.SHA256()
	if err != nil {
		return State{}, fail("manifest_identity")
	}
	lock, err := lockState(c.StateFile)
	if err != nil {
		return State{}, err
	}
	defer lock.Close()
	state, found, err := readState(c.StateFile)
	if err != nil {
		return state, err
	}
	if found && (state.Version != 1 || state.Endpoint != c.Endpoint || state.ManifestSHA256 != manifestHash || state.SnapshotDate != c.SnapshotDate.Format("2006-01-02") || state.TransformVersion != c.TransformVersion || !uuidPattern.MatchString(state.BatchUUID) || state.Generation == 0 || !state.Before.valid()) {
		return state, fail("state_identity_mismatch")
	}
	if err = r.Store.Validate(ctx); err != nil {
		return state, fail("preflight")
	}
	if err = r.BeforeWrite(ctx); err != nil {
		return state, fail("pressure_gate")
	}
	if found && (state.Complete || state.MarkerPending) {
		exists, e := r.Store.MarkerExists(ctx, state)
		if e != nil {
			return state, fail("marker_readback")
		}
		if exists {
			state.Complete = true
			state.MarkerPending = false
			return state, saveState(c.StateFile, state)
		}
		if state.MarkerPending {
			return state, fail("marker_delivery_unresolved")
		}
		return state, fail("completed_marker_missing")
	}
	if err = r.Store.Coverage(ctx, c); err != nil {
		return state, fail("source_coverage")
	}
	before, err := r.Store.SourceStats(ctx, "", "")
	if err != nil {
		return state, fail("source_read")
	}
	if !before.valid() {
		return state, fail("source_parity")
	}
	latest, _, err := r.Store.Latest(ctx)
	if err != nil {
		return state, fail("generation_read")
	}
	if !found {
		now := time.Now()
		if r.Now != nil {
			now = r.Now()
		}
		generation := uint64(now.UnixNano())
		if generation <= latest {
			generation = latest + 1
		}
		if generation == 0 {
			return state, fail("generation_overflow")
		}
		state = State{Version: 1, Endpoint: c.Endpoint, ManifestSHA256: manifestHash, SnapshotDate: c.SnapshotDate.Format("2006-01-02"), TransformVersion: c.TransformVersion, BatchUUID: util.UUIDv7(), Generation: generation, Before: before}
		if err = saveState(c.StateFile, state); err != nil {
			return state, err
		}
	} else if state.Before != before {
		return state, fail("source_changed")
	}
	if latest >= state.Generation {
		return state, fail("superseded_generation")
	}
	// A persisted intent is reconciled, never blindly replayed. Even a zero-row
	// readback may race an INSERT still running after a transport timeout.
	if state.Pending != nil {
		actual, e := r.Store.TargetStats(ctx, state, state.Pending.After, state.Pending.Through)
		if e != nil || actual != state.Pending.Expected {
			return state, fail("chunk_delivery_unresolved")
		}
		state.LastKey = state.Pending.Through
		state.Pending = nil
		if err = saveState(c.StateFile, state); err != nil {
			return state, err
		}
	}
	for {
		if err = ctx.Err(); err != nil {
			return state, fail("cancelled")
		}
		upper, e := r.Store.NextKey(ctx, state.LastKey, c.ChunkSize)
		if e != nil {
			return state, fail("source_chunk_read")
		}
		if upper == "" {
			break
		}
		if upper <= state.LastKey || len(upper) > 256 {
			return state, fail("chunk_identity")
		}
		expected, e := r.Store.SourceStats(ctx, state.LastKey, upper)
		if e != nil {
			return state, fail("source_chunk_read")
		}
		if !expected.valid() || expected.Rows > uint64(c.ChunkSize) {
			return state, fail("chunk_bound")
		}
		span := Span{After: state.LastKey, Through: upper, Expected: expected}
		actual, e := r.Store.TargetStats(ctx, state, span.After, span.Through)
		if e != nil || actual.Rows != 0 {
			return state, fail("unexpected_target_rows")
		}
		if err = r.BeforeWrite(ctx); err != nil {
			return state, fail("pressure_gate")
		}
		state.Pending = &span
		if err = saveState(c.StateFile, state); err != nil {
			return state, err
		}
		insertErr := r.Store.InsertChunk(ctx, state, span)
		actual, e = r.Store.TargetStats(ctx, state, span.After, span.Through)
		if e != nil || actual != expected {
			if insertErr != nil {
				return state, fail("chunk_delivery_unresolved")
			}
			return state, fail("chunk_readback_parity")
		}
		state.LastKey = upper
		state.Pending = nil
		if err = saveState(c.StateFile, state); err != nil {
			return state, err
		}
	}
	// The whole source must still equal the initial observation, including every
	// metadata fingerprint. Matching counts alone do not prove a stable source.
	if err = r.Store.Coverage(ctx, c); err != nil {
		return state, fail("source_coverage")
	}
	after, err := r.Store.SourceStats(ctx, "", "")
	if err != nil || after != state.Before {
		return state, fail("source_changed")
	}
	actual, err := r.Store.TargetStats(ctx, state, "", "")
	if err != nil || actual != state.Before {
		return state, fail("target_parity")
	}
	latest, _, err = r.Store.Latest(ctx)
	if err != nil || latest >= state.Generation {
		return state, fail("superseded_generation")
	}
	if err = r.BeforeWrite(ctx); err != nil {
		return state, fail("pressure_gate")
	}
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	state.PublishedAt = util.FormatCHDateTime64Millis(now)
	state.MarkerPending = true
	if err = saveState(c.StateFile, state); err != nil {
		return state, err
	}
	publishErr := r.Store.Publish(ctx, state)
	exists, err := r.Store.MarkerExists(ctx, state)
	if err != nil || !exists {
		if publishErr != nil {
			return state, fail("marker_delivery_unresolved")
		}
		return state, fail("marker_readback")
	}
	state.MarkerPending = false
	state.Complete = true
	return state, saveState(c.StateFile, state)
}

func token(parts ...string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(parts, "\x00"))))
}
func lockState(path string) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fail("state_lock")
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		f.Close()
		return nil, fail("publisher_already_running")
	}
	return f, nil
}
func readState(path string) (State, bool, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, fail("state_read")
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(body) > 65536 {
		return State{}, false, fail("state_read")
	}
	var state State
	if json.Unmarshal(body, &state) != nil {
		return state, false, fail("state_invalid")
	}
	return state, true, nil
}
func saveState(path string, state State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fail("state_encode")
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".book-catalog-*.tmp")
	if err != nil {
		return fail("state_write")
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return fail("state_write")
	}
	if os.Rename(name, path) != nil {
		return fail("state_write")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fail("state_sync")
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return fail("state_sync")
	}
	return nil
}
