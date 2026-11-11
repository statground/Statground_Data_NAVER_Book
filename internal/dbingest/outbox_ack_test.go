package dbingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/writerlease"
)

// This fixture models the retained outbox independently from the in-memory
// command. No actual database, credentials or network are used.
type ackLedgerFixture struct {
	mu             sync.Mutex
	bodies         []string
	rows           map[string]map[string]any
	marked         map[string]int
	targetRows     []map[string]any
	markerSizes    []int
	markerFailure  bool
	targetFailure  bool
	markerWait     bool
	markerBudget   time.Duration
	markerDeadline time.Time
}

func newACKLedgerFixture() *ackLedgerFixture {
	return &ackLedgerFixture{rows: make(map[string]map[string]any), marked: make(map[string]int)}
}

var markerUUIDs = regexp.MustCompile(`toUUID\('([^']+)'\)`)

func (f *ackLedgerFixture) roundTrip(r *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	text := string(body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bodies = append(f.bodies, text)
	switch {
	case strings.HasPrefix(strings.TrimSpace(text), "ALTER TABLE"):
		matches := markerUUIDs.FindAllStringSubmatch(text, -1)
		f.markerSizes = append(f.markerSizes, len(matches))
		if !strings.Contains(text, "mutations_sync = 2") || r.URL.Query().Get("mutations_sync") != "2" {
			return nil, errors.New("marker lost synchronous acknowledgement")
		}
		if deadline, ok := r.Context().Deadline(); ok {
			f.markerBudget = time.Until(deadline)
			f.markerDeadline = deadline
		}
		if f.markerWait {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		if f.markerFailure {
			return nil, errors.New("fixture marker response lost after dispatch")
		}
		for _, match := range matches {
			if _, exists := f.rows[match[1]]; !exists {
				return nil, errors.New("marker did not identify retained payload")
			}
			f.marked[match[1]]++
		}
	case strings.HasPrefix(text, "INSERT INTO"):
		_, payload, ok := strings.Cut(text, "\n")
		if !ok {
			return nil, errors.New("missing JSONEachRow payload")
		}
		for _, line := range strings.Split(strings.TrimSpace(payload), "\n") {
			var row map[string]any
			if json.Unmarshal([]byte(line), &row) != nil {
				return nil, errors.New("invalid fixture row")
			}
			if uuid, ok := row["outbox_uuid"].(string); ok {
				f.rows[uuid] = row
				continue
			}
			if f.targetFailure {
				return nil, errors.New("fixture target response lost after dispatch")
			}
			// A positive target ACK can only follow durable payload preservation.
			if len(f.rows) <= len(f.targetRows) {
				return nil, errors.New("target preceded durable outbox")
			}
			f.targetRows = append(f.targetRows, row)
		}
	case strings.Contains(text, "WHERE replayed_at IS NULL"):
		var result strings.Builder
		for uuid, row := range f.rows {
			if f.marked[uuid] == 0 {
				result.WriteString(fmtJSONRow(row))
			}
		}
		return dbResponse(r, 200, result.String()), nil
	case strings.Contains(text, "FROM system.replicas"):
		return dbResponse(r, 200, "{\"replicas\":1,\"writable\":1}\n"), nil
	case strings.Contains(text, "clusterAllReplicas("):
		var result strings.Builder
		for _, row := range f.targetRows {
			result.WriteString(fmtJSONRow(row))
		}
		return dbResponse(r, 200, result.String()), nil
	default:
		return nil, errors.New("unexpected fixture operation")
	}
	return dbResponse(r, 200, ""), nil
}

func ackFixtureRows(index int) []map[string]any {
	return []map[string]any{{"created_at": "2026-11-11 00:00:00.000", "provider": "naver", "isbn": fmt.Sprintf("978%010d", index), "version": 1}}
}

func newGroupedTestWriter(client *ch.Client) (*Writer, error) {
	return NewFromEnv(client, "naver_book_raw")
}

func TestOutboxACKDefaultRemainsImmediateAndGroupedNeedsCommand(t *testing.T) {
	f := newACKLedgerFixture()
	c := testWriterClient(f.roundTrip)
	w, err := newGroupedTestWriter(c)
	if err != nil || w.acks != nil {
		t.Fatalf("default scope=%v error=%v", w, err)
	}
	for i := 0; i < 2; i++ {
		if err := w.InsertRawRows(ackFixtureRows(i)); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.markerSizes) != 2 || len(f.marked) != 2 || len(f.bodies) != 6 {
		t.Fatalf("default marker=%v marked=%d calls=%d", f.markerSizes, len(f.marked), len(f.bodies))
	}
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	if _, err := newGroupedTestWriter(c); err == nil || len(f.bodies) != 6 {
		t.Fatal("grouped writer outside a command reached transport")
	}
	for _, bad := range []string{"0", "26", "-1", "unknown"} {
		t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", bad)
		if _, err := newGroupedTestWriter(c); err == nil {
			t.Fatalf("invalid ACK bound %q admitted", bad)
		}
	}
}

func TestOutboxACKGroupBoundariesAndFinalSuccessBarrier(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	for _, batches := range []int{1, 25, 26} {
		t.Run(fmt.Sprint(batches), func(t *testing.T) {
			f := newACKLedgerFixture()
			var writer *Writer
			err := ch.RunWriterCommand(func() error {
				var err error
				writer, err = newGroupedTestWriter(testWriterClient(f.roundTrip))
				if err != nil {
					return err
				}
				for i := 0; i < batches; i++ {
					if err := writer.InsertRawRows(ackFixtureRows(i)); err != nil {
						return err
					}
				}
				if len(f.marked) != batches/25*25 || len(writer.acks.pending) != batches%25 {
					t.Fatal("unflushed durable rows lost pending visibility")
				}
				return nil
			})
			if err != nil || len(f.rows) != batches || len(f.targetRows) != batches || len(f.marked) != batches || len(f.markerSizes) != (batches+24)/25 {
				t.Fatalf("err=%v retained=%d targets=%d marked=%d markerSizes=%v", err, len(f.rows), len(f.targetRows), len(f.marked), f.markerSizes)
			}
			for _, count := range f.marked {
				if count != 1 {
					t.Fatal("duplicate group acknowledgement")
				}
			}
			for _, row := range f.rows {
				decoded, err := decodeOutboxRows(row["rows_json"].(string), int(row["row_count"].(float64)))
				if err != nil {
					t.Fatal(err)
				}
				_, _, recomputed, err := encodeOutboxRows(row["target_table"].(string), decoded)
				if err != nil || recomputed != row["deduplication_token"] {
					t.Fatal("payload/dedup identity changed")
				}
			}
			before := len(f.bodies)
			if err := writer.InsertRawRows(ackFixtureRows(99)); err == nil || len(f.bodies) != before {
				t.Fatal("closed command writer admitted a new mutation")
			}
		})
	}
}

func TestOutboxACKSharedAcrossWritersTimeoutClonesAndTargets(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	f := newACKLedgerFixture()
	c := testWriterClient(f.roundTrip)
	err := ch.RunWriterCommand(func() error {
		first, err := newGroupedTestWriter(c)
		if err != nil {
			return err
		}
		second, err := newGroupedTestWriter(c)
		if err != nil {
			return err
		}
		clone := second.WithTimeout(250 * time.Millisecond)
		if first.acks != second.acks || clone.acks != first.acks || clone.Client == c {
			t.Fatal("collector/clone did not share the same ACK owner")
		}
		var wg sync.WaitGroup
		failures := make(chan error, 30)
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var err error
				switch i % 3 {
				case 0:
					err = first.InsertRawRows(ackFixtureRows(i))
				case 1:
					err = second.InsertPublisherCacheRows([]map[string]any{{"publisher": fmt.Sprint(i)}})
				case 2:
					err = clone.InsertCollectLogRows([]map[string]any{{"event_uuid": fmt.Sprintf("01900000-0000-7000-8000-%012d", i)}})
				}
				failures <- err
			}(i)
		}
		wg.Wait()
		close(failures)
		for err := range failures {
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || len(f.rows) != 30 || len(f.marked) != 30 || fmt.Sprint(f.markerSizes) != "[25 5]" || f.markerBudget > 250*time.Millisecond {
		t.Fatalf("err=%v rows=%d marked=%d markers=%v budget=%s", err, len(f.rows), len(f.marked), f.markerSizes, f.markerBudget)
	}
}

func TestOutboxACKUnknownMarkerLatchesWithoutFinalizerReplay(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "2")
	f := newACKLedgerFixture()
	f.markerFailure = true
	var scope *outboxACKScope
	err := ch.RunWriterCommand(func() error {
		w, err := newGroupedTestWriter(testWriterClient(f.roundTrip))
		if err != nil {
			return err
		}
		scope = w.acks
		if err := w.InsertRawRows(ackFixtureRows(1)); err != nil {
			return err
		}
		first := w.InsertRawRows(ackFixtureRows(2))
		if first == nil || !IsDurabilityError(first) {
			t.Fatal("unknown marker was accepted")
		}
		before := len(f.bodies)
		if again := w.InsertRawRows(ackFixtureRows(3)); again != first || len(f.bodies) != before {
			t.Fatal("unknown marker allowed another write")
		}
		return nil // Even a mistakenly swallowed business error must fail finalization.
	})
	if err == nil || len(f.markerSizes) != 1 || len(f.marked) != 0 || len(scope.pending) != 2 {
		t.Fatalf("err=%v markers=%v marked=%v", err, f.markerSizes, f.marked)
	}
	before := len(f.bodies)
	if scope.FinishWriterCommand(true) == nil || len(f.bodies) != before {
		t.Fatal("repeated finalizer retried unknown mutation")
	}
}

func TestOutboxACKBusinessFailureAndTargetUnknownLeaveDurablePending(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	for _, targetUnknown := range []bool{false, true} {
		t.Run(fmt.Sprint(targetUnknown), func(t *testing.T) {
			f := newACKLedgerFixture()
			business := errors.New("provider failed")
			err := ch.RunWriterCommand(func() error {
				w, err := newGroupedTestWriter(testWriterClient(f.roundTrip))
				if err != nil {
					return err
				}
				if err := w.InsertRawRows(ackFixtureRows(1)); err != nil {
					return err
				}
				if targetUnknown {
					f.targetFailure = true
					return w.InsertRawRows(ackFixtureRows(2))
				}
				return business
			})
			if err == nil || (!targetUnknown && !errors.Is(err, business)) || len(f.marked) != 0 || len(f.markerSizes) != 0 {
				t.Fatalf("err=%v markers=%v", err, f.markerSizes)
			}
		})
	}
}

func TestOutboxACKFinalizerTimeoutUsesCloneBudgetAndNeverRetries(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	f := newACKLedgerFixture()
	f.markerWait = true
	var scope *outboxACKScope
	err := ch.RunWriterCommand(func() error {
		w, err := newGroupedTestWriter(testWriterClient(f.roundTrip))
		if err != nil {
			return err
		}
		scope = w.acks
		return w.WithTimeout(20 * time.Millisecond).InsertCollectLogRows([]map[string]any{{"event_uuid": "01900000-0000-7000-8000-000000000001"}})
	})
	if err == nil || len(f.markerSizes) != 1 || f.markerBudget > 20*time.Millisecond || f.markerBudget <= 0 || len(f.marked) != 0 {
		t.Fatalf("err=%v markers=%v budget=%s", err, f.markerSizes, f.markerBudget)
	}
	if scope.FinishWriterCommand(true) == nil || len(f.markerSizes) != 1 {
		t.Fatal("timed out finalizer retried")
	}
}

func TestOutboxACKExplicitCallerCancellationBeforeAndDuringFinalizer(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	for _, during := range []bool{false, true} {
		t.Run(fmt.Sprint(during), func(t *testing.T) {
			f := newACKLedgerFixture()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			c := testWriterClient(func(r *http.Request) (*http.Response, error) {
				if during && strings.Contains(r.URL.RawQuery, "mutations_sync=2") {
					body, _ := io.ReadAll(r.Body)
					r.Body = io.NopCloser(strings.NewReader(string(body)))
					if strings.HasPrefix(strings.TrimSpace(string(body)), "ALTER TABLE") {
						close(started)
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
				}
				return f.roundTrip(r)
			})
			if during {
				go func() { <-started; cancel() }()
			}
			err := ch.RunWriterCommand(func() error {
				w, err := newGroupedTestWriter(c)
				if err != nil {
					return err
				}
				if err := w.InsertRawRowsContext(parent, ackFixtureRows(1)); err != nil {
					return err
				}
				if !during {
					cancel()
				}
				return nil
			})
			if err == nil || len(f.marked) != 0 || len(f.targetRows) != 1 || len(f.rows) != 1 {
				t.Fatalf("err=%v retained=%d target=%d marked=%d", err, len(f.rows), len(f.targetRows), len(f.marked))
			}
			if !during && len(f.bodies) != 2 {
				t.Fatal("canceled explicit parent reached marker transport")
			}
		})
	}
}

func TestOutboxACKExplicitCallerDeadlineIsNotExtended(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	f := newACKLedgerFixture()
	f.markerWait = true
	parent, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	parentDeadline, _ := parent.Deadline()
	started := time.Now()
	err := ch.RunWriterCommand(func() error {
		w, err := newGroupedTestWriter(testWriterClient(f.roundTrip))
		if err != nil {
			return err
		}
		if err := w.InsertRawRowsContext(parent, ackFixtureRows(1)); err != nil {
			return err
		}
		// Consume part of the caller deadline before the new marker operation.
		time.Sleep(25 * time.Millisecond)
		return nil
	})
	if err == nil || parent.Err() != context.DeadlineExceeded || len(f.marked) != 0 || time.Since(started) > 200*time.Millisecond || f.markerDeadline.After(parentDeadline) || f.markerDeadline.IsZero() {
		t.Fatalf("err=%v parent=%v elapsed=%s", err, parent.Err(), time.Since(started))
	}
}

type ackAdmission struct {
	mu               sync.Mutex
	begun, confirmed int
	failAt           int
}

func (a *ackAdmission) Assert(context.Context) error { return nil }
func (a *ackAdmission) Close() error                 { return nil }
func (a *ackAdmission) Begin(ctx context.Context, intent writerlease.Intent) (context.Context, writerlease.Operation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.begun++
	if intent.RequestSHA256 == "" || intent.QueryID == "" || intent.Target == "" {
		return nil, nil, errors.New("missing mutation identity")
	}
	if a.begun == a.failAt {
		return nil, nil, errors.New("lease lost before mutation")
	}
	return ctx, ackOperation{a}, nil
}

type ackOperation struct{ admission *ackAdmission }

func (o ackOperation) Confirm(context.Context) error {
	o.admission.mu.Lock()
	defer o.admission.mu.Unlock()
	o.admission.confirmed++
	return nil
}
func (ackOperation) Abandon() {}

func TestOutboxACKRequiresLeaseAdmissionForEveryActualMutation(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "2")
	for _, failAt := range []int{0, 5} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			f := newACKLedgerFixture()
			a := &ackAdmission{failAt: failAt}
			c := testWriterClient(f.roundTrip)
			c.WriterAdmission = a
			err := ch.RunWriterCommand(func() error {
				w, err := newGroupedTestWriter(c)
				if err != nil {
					return err
				}
				for i := 0; i < 2; i++ {
					if err := w.InsertRawRows(ackFixtureRows(i)); err != nil {
						return err
					}
				}
				return nil
			})
			if (err != nil) != (failAt != 0) || a.begun != 5 {
				t.Fatalf("err=%v admissions=%d", err, a.begun)
			}
			if failAt == 0 && (a.confirmed != 5 || len(f.marked) != 2) {
				t.Fatal("actual mutations did not each receive positive lease confirmation")
			}
			if failAt != 0 && (a.confirmed != 4 || len(f.markerSizes) != 0 || len(f.marked) != 0) {
				t.Fatal("lost lease reached marker transport")
			}
		})
	}
}

func TestOutboxACKInterruptedCommandReconcilesThroughOriginalOutbox(t *testing.T) {
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	f := newACKLedgerFixture()
	c := testWriterClient(f.roundTrip)
	err := ch.RunWriterCommand(func() error {
		w, err := newGroupedTestWriter(c)
		if err != nil {
			return err
		}
		if err := w.InsertRawRows(ackFixtureRows(1)); err != nil {
			return err
		}
		return errors.New("interrupted before command success")
	})
	if err == nil || len(f.rows) != 1 || len(f.marked) != 0 {
		t.Fatal("interrupted command lost pending durable row")
	}
	// A fresh process has no coalescer memory. The original bounded replay reads
	// the old marker, proves the exact existing target and marks without INSERT.
	beforeTargets := len(f.targetRows)
	if err := testWriter(c).replayOutbox(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.marked) != 1 || len(f.targetRows) != beforeTargets || len(f.markerSizes) != 1 {
		t.Fatalf("marked=%d targets=%d marker=%v", len(f.marked), len(f.targetRows), f.markerSizes)
	}
}
