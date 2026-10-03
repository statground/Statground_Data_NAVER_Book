package nlkstore

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/nlkbackfill"
	"statground_naver_book_go/internal/util"
)

func TestValidateBackfillRejectsSafeButUnallowlistedTableBeforeRequest(t *testing.T) {
	requests := 0
	client := &ch.Client{WriterAdmission: fixtureAdmission{},
		Host:     "https://clickhouse.test",
		Protocol: "https",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			return nil, nil
		})},
	}
	config := ConfigFromEnv()
	config.RawTable = "Private_Data.secret_table"
	store, err := NewClickHouse(client, config)
	if err != nil {
		t.Fatalf("NewClickHouse() error = %v", err)
	}
	if err := store.ValidateBackfill(context.Background()); err == nil {
		t.Fatal("ValidateBackfill() accepted table outside closed allowlist")
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want 0", requests)
	}
}

func TestExecuteProjectionRangeUsesOneBoundedRequest(t *testing.T) {
	requests := 0
	var body string
	client := &ch.Client{WriterAdmission: fixtureAdmission{},
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body = string(payload)
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("UNKNOWN_STATUS")),
				Request:    request,
			}, nil
		})},
	}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatalf("NewClickHouse() error = %v", err)
	}
	entry := nlkbackfill.RawEntry{
		SnapshotDate:    time.Date(2026, 5, 29, 0, 0, 0, 0, util.KST()),
		DatasetName:     "person",
		SourceArchive:   "person.zip",
		SourceEntry:     "person.rdf",
		NextRecordIndex: 100_000,
	}
	err = store.ExecuteProjectionRange(
		context.Background(),
		nlkbackfill.ProjectionAuthority,
		nlkbackfill.DefaultTransformVersion,
		entry,
		nlkbackfill.RecordRange{Start: 0, End: 50_000},
	)
	if err == nil || err.(*StoreError).Category != "unknown_status" {
		t.Fatalf("ExecuteProjectionRange() error = %v", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want exactly 1", requests)
	}
	for _, fragment := range []string{
		"raw.dataset_snapshot_date = toDate('2026-05-29')",
		"raw.dataset_name = 'person'",
		"raw.source_archive = 'person.zip'",
		"raw.source_entry = 'person.rdf'",
		"raw.source_record_index >= 0",
		"raw.source_record_index < 50000",
		"max_threads = 1",
		"parallel_view_processing = 0",
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("request missing %q:\n%s", fragment, body)
		}
	}
}

func TestProjectionRangeCancellationStopsAdmittedRequestWithoutRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := 0
	started := make(chan struct{})
	client := &ch.Client{WriterAdmission: fixtureAdmission{}, Host: "http://database.test", HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		close(started)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() {
		completed <- store.ExecuteProjectionRange(ctx, nlkbackfill.ProjectionAuthority, nlkbackfill.DefaultTransformVersion, nlkbackfill.RawEntry{SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, util.KST()), DatasetName: "person", SourceArchive: "person.zip", SourceEntry: "person.rdf", NextRecordIndex: 100}, nlkbackfill.RecordRange{Start: 0, End: 100})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request not admitted")
	}
	cancel()
	select {
	case err := <-completed:
		if err == nil {
			t.Fatal("canceled projection reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("projection ignored runner cancellation")
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want one attempt", requests)
	}
}
