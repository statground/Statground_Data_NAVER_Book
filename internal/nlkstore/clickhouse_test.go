package nlkstore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/nlkimport"
)

func TestConfigDefaultsMatchNLKSQLContract(t *testing.T) {
	for _, name := range []string{
		"NLK_RAW_TABLE",
		"NLK_RAW_LOCAL_TABLE",
		"NLK_IMPORT_RUN_TABLE",
		"NLK_IMPORT_RUN_LOCAL_TABLE",
		"NLK_IMPORT_CHECKPOINT_TABLE",
		"NLK_IMPORT_CHECKPOINT_LOCAL_TABLE",
		"NLK_IMPORT_RUN_LATEST_VIEW",
		"NLK_IMPORT_CHECKPOINT_LATEST_VIEW",
		"NLK_SERVICE_CHECKPOINT_TABLE",
		"NLK_SERVICE_CHECKPOINT_LOCAL_TABLE",
		"NLK_SERVICE_CHECKPOINT_LATEST_VIEW",
	} {
		t.Setenv(name, "")
	}
	config := ConfigFromEnv()
	want := map[string]string{
		"raw":                       "Data_Book_NLK_Raw.nlk_resource_raw",
		"raw_local":                 "Data_Book_NLK_Raw.nlk_resource_raw_local",
		"run":                       "Data_Book_NLK_Log.nlk_import_run_log",
		"run_local":                 "Data_Book_NLK_Log.nlk_import_run_log_local",
		"checkpoint":                "Data_Book_NLK_Log.nlk_import_entry_checkpoint",
		"checkpoint_local":          "Data_Book_NLK_Log.nlk_import_entry_checkpoint_local",
		"run_latest":                "Data_Book_NLK_Log.v_nlk_import_run_latest",
		"checkpoint_latest":         "Data_Book_NLK_Log.v_nlk_import_entry_checkpoint_latest",
		"service_checkpoint":        "Data_Book_NLK_Log.nlk_service_projection_checkpoint",
		"service_checkpoint_local":  "Data_Book_NLK_Log.nlk_service_projection_checkpoint_local",
		"service_checkpoint_latest": "Data_Book_NLK_Log.v_nlk_service_projection_checkpoint_latest",
	}
	got := map[string]string{
		"raw":                       config.RawTable,
		"raw_local":                 config.RawLocalTable,
		"run":                       config.RunTable,
		"run_local":                 config.RunLocalTable,
		"checkpoint":                config.CheckpointTable,
		"checkpoint_local":          config.CheckpointLocal,
		"run_latest":                config.RunLatestView,
		"checkpoint_latest":         config.CheckpointLatestView,
		"service_checkpoint":        config.ServiceCheckpointTable,
		"service_checkpoint_local":  config.ServiceCheckpointLocal,
		"service_checkpoint_latest": config.ServiceCheckpointLatestView,
	}
	for key, expected := range want {
		if got[key] != expected {
			t.Fatalf("%s=%q want=%q", key, got[key], expected)
		}
	}
}

func TestExistingRawRecordIndexesUsesFullSortedLineagePrefix(t *testing.T) {
	var query string
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			query = string(payload)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    request,
			}, nil
		})},
	}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ExistingRawRecordIndexes(context.Background(), nlkimport.RawLineage{
		SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
		DatasetName:  "book",
		Archive:      "book_rdf_20260529.zip",
		Entry:        "book_rdf_20260529/book_0.rdf",
	}, []uint64{1010, 1011})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"dataset_snapshot_date = toDate('2026-05-29')",
		"dataset_name = 'book'",
		"source_archive = 'book_rdf_20260529.zip'",
		"source_entry = 'book_rdf_20260529/book_0.rdf'",
		"source_record_index IN (1010, 1011)",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("query missing %q: %s", expected, query)
		}
	}
}

func TestExistingRawRecordIndexesChunksLargeResumeRangeAndUnionsResults(t *testing.T) {
	indexes := make([]uint64, existingRawIndexLookupChunkSize*10+3)
	for index := range indexes {
		indexes[index] = uint64(len(indexes) - index + 9)
	}

	var querySizes []int
	expected := make(map[uint64]struct{})
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			query := string(payload)
			const marker = "source_record_index IN ("
			start := strings.Index(query, marker)
			if start < 0 {
				t.Fatalf("query missing IN predicate: %s", query)
			}
			start += len(marker)
			end := strings.Index(query[start:], ")")
			if end < 0 {
				t.Fatalf("query has unterminated IN predicate: %s", query)
			}
			values := strings.Split(query[start:start+end], ", ")
			if len(values) == 0 || len(values) > existingRawIndexLookupChunkSize {
				t.Fatalf("query values=%d, chunk limit=%d", len(values), existingRawIndexLookupChunkSize)
			}
			querySizes = append(querySizes, len(values))
			existing, err := strconv.ParseUint(values[0], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			expected[existing] = struct{}{}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(fmt.Sprintf(
					"{\"source_record_index\":%d}\n",
					existing,
				))),
				Request: request,
			}, nil
		})},
	}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.ExistingRawRecordIndexes(context.Background(), nlkimport.RawLineage{
		SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
		DatasetName:  "book",
		Archive:      "book_rdf_20260529.zip",
		Entry:        "book_rdf_20260529/book_0.rdf",
	}, indexes)
	if err != nil {
		t.Fatal(err)
	}

	wantQuerySizes := []int{
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		existingRawIndexLookupChunkSize,
		3,
	}
	if !reflect.DeepEqual(querySizes, wantQuerySizes) {
		t.Fatalf("query sizes=%v want=%v", querySizes, wantQuerySizes)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("existing indexes=%v want=%v", got, expected)
	}
}

func TestRawLineageDedupTokenIgnoresVolatileObservationFields(t *testing.T) {
	first := []map[string]any{
		{
			"dataset_snapshot_date": "2026-05-29",
			"source_archive":        "book_rdf_20260529.zip",
			"source_entry":          "book_rdf_20260529/book_0.rdf",
			"source_record_index":   uint64(10),
			"uuid":                  "019d-old",
			"run_uuid":              "019d-run-old",
			"imported_at":           "2026-07-26 12:00:00.000",
		},
		{
			"dataset_snapshot_date": "2026-05-29",
			"source_archive":        "book_rdf_20260529.zip",
			"source_entry":          "book_rdf_20260529/book_0.rdf",
			"source_record_index":   uint64(11),
			"uuid":                  "019d-old-2",
		},
	}
	second := []map[string]any{
		{
			"dataset_snapshot_date": "2026-05-29",
			"source_archive":        "book_rdf_20260529.zip",
			"source_entry":          "book_rdf_20260529/book_0.rdf",
			"source_record_index":   uint64(11),
			"uuid":                  "019d-new-2",
		},
		{
			"dataset_snapshot_date": "2026-05-29",
			"source_archive":        "book_rdf_20260529.zip",
			"source_entry":          "book_rdf_20260529/book_0.rdf",
			"source_record_index":   uint64(10),
			"uuid":                  "019d-new",
			"run_uuid":              "019d-run-new",
			"imported_at":           "2026-07-27 12:00:00.000",
		},
	}
	token1, err := rawLineageDedupToken("Data_Book_NLK_Raw.nlk_resource_raw", first)
	if err != nil {
		t.Fatal(err)
	}
	token2, err := rawLineageDedupToken("Data_Book_NLK_Raw.nlk_resource_raw", second)
	if err != nil {
		t.Fatal(err)
	}
	if token1 != token2 || len(token1) != 64 {
		t.Fatalf("lineage tokens differ: %q/%q", token1, token2)
	}
}

func TestUniqueSortedIndexes(t *testing.T) {
	got := uniqueSortedIndexes([]uint64{9, 2, 9, 4, 2})
	want := []uint64{2, 4, 9}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("indexes=%v want=%v", got, want)
	}
}

func TestNewClickHouseRejectsUnqualifiedTable(t *testing.T) {
	config := ConfigFromEnv()
	config.RawTable = "nlk_resource_raw"
	if _, err := NewClickHouse(&ch.Client{}, config); err == nil {
		t.Fatal("expected unqualified table rejection")
	}
}

func TestValidateUsesExactExistsTablePreflight(t *testing.T) {
	var existsQueries int
	client := &ch.Client{
		Host:     "clickhouse.example.invalid",
		Protocol: "https",
		Database: "Data_Book_NLK_Raw",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			query := string(payload)
			responseBody := ""
			if strings.Contains(query, "EXISTS TABLE") {
				existsQueries++
				responseBody = "{\"result\":1}\n"
			}
			if strings.Contains(query, "system.tables") {
				t.Fatalf("Validate queried system.tables: %s", query)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Validate(context.Background()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if existsQueries != 8 {
		t.Fatalf("EXISTS TABLE queries = %d, want 8", existsQueries)
	}
}

func TestBoundedErrorRemovesWhitespaceAndLimitsBytes(t *testing.T) {
	got := boundedError(strings.Repeat("secret-like-value \n", 40))
	if strings.ContainsAny(got, "\r\n\t") || len(got) > 256 {
		t.Fatalf("unsafe bounded error=%q bytes=%d", got, len(got))
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
