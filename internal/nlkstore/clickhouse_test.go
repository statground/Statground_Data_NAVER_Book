package nlkstore

import (
	"reflect"
	"strings"
	"testing"

	"statground_naver_book_go/internal/ch"
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
	} {
		t.Setenv(name, "")
	}
	config := ConfigFromEnv()
	want := map[string]string{
		"raw":               "Data_Book_NLK_Raw.nlk_resource_raw",
		"raw_local":         "Data_Book_NLK_Raw.nlk_resource_raw_local",
		"run":               "Data_Book_NLK_Log.nlk_import_run_log",
		"run_local":         "Data_Book_NLK_Log.nlk_import_run_log_local",
		"checkpoint":        "Data_Book_NLK_Log.nlk_import_entry_checkpoint",
		"checkpoint_local":  "Data_Book_NLK_Log.nlk_import_entry_checkpoint_local",
		"run_latest":        "Data_Book_NLK_Log.v_nlk_import_run_latest",
		"checkpoint_latest": "Data_Book_NLK_Log.v_nlk_import_entry_checkpoint_latest",
	}
	got := map[string]string{
		"raw":               config.RawTable,
		"raw_local":         config.RawLocalTable,
		"run":               config.RunTable,
		"run_local":         config.RunLocalTable,
		"checkpoint":        config.CheckpointTable,
		"checkpoint_local":  config.CheckpointLocal,
		"run_latest":        config.RunLatestView,
		"checkpoint_latest": config.CheckpointLatestView,
	}
	for key, expected := range want {
		if got[key] != expected {
			t.Fatalf("%s=%q want=%q", key, got[key], expected)
		}
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

func TestBoundedErrorRemovesWhitespaceAndLimitsBytes(t *testing.T) {
	got := boundedError(strings.Repeat("secret-like-value \n", 40))
	if strings.ContainsAny(got, "\r\n\t") || len(got) > 256 {
		t.Fatalf("unsafe bounded error=%q bytes=%d", got, len(got))
	}
}
