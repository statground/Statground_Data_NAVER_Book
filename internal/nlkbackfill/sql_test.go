package nlkbackfill

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/util"
)

func TestProjectionSQLUsesBoundedPrimaryKeyPredicatesAndSerialSettings(t *testing.T) {
	recordRange := RecordRange{Start: 50_000, End: 100_000}
	for _, projection := range DefaultProjections() {
		t.Run(string(projection), func(t *testing.T) {
			dataset := "book"
			if projection == ProjectionAuthority {
				dataset = "person"
			}
			if projection == ProjectionLibrary {
				dataset = "library"
			}
			entry := RawEntry{
				SnapshotDate:    time.Date(2026, 5, 29, 0, 0, 0, 0, util.KST()),
				DatasetName:     dataset,
				SourceArchive:   "NLK book's archive.zip",
				SourceEntry:     "nested/book.rdf",
				NextRecordIndex: 100_000,
			}
			target, _ := ProjectionTarget(projection)
			query, err := BuildProjectionSQL(
				projection,
				DefaultTransformVersion,
				entry,
				recordRange,
				DefaultRawTable,
				target,
			)
			if err != nil {
				t.Fatalf("BuildProjectionSQL() error = %v", err)
			}
			expected := []string{
				"FROM " + DefaultRawTable + " AS raw",
				"raw.dataset_snapshot_date = toDate('2026-05-29')",
				"raw.dataset_name = '" + dataset + "'",
				"raw.source_archive = 'NLK book''s archive.zip'",
				"raw.source_entry = 'nested/book.rdf'",
				"raw.source_record_index >= 50000",
				"raw.source_record_index < 100000",
				"max_threads = 1",
				"parallel_view_processing = 0",
				"parallel_distributed_insert_select = 0",
				"distributed_foreground_insert = 1",
				"insert_quorum = 2",
			}
			for _, fragment := range expected {
				if !strings.Contains(query, fragment) {
					t.Fatalf("query missing %q:\n%s", fragment, query)
				}
			}
			for _, forbidden := range []string{"cityHash64(", "% {", "source_record_index IN"} {
				if strings.Contains(query, forbidden) {
					t.Fatalf("query contains unsafe unbounded/hash predicate %q:\n%s", forbidden, query)
				}
			}
			if !strings.HasPrefix(query, "INSERT INTO "+target) {
				t.Fatalf("query target mismatch: %s", query)
			}
		})
	}
}

func TestProjectionSQLRejectsTablesOutsideClosedAllowlist(t *testing.T) {
	entry := RawEntry{
		SnapshotDate:    time.Date(2026, 5, 29, 0, 0, 0, 0, util.KST()),
		DatasetName:     "person",
		SourceArchive:   "book.zip",
		SourceEntry:     "book.rdf",
		NextRecordIndex: 10,
	}
	tests := []struct {
		raw    string
		target string
	}{
		{raw: "attacker.raw", target: projectionTargets[ProjectionAuthority]},
		{raw: DefaultRawTable, target: "attacker.target"},
	}
	for index, test := range tests {
		t.Run(fmt.Sprintf("case_%d", index), func(t *testing.T) {
			if _, err := BuildProjectionSQL(
				ProjectionAuthority,
				DefaultTransformVersion,
				entry,
				RecordRange{Start: 0, End: 10},
				test.raw,
				test.target,
			); err == nil {
				t.Fatal("BuildProjectionSQL() accepted table outside allowlist")
			}
		})
	}
}
