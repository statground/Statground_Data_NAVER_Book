package nlkimport

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

type fakeStore struct {
	validateCalls  int
	checkpoint     Checkpoint
	hasCheckpoint  bool
	rawBatches     [][]map[string]any
	runs           []RunState
	checkpoints    []Checkpoint
	existing       map[uint64]struct{}
	existingChecks int
	insertError    error
}

func (s *fakeStore) Validate(context.Context) error {
	s.validateCalls++
	return nil
}

func (s *fakeStore) LoadCheckpoint(context.Context, CheckpointKey) (Checkpoint, bool, error) {
	return s.checkpoint, s.hasCheckpoint, nil
}

func (s *fakeStore) InsertRawRows(_ context.Context, rows []map[string]any) error {
	if s.insertError != nil {
		return s.insertError
	}
	copied := make([]map[string]any, len(rows))
	for index, row := range rows {
		copied[index] = make(map[string]any, len(row))
		for key, value := range row {
			copied[index][key] = value
		}
	}
	s.rawBatches = append(s.rawBatches, copied)
	return nil
}

func TestImporterFailureCheckpointDoesNotAdvanceUncommittedCounters(t *testing.T) {
	inputDir := t.TempDir()
	writeSyntheticArchive(t, inputDir)
	store := &fakeStore{insertError: errors.New("injected")}
	importer := Importer{Store: store, IDGenerator: incrementingID()}
	_, err := importer.Run(context.Background(), Config{
		InputDir:     inputDir,
		Datasets:     []string{"book"},
		SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
		BatchSize:    1,
		Resume:       true,
	})
	if ErrorCategory(err) != "raw_insert_failed" {
		t.Fatalf("error=%v", err)
	}
	last := store.checkpoints[len(store.checkpoints)-1]
	if last.Status != "failed" ||
		last.NextRecordIndex != 0 ||
		last.RecordsParsed != 0 ||
		last.RecordsInserted != 0 ||
		last.RecordsRejected != 0 {
		t.Fatalf("failure checkpoint advanced uncommitted state: %+v", last)
	}
}

func (s *fakeStore) ExistingRawRecordIndexes(_ context.Context, _ RawLineage, indexes []uint64) (map[uint64]struct{}, error) {
	s.existingChecks++
	out := make(map[uint64]struct{})
	for _, index := range indexes {
		if _, found := s.existing[index]; found {
			out[index] = struct{}{}
		}
	}
	return out, nil
}

func (s *fakeStore) SaveRun(_ context.Context, state RunState) error {
	s.runs = append(s.runs, state)
	return nil
}

func (s *fakeStore) SaveCheckpoint(_ context.Context, state Checkpoint) error {
	s.checkpoints = append(s.checkpoints, state)
	return nil
}

func TestImporterStreamsSyntheticZIPInBoundedBatches(t *testing.T) {
	inputDir := t.TempDir()
	writeSyntheticArchive(t, inputDir)
	store := &fakeStore{}
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.FixedZone("Asia/Seoul", 9*60*60))
	importer := Importer{
		Store:       store,
		IDGenerator: incrementingID(),
		Now:         func() time.Time { return now },
	}
	result, err := importer.Run(context.Background(), Config{
		InputDir:        inputDir,
		Datasets:        []string{"book"},
		SnapshotDate:    time.Date(2026, 5, 29, 0, 0, 0, 0, now.Location()),
		BatchSize:       2,
		Resume:          true,
		DryRun:          false,
		ImporterVersion: "test",
	})
	if err != nil {
		t.Fatalf("Run() error=%v", err)
	}
	if store.validateCalls != 1 {
		t.Fatalf("validate calls=%d", store.validateCalls)
	}
	if store.existingChecks != 0 {
		t.Fatalf("fresh import must not issue recovery lookups: %d", store.existingChecks)
	}
	if result.RecordsParsed != 3 || result.RecordsInserted != 3 || result.RecordsRejected != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.ArchivesCompleted != 1 || result.EntriesCompleted != 1 || result.Limited {
		t.Fatalf("unexpected completion: %+v", result)
	}
	if len(store.rawBatches) != 2 || len(store.rawBatches[0]) != 2 || len(store.rawBatches[1]) != 1 {
		t.Fatalf("raw batch shape=%v", batchLengths(store.rawBatches))
	}
	if len(store.checkpoints) != 4 {
		t.Fatalf("checkpoint writes=%d want=4 (start, one per committed batch, success)", len(store.checkpoints))
	}
	finalCheckpoint := store.checkpoints[len(store.checkpoints)-1]
	if finalCheckpoint.Status != "succeeded" || finalCheckpoint.NextRecordIndex != 3 ||
		finalCheckpoint.RecordsInserted != 3 || len(finalCheckpoint.ContentHash) != 64 {
		t.Fatalf("unexpected final checkpoint: %+v", finalCheckpoint)
	}
	finalRun := store.runs[len(store.runs)-1]
	if finalRun.Status != "succeeded" || finalRun.RecordsInserted != 3 ||
		finalRun.EntriesCompleted != 1 || finalRun.ArchivesCompleted != 1 {
		t.Fatalf("unexpected final run: %+v", finalRun)
	}
}

func TestImporterFlushesAtEstimatedByteLimit(t *testing.T) {
	inputDir := t.TempDir()
	writeSyntheticArchive(t, inputDir)
	store := &fakeStore{}
	importer := Importer{Store: store, IDGenerator: incrementingID()}
	result, err := importer.Run(context.Background(), Config{
		InputDir:       inputDir,
		Datasets:       []string{"book"},
		SnapshotDate:   time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
		BatchSize:      50000,
		BatchByteLimit: 1,
		Resume:         true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordsInserted != 3 {
		t.Fatalf("result=%+v", result)
	}
	if got := batchLengths(store.rawBatches); len(got) != 3 || got[0] != 1 || got[1] != 1 || got[2] != 1 {
		t.Fatalf("byte-limited raw batch shape=%v", got)
	}
}

func TestImporterResumeFiltersRawRowsConfirmedBeforeCheckpointAdvance(t *testing.T) {
	inputDir := t.TempDir()
	writeSyntheticArchive(t, inputDir)
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.FixedZone("Asia/Seoul", 9*60*60))
	store := &fakeStore{
		hasCheckpoint: true,
		existing:      map[uint64]struct{}{1: {}},
		checkpoint: Checkpoint{
			DatasetName:       "book",
			EntryCRC32:        archiveCRC32(t, inputDir),
			EntryUncompressed: archiveUncompressedSize(t, inputDir),
			Status:            "failed",
			NextRecordIndex:   1,
			RecordsParsed:     3,
			RecordsInserted:   1,
			Attempts:          1,
			Version:           10,
		},
	}
	importer := Importer{Store: store, IDGenerator: incrementingID(), Now: func() time.Time { return now }}
	result, err := importer.Run(context.Background(), Config{
		InputDir:     inputDir,
		Datasets:     []string{"book"},
		SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, now.Location()),
		BatchSize:    1,
		Resume:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordsParsed != 2 || result.RecordsInserted != 1 {
		t.Fatalf("resume result=%+v", result)
	}
	var indexes []uint64
	for _, batch := range store.rawBatches {
		for _, row := range batch {
			indexes = append(indexes, row["source_record_index"].(uint64))
		}
	}
	if len(indexes) != 1 || indexes[0] != 2 {
		t.Fatalf("resumed indexes=%v", indexes)
	}
	if store.existingChecks != 1 {
		t.Fatalf("existing lineage checks=%d want=1", store.existingChecks)
	}
	final := store.checkpoints[len(store.checkpoints)-1]
	if final.Attempts != 2 || final.RecordsParsed != 3 || final.RecordsInserted != 3 {
		t.Fatalf("resumed checkpoint=%+v", final)
	}
}

func TestImporterSkipsSucceededEntryOnResume(t *testing.T) {
	inputDir := t.TempDir()
	writeSyntheticArchive(t, inputDir)
	store := &fakeStore{
		hasCheckpoint: true,
		checkpoint: Checkpoint{
			DatasetName:       "book",
			EntryCRC32:        archiveCRC32(t, inputDir),
			EntryUncompressed: archiveUncompressedSize(t, inputDir),
			Status:            "succeeded",
			NextRecordIndex:   3,
		},
	}
	importer := Importer{Store: store, IDGenerator: incrementingID()}
	result, err := importer.Run(context.Background(), Config{
		InputDir:     inputDir,
		Datasets:     []string{"book"},
		SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
		Resume:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordsParsed != 0 || result.RecordsInserted != 0 ||
		result.EntriesCompleted != 1 || result.ArchivesCompleted != 1 {
		t.Fatalf("skip result=%+v", result)
	}
	if len(store.rawBatches) != 0 {
		t.Fatalf("unexpected raw writes=%d", len(store.rawBatches))
	}
}

func TestImporterDryRunHonorsMaxRecordsWithoutStoreWrites(t *testing.T) {
	inputDir := t.TempDir()
	writeSyntheticArchive(t, inputDir)
	importer := Importer{IDGenerator: incrementingID()}
	result, err := importer.Run(context.Background(), Config{
		InputDir:     inputDir,
		Datasets:     []string{"book"},
		SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
		BatchSize:    1,
		DryRun:       true,
		MaxRecords:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Limited || result.RecordsParsed != 2 || result.RecordsInserted != 0 {
		t.Fatalf("dry-run limited result=%+v", result)
	}
	if result.EntriesCompleted != 0 || result.ArchivesCompleted != 0 {
		t.Fatalf("partial dry-run must not report completed entry: %+v", result)
	}
}

func TestImporterRejectsUnboundedBatchMemory(t *testing.T) {
	importer := Importer{}
	_, err := importer.Run(context.Background(), Config{
		BatchSize:      maxBatchSize,
		BatchByteLimit: maxBatchByteLimit + 1,
		DryRun:         true,
	})
	if ErrorCategory(err) != "invalid_batch_byte_limit" {
		t.Fatalf("error=%v", err)
	}
}

func TestImporterRejectsInvalidEntryShardContract(t *testing.T) {
	tests := []Config{
		{EntryShardCount: -1, EntryShardIndex: 0, DryRun: true},
		{EntryShardCount: MaxEntryShardCount + 1, EntryShardIndex: 0, DryRun: true},
		{EntryShardCount: 2, EntryShardIndex: -1, DryRun: true},
		{EntryShardCount: 2, EntryShardIndex: 2, DryRun: true},
	}
	for _, config := range tests {
		_, err := (&Importer{}).Run(context.Background(), config)
		if ErrorCategory(err) != "invalid_entry_shard" {
			t.Fatalf("config=%+v error=%v", config, err)
		}
	}
}

func TestEntryShardPartitionHasStableCompleteUnionWithoutOverlap(t *testing.T) {
	inputDir := t.TempDir()
	entryNames := make([]string, 37)
	for index := range entryNames {
		entryNames[index] = fmt.Sprintf("book_rdf_20260529/chunk_%03d.rdf", index)
	}
	writeSyntheticArchiveEntries(t, inputDir, entryNames)
	plans, err := discoverArchives(
		inputDir,
		[]string{"book"},
		time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}

	const shardCount = 7
	seen := make(map[string]int, len(entryNames))
	for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
		first := selectEntryShard(plans, shardCount, shardIndex)
		second := selectEntryShard(plans, shardCount, shardIndex)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("shard %d assignment changed between calls", shardIndex)
		}
		for _, plan := range first {
			if !sort.SliceIsSorted(plan.Entries, func(left, right int) bool {
				return plan.Entries[left].Name < plan.Entries[right].Name
			}) {
				t.Fatalf("shard %d entry order is unstable: %+v", shardIndex, plan.Entries)
			}
			var assignedCompressedBytes uint64
			var assignedUncompressedBytes uint64
			for _, entry := range plan.Entries {
				if got := entryShard(plan.Dataset, plan.BaseName, entry.Name, shardCount); got != shardIndex {
					t.Fatalf("entry %q assigned to shard %d, selected by %d", entry.Name, got, shardIndex)
				}
				seen[entry.Name]++
				assignedCompressedBytes += entry.CompressedBytes
				assignedUncompressedBytes += entry.UncompressedBytes
			}
			if plan.CompressedBytes != assignedCompressedBytes ||
				plan.UncompressedBytes != assignedUncompressedBytes {
				t.Fatalf(
					"shard %d bytes=(%d,%d) want=(%d,%d)",
					shardIndex,
					plan.CompressedBytes,
					plan.UncompressedBytes,
					assignedCompressedBytes,
					assignedUncompressedBytes,
				)
			}
		}
	}
	if len(seen) != len(entryNames) {
		t.Fatalf("union has %d entries, want %d", len(seen), len(entryNames))
	}
	for _, name := range entryNames {
		if seen[name] != 1 {
			t.Fatalf("entry %q selected %d times", name, seen[name])
		}
	}
}

func TestEntryShardDefaultIsUnchangedAndRunTotalsDescribeAssignedSubset(t *testing.T) {
	inputDir := t.TempDir()
	entryNames := []string{
		"book_rdf_20260529/part_0.rdf",
		"book_rdf_20260529/part_1.rdf",
		"book_rdf_20260529/part_2.rdf",
		"book_rdf_20260529/part_3.rdf",
		"book_rdf_20260529/part_4.rdf",
		"book_rdf_20260529/part_5.rdf",
		"book_rdf_20260529/part_6.rdf",
		"book_rdf_20260529/part_7.rdf",
	}
	writeSyntheticArchiveEntries(t, inputDir, entryNames)
	snapshot := time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC)
	plans, err := discoverArchives(inputDir, []string{"book"}, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := selectEntryShard(plans, 1, 0); !reflect.DeepEqual(got, plans) {
		t.Fatalf("default shard changed plans: got=%+v want=%+v", got, plans)
	}

	const shardCount = 4
	var totalEntries uint32
	var totalRecords uint64
	for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
		assigned := selectEntryShard(plans, shardCount, shardIndex)
		var expectedEntries uint32
		for _, plan := range assigned {
			expectedEntries += uint32(len(plan.Entries))
		}
		result, runErr := (&Importer{IDGenerator: incrementingID()}).Run(context.Background(), Config{
			InputDir:        inputDir,
			Datasets:        []string{"book"},
			SnapshotDate:    snapshot,
			EntryShardCount: shardCount,
			EntryShardIndex: shardIndex,
			DryRun:          true,
		})
		if runErr != nil {
			t.Fatalf("shard %d: %v", shardIndex, runErr)
		}
		if result.EntriesTotal != expectedEntries ||
			result.EntriesCompleted != expectedEntries ||
			result.RecordsParsed != uint64(expectedEntries)*3 {
			t.Fatalf("shard %d result=%+v expected_entries=%d", shardIndex, result, expectedEntries)
		}
		expectedArchives := uint16(0)
		if expectedEntries > 0 {
			expectedArchives = 1
		}
		if result.ArchivesTotal != expectedArchives || result.ArchivesCompleted != expectedArchives {
			t.Fatalf("shard %d archive totals=%+v expected=%d", shardIndex, result, expectedArchives)
		}
		totalEntries += result.EntriesTotal
		totalRecords += result.RecordsParsed
	}
	if totalEntries != uint32(len(entryNames)) || totalRecords != uint64(len(entryNames))*3 {
		t.Fatalf("union totals entries=%d records=%d", totalEntries, totalRecords)
	}
}

func TestNormalizeDatasetsKeepsSafeDefaultsAndAliases(t *testing.T) {
	got, err := NormalizeDatasets([]string{"book", "Concept", "PERSON", "Library", "book", "govermentpublication"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"book", "concept", "person", "library", "government_publication"}
	if len(got) != len(want) {
		t.Fatalf("datasets=%v", got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("datasets=%v want=%v", got, want)
		}
	}
}

func TestDiscoverArchivesNormalizesDatasetButPreservesOfficialArchiveName(t *testing.T) {
	tests := []struct {
		input       string
		archiveName string
		wantDataset string
	}{
		{input: "PERSON", archiveName: "Person_rdf_20260529.zip", wantDataset: "person"},
		{
			input:       "governmentpublication",
			archiveName: "govermentpublication_rdf_20260529.zip",
			wantDataset: "government_publication",
		},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			inputDir := t.TempDir()
			writeSyntheticArchive(t, inputDir)
			if err := os.Rename(
				filepath.Join(inputDir, "book_rdf_20260529.zip"),
				filepath.Join(inputDir, test.archiveName),
			); err != nil {
				t.Fatal(err)
			}
			plans, err := discoverArchives(
				inputDir,
				[]string{test.input},
				time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(plans) != 1 ||
				plans[0].Dataset != test.wantDataset ||
				plans[0].BaseName != test.archiveName {
				t.Fatalf("plans=%+v", plans)
			}
		})
	}
}

func writeSyntheticArchive(t *testing.T, directory string) {
	t.Helper()
	writeSyntheticArchiveEntries(t, directory, []string{"book_rdf_20260529/book_0.rdf"})
}

func writeSyntheticArchiveEntries(t *testing.T, directory string, entryNames []string) {
	t.Helper()
	rdf, err := os.ReadFile(filepath.Join("..", "nlklod", "testdata", "sample.rdf"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(directory, "book_rdf_20260529.zip"))
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	for _, entryName := range entryNames {
		entry, createErr := writer.Create(entryName)
		if createErr != nil {
			t.Fatal(createErr)
		}
		if _, writeErr := entry.Write(rdf); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func archiveCRC32(t *testing.T, directory string) string {
	t.Helper()
	reader, err := zip.OpenReader(filepath.Join(directory, "book_rdf_20260529.zip"))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	return fmt.Sprintf("%08x", reader.File[0].CRC32)
}

func archiveUncompressedSize(t *testing.T, directory string) uint64 {
	t.Helper()
	reader, err := zip.OpenReader(filepath.Join(directory, "book_rdf_20260529.zip"))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	return reader.File[0].UncompressedSize64
}

func incrementingID() func() string {
	counter := 0
	return func() string {
		counter++
		return "019d0000-0000-7000-8000-" + leftPad(counter)
	}
}

func leftPad(value int) string {
	const digits = "000000000000"
	text := []byte(digits)
	for index := len(text) - 1; value > 0 && index >= 0; index-- {
		text[index] = byte('0' + value%10)
		value /= 10
	}
	return string(text)
}

func batchLengths(batches [][]map[string]any) []int {
	out := make([]int, 0, len(batches))
	for _, batch := range batches {
		out = append(out, len(batch))
	}
	return out
}
