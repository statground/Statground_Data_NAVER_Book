package nlkbackfill

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"statground_naver_book_go/internal/util"
)

type execution struct {
	projection  Projection
	dataset     string
	recordRange RecordRange
}

type fakeStore struct {
	entries       []RawEntry
	checkpoint    Checkpoint
	hasCheckpoint bool
	executions    []execution
	saves         []Checkpoint
	executeErr    error
}

func (s *fakeStore) ValidateBackfill(context.Context) error {
	return nil
}

func (s *fakeStore) ListSucceededRawEntries(context.Context, time.Time) ([]RawEntry, error) {
	return append([]RawEntry(nil), s.entries...), nil
}

func (s *fakeStore) LoadProjectionCheckpoint(
	context.Context,
	CheckpointKey,
) (Checkpoint, bool, error) {
	return s.checkpoint, s.hasCheckpoint, nil
}

func (s *fakeStore) ExecuteProjectionRange(
	_ context.Context,
	projection Projection,
	_ string,
	entry RawEntry,
	recordRange RecordRange,
) error {
	s.executions = append(s.executions, execution{
		projection:  projection,
		dataset:     entry.DatasetName,
		recordRange: recordRange,
	})
	return s.executeErr
}

func (s *fakeStore) SaveProjectionCheckpoint(_ context.Context, checkpoint Checkpoint) error {
	s.saves = append(s.saves, checkpoint)
	return nil
}

func TestPlanRangesHasNoGapOrOverlap(t *testing.T) {
	ranges, err := PlanRanges(7, 120_011, 50_000)
	if err != nil {
		t.Fatalf("PlanRanges() error = %v", err)
	}
	want := []RecordRange{
		{Start: 7, End: 50_007},
		{Start: 50_007, End: 100_007},
		{Start: 100_007, End: 120_011},
	}
	if !reflect.DeepEqual(ranges, want) {
		t.Fatalf("ranges = %#v, want %#v", ranges, want)
	}
	if err := ValidateContiguousRanges(7, 120_011, ranges); err != nil {
		t.Fatalf("ValidateContiguousRanges() error = %v", err)
	}
}

func TestValidateContiguousRangesRejectsGapAndOverlap(t *testing.T) {
	tests := []struct {
		name   string
		ranges []RecordRange
	}{
		{
			name: "gap",
			ranges: []RecordRange{
				{Start: 0, End: 10},
				{Start: 11, End: 20},
			},
		},
		{
			name: "overlap",
			ranges: []RecordRange{
				{Start: 0, End: 11},
				{Start: 10, End: 20},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateContiguousRanges(0, 20, test.ranges); err == nil {
				t.Fatal("ValidateContiguousRanges() accepted unsafe ranges")
			}
		})
	}
}

func TestRunnerResumesAtDurableProjectionCheckpoint(t *testing.T) {
	snapshot := time.Date(2026, 5, 29, 0, 0, 0, 0, util.KST())
	entry := RawEntry{
		SnapshotDate:    snapshot,
		DatasetName:     "person",
		SourceArchive:   "person.zip",
		SourceEntry:     "person.rdf",
		NextRecordIndex: 120_000,
	}
	key := CheckpointKey{
		SnapshotDate:     snapshot,
		DatasetName:      entry.DatasetName,
		SourceArchive:    entry.SourceArchive,
		SourceEntry:      entry.SourceEntry,
		Projection:       ProjectionAuthority,
		TransformVersion: DefaultTransformVersion,
	}
	store := &fakeStore{
		entries: []RawEntry{entry},
		checkpoint: Checkpoint{
			CheckpointKey:   key,
			Status:          "failed",
			NextRecordIndex: 50_000,
			Attempts:        1,
		},
		hasCheckpoint: true,
	}
	now := time.Date(2026, 7, 29, 1, 2, 3, 0, util.KST())
	id := 0
	runner := Runner{
		Store: store,
		Now:   func() time.Time { return now },
		NewUUID: func() string {
			id++
			return fmt.Sprintf("00000000-0000-7000-8000-%012d", id)
		},
	}
	result, err := runner.Run(context.Background(), Config{
		SnapshotDate:     snapshot,
		RangeSize:        50_000,
		Projections:      []Projection{ProjectionAuthority},
		TransformVersion: DefaultTransformVersion,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	wantExecutions := []execution{
		{projection: ProjectionAuthority, dataset: "person", recordRange: RecordRange{Start: 50_000, End: 100_000}},
		{projection: ProjectionAuthority, dataset: "person", recordRange: RecordRange{Start: 100_000, End: 120_000}},
	}
	if !reflect.DeepEqual(store.executions, wantExecutions) {
		t.Fatalf("executions = %#v, want %#v", store.executions, wantExecutions)
	}
	if result.RangesCompleted != 2 || result.RecordsCovered != 70_000 {
		t.Fatalf("unexpected result: %+v", result)
	}
	final := store.saves[len(store.saves)-1]
	if final.Status != "succeeded" || final.NextRecordIndex != 120_000 {
		t.Fatalf("final checkpoint = %+v", final)
	}
}

func TestRunnerDoesNotAdvanceCheckpointOnAmbiguousError(t *testing.T) {
	snapshot := time.Date(2026, 5, 29, 0, 0, 0, 0, util.KST())
	store := &fakeStore{
		entries: []RawEntry{{
			SnapshotDate:    snapshot,
			DatasetName:     "person",
			SourceArchive:   "person.zip",
			SourceEntry:     "person.rdf",
			NextRecordIndex: 100_000,
		}},
		executeErr: errors.New("UNKNOWN_STATUS after timeout"),
	}
	runner := Runner{
		Store:   store,
		Now:     func() time.Time { return time.Date(2026, 7, 29, 1, 2, 3, 0, util.KST()) },
		NewUUID: func() string { return "00000000-0000-7000-8000-000000000001" },
	}
	_, err := runner.Run(context.Background(), Config{
		SnapshotDate: snapshot,
		Projections:  []Projection{ProjectionAuthority},
	})
	if ErrorCategory(err) != "unknown_status" {
		t.Fatalf("error category = %q, want unknown_status; err=%v", ErrorCategory(err), err)
	}
	if len(store.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(store.executions))
	}
	for _, saved := range store.saves {
		if saved.NextRecordIndex != 0 {
			t.Fatalf("checkpoint advanced after ambiguous error: %+v", saved)
		}
	}
	if final := store.saves[len(store.saves)-1]; final.Status != "failed" {
		t.Fatalf("final checkpoint status = %q, want failed", final.Status)
	}
}

func TestDefaultProjectionOrdering(t *testing.T) {
	want := []Projection{
		ProjectionAuthority,
		ProjectionBibliography,
		ProjectionLibrary,
		ProjectionProviderLatest,
		ProjectionBibliographyContext,
		ProjectionKDCSummary,
		ProjectionISBNAlias,
	}
	if got := DefaultProjections(); !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultProjections() = %#v, want %#v", got, want)
	}
	got := DefaultProjections()
	got[0] = ProjectionISBNAlias
	if DefaultProjections()[0] != ProjectionAuthority {
		t.Fatal("DefaultProjections() returned mutable package state")
	}
}

func TestProjectionDatasetRoutingMatchesNormalizedImporterContract(t *testing.T) {
	tests := []struct {
		projection Projection
		dataset    string
		want       bool
	}{
		{ProjectionAuthority, "person", true},
		{ProjectionAuthority, "concept", true},
		{ProjectionAuthority, "organization", false},
		{ProjectionAuthority, "book", false},
		{ProjectionLibrary, "library", true},
		{ProjectionLibrary, "book", false},
		{ProjectionBibliography, "book", true},
		{ProjectionProviderLatest, "offline", true},
		{ProjectionBibliographyContext, "online", true},
		{ProjectionKDCSummary, "audiovisual", true},
		{ProjectionISBNAlias, "government_publication", true},
		{ProjectionBibliography, "serial", true},
		{ProjectionBibliography, "thesis", true},
	}
	for _, test := range tests {
		t.Run(string(test.projection)+"/"+test.dataset, func(t *testing.T) {
			got, err := ProjectionAppliesToDataset(test.projection, test.dataset)
			if err != nil {
				t.Fatalf("ProjectionAppliesToDataset() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("ProjectionAppliesToDataset() = %t, want %t", got, test.want)
			}
		})
	}
	for _, dataset := range []string{"PERSON", "governmentpublication", "unknown"} {
		if _, err := ProjectionAppliesToDataset(ProjectionAuthority, dataset); err == nil {
			t.Fatalf("ProjectionAppliesToDataset() accepted non-canonical/unknown dataset %q", dataset)
		}
	}
}

func TestDefaultFourDatasetsSkipIrrelevantQueriesAndCountReadCoverage(t *testing.T) {
	snapshot := time.Date(2026, 5, 29, 0, 0, 0, 0, util.KST())
	store := &fakeStore{entries: []RawEntry{
		{
			SnapshotDate:    snapshot,
			DatasetName:     "book",
			SourceArchive:   "book.zip",
			SourceEntry:     "book.rdf",
			NextRecordIndex: 3_898_083,
		},
		{
			SnapshotDate:    snapshot,
			DatasetName:     "concept",
			SourceArchive:   "concept.zip",
			SourceEntry:     "concept.rdf",
			NextRecordIndex: 445_541,
		},
		{
			SnapshotDate:    snapshot,
			DatasetName:     "library",
			SourceArchive:   "library.zip",
			SourceEntry:     "library.rdf",
			NextRecordIndex: 14_054,
		},
		{
			SnapshotDate:    snapshot,
			DatasetName:     "person",
			SourceArchive:   "person.zip",
			SourceEntry:     "person.rdf",
			NextRecordIndex: 1_830_640,
		},
	}}
	id := 0
	runner := Runner{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 7, 29, 1, 2, 3, 0, util.KST()) },
		NewUUID: func() string {
			id++
			return fmt.Sprintf("00000000-0000-7000-8000-%012d", id)
		},
	}
	result, err := runner.Run(context.Background(), Config{SnapshotDate: snapshot})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	const (
		wantProjectionCount = 8
		wantSkippedCount    = 20
		wantRangeCount      = 437
		wantReadCoverage    = 21_780_650
	)
	if result.ProjectionsHandled != wantProjectionCount ||
		result.ProjectionsSkipped != wantSkippedCount ||
		result.RangesCompleted != wantRangeCount ||
		result.RecordsCovered != wantReadCoverage {
		t.Fatalf("result = %+v", result)
	}
	if len(store.executions) != wantRangeCount {
		t.Fatalf("executions = %d, want %d", len(store.executions), wantRangeCount)
	}
	for _, executed := range store.executions {
		applies, err := ProjectionAppliesToDataset(executed.projection, executed.dataset)
		if err != nil || !applies {
			t.Fatalf("irrelevant execution = %+v, applies=%t err=%v", executed, applies, err)
		}
	}
}
