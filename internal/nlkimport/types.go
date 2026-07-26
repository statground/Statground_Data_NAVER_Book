package nlkimport

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	InputDir        string
	Datasets        []string
	SnapshotDate    time.Time
	BatchSize       int
	Resume          bool
	DryRun          bool
	MaxRecords      uint64
	ImporterVersion string
	Source          string
}

type Result struct {
	RunUUID           string
	ArchivesTotal     uint16
	ArchivesCompleted uint16
	EntriesTotal      uint32
	EntriesCompleted  uint32
	RecordsParsed     uint64
	RecordsInserted   uint64
	RecordsRejected   uint64
	Limited           bool
}

type RunState struct {
	RunUUID             string
	Version             uint64
	Status              string
	DatasetSnapshotDate time.Time
	DatasetUpdatedAt    time.Time
	SourceURL           string
	LicenseName         string
	LicenseURL          string
	Attribution         string
	ArchivesTotal       uint16
	ArchivesCompleted   uint16
	EntriesTotal        uint32
	EntriesCompleted    uint32
	RecordsParsed       uint64
	RecordsInserted     uint64
	RecordsRejected     uint64
	BytesCompressed     uint64
	BytesUncompressed   uint64
	ImporterVersion     string
	ErrorCode           string
	ErrorMessage        string
	StartedAt           time.Time
	FinishedAt          *time.Time
	HeartbeatAt         time.Time
	Source              string
}

type CheckpointKey struct {
	SnapshotDate time.Time
	Archive      string
	Entry        string
}

type RawLineage struct {
	SnapshotDate time.Time
	Archive      string
	Entry        string
}

type Checkpoint struct {
	CheckpointUUID      string
	RunUUID             string
	Version             uint64
	DatasetSnapshotDate time.Time
	DatasetName         string
	SourceArchive       string
	SourceEntry         string
	EntryCRC32          string
	EntryUncompressed   uint64
	Status              string
	NextRecordIndex     uint64
	LastResourceID      string
	RecordsParsed       uint64
	RecordsInserted     uint64
	RecordsRejected     uint64
	Attempts            uint16
	ContentHash         string
	ErrorCode           string
	ErrorMessage        string
	StartedAt           *time.Time
	CompletedAt         *time.Time
	UpdatedAt           time.Time
	Source              string
}

type Store interface {
	Validate(context.Context) error
	LoadCheckpoint(context.Context, CheckpointKey) (Checkpoint, bool, error)
	ExistingRawRecordIndexes(context.Context, RawLineage, []uint64) (map[uint64]struct{}, error)
	InsertRawRows(context.Context, []map[string]any) error
	SaveRun(context.Context, RunState) error
	SaveCheckpoint(context.Context, Checkpoint) error
}

type SafeError struct {
	Category string
}

func (e *SafeError) Error() string {
	category := strings.TrimSpace(e.Category)
	if category == "" {
		category = "unknown"
	}
	return fmt.Sprintf("NLK LOD import failed category=%s", category)
}

func ErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	if safe, ok := err.(*SafeError); ok {
		return safe.Category
	}
	return "unknown"
}

func safeError(category string) error {
	return &SafeError{Category: category}
}
