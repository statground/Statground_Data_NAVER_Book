package nlkbackfill

import (
	"context"
	"fmt"
	"strings"
	"time"

	"statground_naver_book_go/internal/nlkimport"
)

const (
	DefaultRangeSize        uint64 = 50_000
	DefaultTransformVersion        = "nlk_service_projection_v1"
	MaxRangeSize            uint64 = 1_000_000
)

type Projection string

const (
	ProjectionAuthority           Projection = "authority"
	ProjectionBibliography        Projection = "bibliography"
	ProjectionLibrary             Projection = "library"
	ProjectionProviderLatest      Projection = "provider_latest"
	ProjectionBibliographyContext Projection = "bibliography_context"
	ProjectionKDCSummary          Projection = "kdc_summary"
	ProjectionISBNAlias           Projection = "isbn_alias"
)

var defaultProjections = []Projection{
	ProjectionAuthority,
	ProjectionBibliography,
	ProjectionLibrary,
	ProjectionProviderLatest,
	ProjectionBibliographyContext,
	ProjectionKDCSummary,
	ProjectionISBNAlias,
}

var projectionDatasets = map[Projection]map[string]struct{}{
	ProjectionAuthority: {
		"person":  {},
		"concept": {},
	},
	ProjectionBibliography: {
		"book":                   {},
		"offline":                {},
		"online":                 {},
		"audiovisual":            {},
		"government_publication": {},
		"serial":                 {},
		"thesis":                 {},
	},
	ProjectionLibrary: {
		"library": {},
	},
	ProjectionProviderLatest: {
		"book":                   {},
		"offline":                {},
		"online":                 {},
		"audiovisual":            {},
		"government_publication": {},
		"serial":                 {},
		"thesis":                 {},
	},
	ProjectionBibliographyContext: {
		"book":                   {},
		"offline":                {},
		"online":                 {},
		"audiovisual":            {},
		"government_publication": {},
		"serial":                 {},
		"thesis":                 {},
	},
	ProjectionKDCSummary: {
		"book":                   {},
		"offline":                {},
		"online":                 {},
		"audiovisual":            {},
		"government_publication": {},
		"serial":                 {},
		"thesis":                 {},
	},
	ProjectionISBNAlias: {
		"book":                   {},
		"offline":                {},
		"online":                 {},
		"audiovisual":            {},
		"government_publication": {},
		"serial":                 {},
		"thesis":                 {},
	},
}

func DefaultProjections() []Projection {
	return append([]Projection(nil), defaultProjections...)
}

// ProjectionAppliesToDataset is the closed routing contract between normalized
// raw importer datasets and service projections. Known importer datasets with
// no service projection, such as organization, are explicitly skipped. Unknown
// or non-canonical checkpoint values stop the run before any heavy query.
func ProjectionAppliesToDataset(projection Projection, dataset string) (bool, error) {
	datasets, ok := projectionDatasets[projection]
	if !ok {
		return false, safeError("invalid_projection")
	}
	canonical, err := canonicalDatasetName(dataset)
	if err != nil {
		return false, err
	}
	_, applies := datasets[canonical]
	return applies, nil
}

func NormalizeProjections(values []string) ([]Projection, error) {
	if len(values) == 0 {
		return DefaultProjections(), nil
	}
	allowed := make(map[Projection]struct{}, len(defaultProjections))
	for _, projection := range defaultProjections {
		allowed[projection] = struct{}{}
	}
	seen := make(map[Projection]struct{}, len(values))
	out := make([]Projection, 0, len(values))
	for _, value := range values {
		projection := Projection(strings.ToLower(strings.TrimSpace(value)))
		if _, ok := allowed[projection]; !ok {
			return nil, safeError("invalid_projection")
		}
		if _, duplicate := seen[projection]; duplicate {
			continue
		}
		seen[projection] = struct{}{}
		out = append(out, projection)
	}
	if len(out) == 0 {
		return nil, safeError("invalid_projection")
	}
	return out, nil
}

type Config struct {
	SnapshotDate     time.Time
	RangeSize        uint64
	Projections      []Projection
	TransformVersion string
}

type RawEntry struct {
	SnapshotDate    time.Time
	DatasetName     string
	SourceArchive   string
	SourceEntry     string
	NextRecordIndex uint64
}

type CheckpointKey struct {
	SnapshotDate     time.Time
	DatasetName      string
	SourceArchive    string
	SourceEntry      string
	Projection       Projection
	TransformVersion string
}

type Checkpoint struct {
	CheckpointUUID string
	Version        uint64
	CheckpointKey
	Status          string
	NextRecordIndex uint64
	RangeStartIndex uint64
	RangeEndIndex   uint64
	Attempts        uint16
	ErrorCategory   string
	ErrorMessage    string
	StartedAt       *time.Time
	CompletedAt     *time.Time
	UpdatedAt       time.Time
}

type RecordRange struct {
	Start uint64
	End   uint64
}

type Result struct {
	EntriesTotal       int
	ProjectionsHandled int
	ProjectionsSkipped int
	RangesCompleted    uint64
	RecordsCovered     uint64
}

type Store interface {
	ValidateBackfill(context.Context) error
	ListSucceededRawEntries(context.Context, time.Time) ([]RawEntry, error)
	LoadProjectionCheckpoint(context.Context, CheckpointKey) (Checkpoint, bool, error)
	ExecuteProjectionRange(context.Context, Projection, string, RawEntry, RecordRange) error
	SaveProjectionCheckpoint(context.Context, Checkpoint) error
}

type SafeError struct {
	Category string
}

func (e *SafeError) Error() string {
	category := strings.TrimSpace(e.Category)
	if category == "" {
		category = "unknown"
	}
	return fmt.Sprintf("NLK service projection backfill failed category=%s", category)
}

func ErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	type categorized interface {
		SafeCategory() string
	}
	if value, ok := err.(categorized); ok {
		if category := strings.TrimSpace(value.SafeCategory()); category != "" {
			return category
		}
	}
	if safe, ok := err.(*SafeError); ok {
		return safe.Category
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "unknown_status"):
		return "unknown_status"
	case strings.Contains(message, "timeout"), strings.Contains(message, "deadline"):
		return "timeout"
	case strings.Contains(message, "keeper"), strings.Contains(message, "coordination"), strings.Contains(message, "session_expired"):
		return "keeper_unavailable"
	case strings.Contains(message, "replica"), strings.Contains(message, "not_initialized"), strings.Contains(message, "readonly"):
		return "replica_unavailable"
	default:
		return "execution_failed"
	}
}

func safeError(category string) error {
	return &SafeError{Category: category}
}

func canonicalDatasetName(dataset string) (string, error) {
	dataset = strings.TrimSpace(dataset)
	normalized, err := nlkimport.NormalizeDatasets([]string{dataset})
	if err != nil || len(normalized) != 1 {
		return "", safeError("raw_checkpoint_dataset_unsupported")
	}
	if normalized[0] != dataset {
		return "", safeError("raw_checkpoint_dataset_not_normalized")
	}
	return normalized[0], nil
}
