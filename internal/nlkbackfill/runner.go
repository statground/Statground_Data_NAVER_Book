package nlkbackfill

import (
	"context"
	"strings"
	"time"

	"statground_naver_book_go/internal/util"
)

type Runner struct {
	Store   Store
	Now     func() time.Time
	NewUUID func() string
}

func (r Runner) Run(ctx context.Context, config Config) (Result, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return Result{}, err
	}
	if r.Store == nil {
		return Result{}, safeError("configuration")
	}
	if err := r.Store.ValidateBackfill(ctx); err != nil {
		return Result{}, safeError(ErrorCategory(err))
	}
	entries, err := r.Store.ListSucceededRawEntries(ctx, config.SnapshotDate)
	if err != nil {
		return Result{}, safeError(ErrorCategory(err))
	}
	if err := validateRawEntries(entries, config.SnapshotDate); err != nil {
		return Result{}, err
	}

	result := Result{EntriesTotal: len(entries)}
	for _, entry := range entries {
		for _, projection := range config.Projections {
			if err := ctx.Err(); err != nil {
				return result, safeError("cancelled")
			}
			applies, err := ProjectionAppliesToDataset(projection, entry.DatasetName)
			if err != nil {
				return result, err
			}
			if !applies {
				result.ProjectionsSkipped++
				continue
			}
			handled, ranges, records, err := r.runProjection(ctx, config, entry, projection)
			if err != nil {
				return result, err
			}
			if handled {
				result.ProjectionsHandled++
			}
			result.RangesCompleted += ranges
			result.RecordsCovered += records
		}
	}
	return result, nil
}

func (r Runner) runProjection(
	ctx context.Context,
	config Config,
	entry RawEntry,
	projection Projection,
) (bool, uint64, uint64, error) {
	key := CheckpointKey{
		SnapshotDate:     entry.SnapshotDate,
		DatasetName:      entry.DatasetName,
		SourceArchive:    entry.SourceArchive,
		SourceEntry:      entry.SourceEntry,
		Projection:       projection,
		TransformVersion: config.TransformVersion,
	}
	checkpoint, found, err := r.Store.LoadProjectionCheckpoint(ctx, key)
	if err != nil {
		return false, 0, 0, safeError(ErrorCategory(err))
	}
	if found {
		if !checkpointKeysEqual(checkpoint.CheckpointKey, key) ||
			checkpoint.NextRecordIndex > entry.NextRecordIndex {
			return false, 0, 0, safeError("checkpoint_lineage_mismatch")
		}
		if checkpoint.Status == "succeeded" && checkpoint.NextRecordIndex == entry.NextRecordIndex {
			return false, 0, 0, nil
		}
	} else {
		checkpoint = Checkpoint{CheckpointKey: key}
	}

	started := r.now()
	if checkpoint.StartedAt == nil {
		checkpoint.StartedAt = &started
	}
	checkpoint.CompletedAt = nil
	checkpoint.ErrorCategory = ""
	checkpoint.ErrorMessage = ""

	ranges, err := PlanRanges(checkpoint.NextRecordIndex, entry.NextRecordIndex, config.RangeSize)
	if err != nil {
		return false, 0, 0, err
	}
	var completed uint64
	var covered uint64
	for _, recordRange := range ranges {
		if checkpoint.Attempts == ^uint16(0) {
			return true, completed, covered, safeError("attempt_limit")
		}
		attemptStarted := r.now()
		checkpoint.Status = "running"
		checkpoint.RangeStartIndex = recordRange.Start
		checkpoint.RangeEndIndex = recordRange.End
		checkpoint.Attempts++
		checkpoint.StartedAt = &attemptStarted
		checkpoint.CompletedAt = nil
		r.prepareCheckpoint(&checkpoint)
		if err := r.Store.SaveProjectionCheckpoint(ctx, checkpoint); err != nil {
			return true, completed, covered, safeError(ErrorCategory(err))
		}

		if err := r.Store.ExecuteProjectionRange(
			ctx,
			projection,
			config.TransformVersion,
			entry,
			recordRange,
		); err != nil {
			category := ErrorCategory(err)
			failed := checkpoint
			failed.Status = "failed"
			failed.ErrorCategory = category
			failed.ErrorMessage = category
			finished := r.now()
			failed.CompletedAt = &finished
			r.prepareCheckpoint(&failed)
			_ = r.Store.SaveProjectionCheckpoint(ctx, failed)
			return true, completed, covered, safeError(category)
		}

		// The range is durable only after the heavy INSERT SELECT returned a
		// confirmed success response. Ambiguous failures never reach this line.
		checkpoint.NextRecordIndex = recordRange.End
		checkpoint.ErrorCategory = ""
		checkpoint.ErrorMessage = ""
		r.prepareCheckpoint(&checkpoint)
		if err := r.Store.SaveProjectionCheckpoint(ctx, checkpoint); err != nil {
			return true, completed, covered, safeError(ErrorCategory(err))
		}
		completed++
		covered += recordRange.End - recordRange.Start
	}

	checkpoint.Status = "succeeded"
	checkpoint.NextRecordIndex = entry.NextRecordIndex
	checkpoint.RangeStartIndex = entry.NextRecordIndex
	checkpoint.RangeEndIndex = entry.NextRecordIndex
	checkpoint.ErrorCategory = ""
	checkpoint.ErrorMessage = ""
	finished := r.now()
	checkpoint.CompletedAt = &finished
	r.prepareCheckpoint(&checkpoint)
	if err := r.Store.SaveProjectionCheckpoint(ctx, checkpoint); err != nil {
		return true, completed, covered, safeError(ErrorCategory(err))
	}
	return true, completed, covered, nil
}

func PlanRanges(start, end, size uint64) ([]RecordRange, error) {
	if size == 0 || size > MaxRangeSize || start > end {
		return nil, safeError("invalid_range")
	}
	ranges := make([]RecordRange, 0)
	for next := start; next < end; {
		rangeEnd := next + size
		if rangeEnd < next || rangeEnd > end {
			rangeEnd = end
		}
		ranges = append(ranges, RecordRange{Start: next, End: rangeEnd})
		next = rangeEnd
	}
	if err := ValidateContiguousRanges(start, end, ranges); err != nil {
		return nil, err
	}
	return ranges, nil
}

func ValidateContiguousRanges(start, end uint64, ranges []RecordRange) error {
	cursor := start
	for _, recordRange := range ranges {
		if recordRange.Start != cursor || recordRange.End <= recordRange.Start || recordRange.End > end {
			return safeError("range_gap_or_overlap")
		}
		cursor = recordRange.End
	}
	if cursor != end {
		return safeError("range_gap_or_overlap")
	}
	return nil
}

func normalizeConfig(config Config) (Config, error) {
	if config.SnapshotDate.IsZero() {
		return Config{}, safeError("invalid_snapshot_date")
	}
	config.SnapshotDate = dateOnlyKST(config.SnapshotDate)
	if config.RangeSize == 0 {
		config.RangeSize = DefaultRangeSize
	}
	if config.RangeSize > MaxRangeSize {
		return Config{}, safeError("invalid_range_size")
	}
	if len(config.Projections) == 0 {
		config.Projections = DefaultProjections()
	}
	normalized, err := NormalizeProjections(projectionsToStrings(config.Projections))
	if err != nil {
		return Config{}, err
	}
	config.Projections = normalized
	config.TransformVersion = strings.TrimSpace(config.TransformVersion)
	if config.TransformVersion == "" {
		config.TransformVersion = DefaultTransformVersion
	}
	if len(config.TransformVersion) > 64 {
		return Config{}, safeError("invalid_transform_version")
	}
	for _, char := range config.TransformVersion {
		if (char < 'a' || char > 'z') &&
			(char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') &&
			char != '_' && char != '-' && char != '.' {
			return Config{}, safeError("invalid_transform_version")
		}
	}
	return config, nil
}

func validateRawEntries(entries []RawEntry, snapshot time.Time) error {
	seen := make(map[string]struct{}, len(entries))
	var previous string
	for _, entry := range entries {
		if !dateOnlyKST(entry.SnapshotDate).Equal(snapshot) ||
			strings.TrimSpace(entry.SourceArchive) == "" ||
			strings.TrimSpace(entry.SourceEntry) == "" {
			return safeError("raw_checkpoint_contract")
		}
		if _, err := canonicalDatasetName(entry.DatasetName); err != nil {
			return err
		}
		key := strings.Join([]string{
			entry.DatasetName,
			entry.SourceArchive,
			entry.SourceEntry,
		}, "\x1f")
		if _, duplicate := seen[key]; duplicate {
			return safeError("raw_checkpoint_duplicate")
		}
		if previous != "" && key < previous {
			return safeError("raw_checkpoint_order")
		}
		seen[key] = struct{}{}
		previous = key
	}
	return nil
}

func projectionsToStrings(projections []Projection) []string {
	out := make([]string, 0, len(projections))
	for _, projection := range projections {
		out = append(out, string(projection))
	}
	return out
}

func dateOnlyKST(value time.Time) time.Time {
	value = value.In(util.KST())
	year, month, day := value.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, util.KST())
}

func checkpointKeysEqual(left, right CheckpointKey) bool {
	return left.SnapshotDate.Equal(right.SnapshotDate) &&
		left.DatasetName == right.DatasetName &&
		left.SourceArchive == right.SourceArchive &&
		left.SourceEntry == right.SourceEntry &&
		left.Projection == right.Projection &&
		left.TransformVersion == right.TransformVersion
}

func (r Runner) prepareCheckpoint(checkpoint *Checkpoint) {
	checkpoint.CheckpointUUID = r.id()
	checkpoint.UpdatedAt = r.now()
	next := uint64(checkpoint.UpdatedAt.UnixMicro())
	if next <= checkpoint.Version {
		next = checkpoint.Version + 1
	}
	checkpoint.Version = next
}

func (r Runner) now() time.Time {
	if r.Now != nil {
		return r.Now().In(util.KST())
	}
	return util.NowKST()
}

func (r Runner) id() string {
	if r.NewUUID != nil {
		return r.NewUUID()
	}
	return util.UUIDv7()
}
