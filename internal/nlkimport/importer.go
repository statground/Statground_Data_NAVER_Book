package nlkimport

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"statground_naver_book_go/internal/nlklod"
	"statground_naver_book_go/internal/util"
)

const (
	defaultBatchSize      = 20000
	maxBatchSize          = 50000
	defaultBatchByteLimit = 64 * 1024 * 1024
	maxBatchByteLimit     = 256 * 1024 * 1024
)

var errLimitReached = errors.New("maximum record limit reached")

type Importer struct {
	Store       Store
	IDGenerator nlklod.IDGenerator
	Now         func() time.Time
}

func (i *Importer) Run(ctx context.Context, config Config) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, safeError("cancelled")
	}
	config = normalizeConfig(config)
	if config.BatchSize < 1 || config.BatchSize > maxBatchSize {
		return Result{}, safeError("invalid_batch_size")
	}
	if config.BatchByteLimit < 1 || config.BatchByteLimit > maxBatchByteLimit {
		return Result{}, safeError("invalid_batch_byte_limit")
	}
	if !validEntryShard(config.EntryShardCount, config.EntryShardIndex) {
		return Result{}, safeError("invalid_entry_shard")
	}
	if !config.DryRun && i.Store == nil {
		return Result{}, safeError("store_required")
	}
	plans, err := discoverArchives(config.InputDir, config.Datasets, config.SnapshotDate)
	if err != nil {
		return Result{}, err
	}
	plans = selectEntryShard(plans, config.EntryShardCount, config.EntryShardIndex)
	if !config.DryRun {
		if err := i.Store.Validate(ctx); err != nil {
			return Result{}, safeError("preflight_failed")
		}
	}

	now := i.now()
	runUUID := i.id()
	result := Result{RunUUID: runUUID, ArchivesTotal: uint16(len(plans))}
	var compressedBytes, uncompressedBytes uint64
	for _, plan := range plans {
		result.EntriesTotal += uint32(len(plan.Entries))
		compressedBytes += plan.CompressedBytes
		uncompressedBytes += plan.UncompressedBytes
	}
	runState := RunState{
		RunUUID:             runUUID,
		Status:              "running",
		DatasetSnapshotDate: config.SnapshotDate,
		DatasetUpdatedAt:    snapshotMidnight(config.SnapshotDate),
		SourceURL:           nlklod.SourceURL,
		LicenseName:         nlklod.LicenseName,
		LicenseURL:          nlklod.LicenseURL,
		Attribution:         nlklod.Attribution,
		ArchivesTotal:       result.ArchivesTotal,
		EntriesTotal:        result.EntriesTotal,
		BytesCompressed:     compressedBytes,
		BytesUncompressed:   uncompressedBytes,
		ImporterVersion:     config.ImporterVersion,
		StartedAt:           now,
		HeartbeatAt:         now,
		Source:              config.Source,
	}
	i.advanceRunVersion(&runState)
	if !config.DryRun {
		if err := i.Store.SaveRun(ctx, runState); err != nil {
			return Result{}, safeError("run_log_insert_failed")
		}
	}

	for _, plan := range plans {
		archiveComplete := true
		reader, err := zip.OpenReader(plan.LocalPath)
		if err != nil {
			return i.failRun(ctx, config, result, runState, "archive_open_failed")
		}
		files := make(map[string]*zip.File, len(reader.File))
		for _, file := range reader.File {
			files[file.Name] = file
		}
		for _, entry := range plan.Entries {
			if config.MaxRecords > 0 && result.RecordsParsed >= config.MaxRecords {
				result.Limited = true
				archiveComplete = false
				break
			}
			file := files[entry.Name]
			if file == nil {
				_ = reader.Close()
				return i.failRun(ctx, config, result, runState, "archive_contract")
			}
			outcome, processErr := i.processEntry(ctx, config, plan, entry, file, result.RunUUID, &result)
			if processErr != nil {
				_ = reader.Close()
				return i.failRun(ctx, config, result, runState, ErrorCategory(processErr))
			}
			if outcome.skipped || outcome.completed {
				result.EntriesCompleted++
			}
			if outcome.limited {
				result.Limited = true
				archiveComplete = false
				break
			}
			runState = updateRunState(runState, result, i.now())
			if !config.DryRun {
				if err := i.Store.SaveRun(ctx, runState); err != nil {
					_ = reader.Close()
					return Result{}, safeError("run_log_insert_failed")
				}
			}
		}
		_ = reader.Close()
		if archiveComplete {
			result.ArchivesCompleted++
		}
		if result.Limited {
			break
		}
	}

	finished := i.now()
	runState = updateRunState(runState, result, finished)
	runState.FinishedAt = &finished
	if result.Limited {
		runState.Status = "cancelled"
	} else {
		runState.Status = "succeeded"
	}
	i.advanceRunVersion(&runState)
	if !config.DryRun {
		if err := i.Store.SaveRun(ctx, runState); err != nil {
			return Result{}, safeError("run_log_insert_failed")
		}
	}
	return result, nil
}

type entryOutcome struct {
	completed bool
	skipped   bool
	limited   bool
}

func (i *Importer) processEntry(
	ctx context.Context,
	config Config,
	archive archivePlan,
	entry entryPlan,
	file *zip.File,
	runUUID string,
	result *Result,
) (entryOutcome, error) {
	key := CheckpointKey{
		SnapshotDate: config.SnapshotDate,
		Archive:      archive.BaseName,
		Entry:        entry.Name,
	}
	var checkpoint Checkpoint
	found := false
	if config.Resume && !config.DryRun {
		loaded, exists, err := i.Store.LoadCheckpoint(ctx, key)
		if err != nil {
			return entryOutcome{}, safeError("checkpoint_read_failed")
		}
		checkpoint, found = loaded, exists
		expectedCRC := fmt.Sprintf("%08x", entry.CRC32)
		if found && (checkpoint.EntryCRC32 != expectedCRC ||
			checkpoint.EntryUncompressed != entry.UncompressedBytes ||
			(checkpoint.DatasetName != "" && checkpoint.DatasetName != archive.Dataset)) {
			return entryOutcome{}, safeError("checkpoint_lineage_mismatch")
		}
		if found {
			normalizeCommittedCheckpointProgress(&checkpoint)
		}
		if found && checkpoint.Status == "succeeded" {
			return entryOutcome{completed: true, skipped: true}, nil
		}
	}

	started := i.now()
	if !found {
		checkpoint = Checkpoint{
			DatasetSnapshotDate: config.SnapshotDate,
			DatasetName:         archive.Dataset,
			SourceArchive:       archive.BaseName,
			SourceEntry:         entry.Name,
			EntryCRC32:          fmt.Sprintf("%08x", entry.CRC32),
			EntryUncompressed:   entry.UncompressedBytes,
			Source:              config.Source,
		}
	}
	checkpoint.RunUUID = runUUID
	checkpoint.DatasetName = archive.Dataset
	checkpoint.SourceArchive = archive.BaseName
	checkpoint.SourceEntry = entry.Name
	checkpoint.EntryCRC32 = fmt.Sprintf("%08x", entry.CRC32)
	checkpoint.EntryUncompressed = entry.UncompressedBytes
	checkpoint.Source = config.Source
	checkpoint.Status = "running"
	checkpoint.Attempts++
	checkpoint.StartedAt = &started
	checkpoint.CompletedAt = nil
	checkpoint.ErrorCode = ""
	checkpoint.ErrorMessage = ""
	i.prepareCheckpoint(&checkpoint)
	if !config.DryRun {
		if err := i.Store.SaveCheckpoint(ctx, checkpoint); err != nil {
			return entryOutcome{}, safeError("checkpoint_insert_failed")
		}
	}

	stream, err := file.Open()
	if err != nil {
		return entryOutcome{}, i.failCheckpoint(ctx, config, checkpoint, "entry_open_failed")
	}
	defer stream.Close()
	entryHash := sha256.New()
	reader := io.TeeReader(stream, entryHash)
	startIndex := checkpoint.NextRecordIndex
	verifyExisting := found
	batchRows := make([]map[string]any, 0, config.BatchSize)
	var batchBytes uint64
	batchEnd := startIndex
	batchProcessed := 0
	batchRejected := 0
	lastResourceID := checkpoint.LastResourceID
	var recordsSeen uint64

	flush := func() error {
		if batchProcessed == 0 {
			return nil
		}
		checkpoint.Status = "running"
		checkpoint.ErrorCode = ""
		checkpoint.ErrorMessage = ""
		rowsToInsert := batchRows
		if !config.DryRun {
			if verifyExisting && len(batchRows) > 0 {
				indexes := rawRecordIndexes(batchRows)
				existing, err := i.Store.ExistingRawRecordIndexes(ctx, RawLineage{
					SnapshotDate: config.SnapshotDate,
					DatasetName:  archive.Dataset,
					Archive:      archive.BaseName,
					Entry:        entry.Name,
				}, indexes)
				if err != nil {
					return safeError("existing_range_failed")
				}
				rowsToInsert = missingRawRows(batchRows, existing)
			}
			if len(rowsToInsert) > 0 {
				if err := i.Store.InsertRawRows(ctx, rowsToInsert); err != nil {
					return safeError("raw_insert_failed")
				}
			}
		}
		checkpoint.NextRecordIndex = batchEnd
		checkpoint.LastResourceID = lastResourceID
		checkpoint.RecordsParsed += uint64(batchProcessed)
		checkpoint.RecordsRejected += uint64(batchRejected)
		if !config.DryRun {
			checkpoint.RecordsInserted += uint64(len(batchRows))
			result.RecordsInserted += uint64(len(rowsToInsert))
		}
		i.prepareCheckpoint(&checkpoint)
		if !config.DryRun {
			if err := i.Store.SaveCheckpoint(ctx, checkpoint); err != nil {
				return safeError("checkpoint_insert_failed")
			}
		}
		verifyExisting = false
		batchRows = batchRows[:0]
		batchBytes = 0
		batchProcessed = 0
		batchRejected = 0
		return nil
	}

	parseErr := nlklod.StreamResources(reader, func(index uint64, resource nlklod.Resource) error {
		recordsSeen = index + 1
		if err := ctx.Err(); err != nil {
			return safeError("cancelled")
		}
		if index < startIndex {
			return nil
		}
		if config.MaxRecords > 0 && result.RecordsParsed >= config.MaxRecords {
			return errLimitReached
		}
		result.RecordsParsed++
		batchEnd = index + 1
		batchProcessed++

		row, err := nlklod.BuildRow(resource, nlklod.Evidence{
			RunUUID:       runUUID,
			DatasetName:   archive.Dataset,
			SnapshotDate:  config.SnapshotDate,
			SourceArchive: archive.BaseName,
			SourceEntry:   entry.Name,
			RecordIndex:   index,
			ImportedAt:    i.now(),
		})
		if err != nil {
			batchRejected++
			result.RecordsRejected++
		} else {
			batchRows = append(batchRows, row)
			batchBytes += estimateRawRowBytes(row)
			lastResourceID = strings.TrimSpace(fmt.Sprint(row["resource_id"]))
		}
		if batchProcessed >= config.BatchSize || batchBytes >= config.BatchByteLimit {
			if err := flush(); err != nil {
				return err
			}
		}
		if config.MaxRecords > 0 && result.RecordsParsed >= config.MaxRecords {
			if err := flush(); err != nil {
				return err
			}
			return errLimitReached
		}
		return nil
	})
	if errors.Is(parseErr, errLimitReached) {
		if err := flush(); err != nil {
			return entryOutcome{}, i.failCheckpoint(ctx, config, checkpoint, ErrorCategory(err))
		}
		return entryOutcome{limited: true}, nil
	}
	if parseErr != nil {
		category := ErrorCategory(parseErr)
		if category == "unknown" || category == "" {
			category = "rdf_decode_failed"
		}
		return entryOutcome{}, i.failCheckpoint(ctx, config, checkpoint, category)
	}
	if err := flush(); err != nil {
		return entryOutcome{}, i.failCheckpoint(ctx, config, checkpoint, ErrorCategory(err))
	}
	if startIndex > recordsSeen {
		return entryOutcome{}, i.failCheckpoint(ctx, config, checkpoint, "checkpoint_index_mismatch")
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return entryOutcome{}, i.failCheckpoint(ctx, config, checkpoint, "entry_checksum_failed")
	}
	checkpoint.ContentHash = hex.EncodeToString(entryHash.Sum(nil))
	checkpoint.Status = "succeeded"
	completed := i.now()
	checkpoint.CompletedAt = &completed
	i.prepareCheckpoint(&checkpoint)
	if !config.DryRun {
		if err := i.Store.SaveCheckpoint(ctx, checkpoint); err != nil {
			return entryOutcome{}, safeError("checkpoint_insert_failed")
		}
	}
	return entryOutcome{completed: true}, nil
}

func normalizeCommittedCheckpointProgress(checkpoint *Checkpoint) {
	if checkpoint == nil {
		return
	}
	checkpoint.RecordsParsed = checkpoint.NextRecordIndex
	if checkpoint.RecordsInserted > checkpoint.NextRecordIndex {
		checkpoint.RecordsInserted = checkpoint.NextRecordIndex
		checkpoint.RecordsRejected = 0
		return
	}
	checkpoint.RecordsRejected = checkpoint.NextRecordIndex - checkpoint.RecordsInserted
}

func rawRecordIndexes(rows []map[string]any) []uint64 {
	indexes := make([]uint64, 0, len(rows))
	for _, row := range rows {
		switch value := row["source_record_index"].(type) {
		case uint64:
			indexes = append(indexes, value)
		case uint:
			indexes = append(indexes, uint64(value))
		case int:
			if value >= 0 {
				indexes = append(indexes, uint64(value))
			}
		}
	}
	return indexes
}

func missingRawRows(rows []map[string]any, existing map[uint64]struct{}) []map[string]any {
	if len(existing) == 0 {
		return rows
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		indexes := rawRecordIndexes([]map[string]any{row})
		if len(indexes) == 1 {
			if _, found := existing[indexes[0]]; found {
				continue
			}
		}
		out = append(out, row)
	}
	return out
}

func (i *Importer) failCheckpoint(ctx context.Context, config Config, checkpoint Checkpoint, category string) error {
	if category == "" || category == "unknown" {
		category = "entry_failed"
	}
	checkpoint.Status = "failed"
	checkpoint.ErrorCode = category
	checkpoint.ErrorMessage = category
	completed := i.now()
	checkpoint.CompletedAt = &completed
	i.prepareCheckpoint(&checkpoint)
	if !config.DryRun && i.Store != nil {
		_ = i.Store.SaveCheckpoint(ctx, checkpoint)
	}
	return safeError(category)
}

func (i *Importer) failRun(
	ctx context.Context,
	config Config,
	result Result,
	runState RunState,
	category string,
) (Result, error) {
	if category == "" || category == "unknown" {
		category = "run_failed"
	}
	finished := i.now()
	runState = updateRunState(runState, result, finished)
	runState.Status = "failed"
	runState.ErrorCode = category
	runState.ErrorMessage = category
	runState.FinishedAt = &finished
	i.advanceRunVersion(&runState)
	if !config.DryRun && i.Store != nil {
		_ = i.Store.SaveRun(ctx, runState)
	}
	return result, safeError(category)
}

func normalizeConfig(config Config) Config {
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.BatchByteLimit == 0 {
		config.BatchByteLimit = defaultBatchByteLimit
	}
	if config.EntryShardCount == 0 {
		config.EntryShardCount = 1
	}
	if strings.TrimSpace(config.ImporterVersion) == "" {
		config.ImporterVersion = "nlk_lod_importer_v2"
	}
	if strings.TrimSpace(config.Source) == "" {
		config.Source = "controlled_local_import"
	}
	return config
}

func estimateRawRowBytes(row map[string]any) uint64 {
	var size uint64
	for key, value := range row {
		size += uint64(len(key) + 16)
		switch typed := value.(type) {
		case string:
			size += uint64(len(typed) + 8)
		case []string:
			size += 24
			for _, item := range typed {
				size += uint64(len(item) + 8)
			}
		case []int:
			size += uint64(24 + len(typed)*8)
		case []uint8:
			size += uint64(24 + len(typed))
		case uint64, uint32, uint16, uint8, uint, int64, int32, int16, int8, int, time.Time:
			size += 16
		default:
			size += 16
		}
	}
	return size
}

func updateRunState(state RunState, result Result, now time.Time) RunState {
	state.ArchivesCompleted = result.ArchivesCompleted
	state.EntriesCompleted = result.EntriesCompleted
	state.RecordsParsed = result.RecordsParsed
	state.RecordsInserted = result.RecordsInserted
	state.RecordsRejected = result.RecordsRejected
	state.HeartbeatAt = now
	state.ErrorCode = ""
	state.ErrorMessage = ""
	return state
}

func (i *Importer) prepareCheckpoint(checkpoint *Checkpoint) {
	checkpoint.CheckpointUUID = i.id()
	checkpoint.UpdatedAt = i.now()
	next := uint64(checkpoint.UpdatedAt.UnixMicro())
	if next <= checkpoint.Version {
		next = checkpoint.Version + 1
	}
	checkpoint.Version = next
}

func (i *Importer) advanceRunVersion(state *RunState) {
	next := uint64(i.now().UnixMicro())
	if next <= state.Version {
		next = state.Version + 1
	}
	state.Version = next
}

func (i *Importer) now() time.Time {
	if i.Now != nil {
		return i.Now().In(util.KST())
	}
	return util.NowKST()
}

func (i *Importer) id() string {
	if i.IDGenerator != nil {
		return i.IDGenerator()
	}
	return util.UUIDv7()
}

func snapshotMidnight(value time.Time) time.Time {
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, util.KST())
}
