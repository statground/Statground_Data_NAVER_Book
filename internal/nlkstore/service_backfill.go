package nlkstore

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/nlkbackfill"
	"statground_naver_book_go/internal/util"
)

const (
	defaultServiceCheckpointTable      = "Data_Book_NLK_Log.nlk_service_projection_checkpoint"
	defaultServiceCheckpointLocal      = "Data_Book_NLK_Log.nlk_service_projection_checkpoint_local"
	defaultServiceCheckpointLatestView = "Data_Book_NLK_Log.v_nlk_service_projection_checkpoint_latest"
	defaultRawCheckpointLatestView     = "Data_Book_NLK_Log.v_nlk_import_entry_checkpoint_latest"
)

func (s *ClickHouseStore) ValidateBackfill(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateConnectionBoundary(); err != nil {
		return err
	}
	if s.Config.RawTable != nlkbackfill.DefaultRawTable ||
		s.Config.CheckpointLatestView != defaultRawCheckpointLatestView ||
		s.Config.ServiceCheckpointTable != defaultServiceCheckpointTable ||
		s.Config.ServiceCheckpointLocal != defaultServiceCheckpointLocal ||
		s.Config.ServiceCheckpointLatestView != defaultServiceCheckpointLatestView {
		return &StoreError{Category: "configuration"}
	}

	tables := []string{
		s.Config.RawTable,
		s.Config.CheckpointLatestView,
		s.Config.ServiceCheckpointTable,
		s.Config.ServiceCheckpointLocal,
		s.Config.ServiceCheckpointLatestView,
	}
	for _, projection := range nlkbackfill.DefaultProjections() {
		target, ok := nlkbackfill.ProjectionTarget(projection)
		if !ok {
			return &StoreError{Category: "configuration"}
		}
		localTarget, ok := nlkbackfill.ProjectionLocalTarget(projection)
		if !ok {
			return &StoreError{Category: "configuration"}
		}
		tables = append(tables, target, localTarget)
	}
	for _, table := range uniqueStrings(tables) {
		if err := s.tableExists(ctx, table); err != nil {
			return err
		}
	}
	for _, table := range uniqueStrings(append(tables, s.Config.ServiceCheckpointLocal)) {
		if err := s.checkGrant(ctx, "SELECT", table); err != nil {
			return err
		}
	}
	for _, table := range append(
		[]string{s.Config.ServiceCheckpointTable, s.Config.ServiceCheckpointLocal},
		projectionWriteTargetList()...,
	) {
		if err := s.checkGrant(ctx, "INSERT", table); err != nil {
			return err
		}
	}
	return nil
}

func (s *ClickHouseStore) ListSucceededRawEntries(
	ctx context.Context,
	snapshot time.Time,
) ([]nlkbackfill.RawEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if snapshot.IsZero() {
		return nil, &StoreError{Category: "invalid_snapshot_date"}
	}
	query := fmt.Sprintf(`
		SELECT
			toString(dataset_snapshot_date) AS dataset_snapshot_date,
			dataset_name,
			source_archive,
			source_entry,
			next_record_index
		FROM %s
		WHERE dataset_snapshot_date = toDate(%s)
		  AND status = 'succeeded'
		ORDER BY dataset_name, source_archive, source_entry
		SETTINGS max_threads = 1, max_execution_time = 60
	`, s.Config.CheckpointLatestView, util.SQLString(snapshot.Format("2006-01-02")))
	rows, err := s.Client.QueryJSONEachRow(query)
	if err != nil {
		return nil, &StoreError{Category: classifyProjectionError(err)}
	}
	entries := make([]nlkbackfill.RawEntry, 0, len(rows))
	for _, row := range rows {
		parsedSnapshot := parseDate(util.ToString(row["dataset_snapshot_date"]))
		if parsedSnapshot.IsZero() {
			return nil, &StoreError{Category: "raw_checkpoint_contract"}
		}
		entries = append(entries, nlkbackfill.RawEntry{
			SnapshotDate:    parsedSnapshot,
			DatasetName:     strings.TrimSpace(util.ToString(row["dataset_name"])),
			SourceArchive:   strings.TrimSpace(util.ToString(row["source_archive"])),
			SourceEntry:     strings.TrimSpace(util.ToString(row["source_entry"])),
			NextRecordIndex: toUint64(row["next_record_index"]),
		})
	}
	return entries, nil
}

func (s *ClickHouseStore) LoadProjectionCheckpoint(
	ctx context.Context,
	key nlkbackfill.CheckpointKey,
) (nlkbackfill.Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return nlkbackfill.Checkpoint{}, false, err
	}
	query := fmt.Sprintf(`
		SELECT
			toString(checkpoint_uuid) AS checkpoint_uuid,
			version,
			toString(dataset_snapshot_date) AS dataset_snapshot_date,
			dataset_name,
			source_archive,
			source_entry,
			`+"`projection`"+`,
			transform_version,
			status,
			next_record_index,
			range_start_index,
			range_end_index,
			attempts,
			error_category,
			error_message,
			started_at,
			completed_at,
			updated_at
		FROM %s
		WHERE dataset_snapshot_date = toDate(%s)
		  AND dataset_name = %s
		  AND source_archive = %s
		  AND source_entry = %s
		  AND `+"`projection`"+` = %s
		  AND transform_version = %s
		LIMIT 1
		SETTINGS max_threads = 1, max_execution_time = 30
	`,
		s.Config.ServiceCheckpointLatestView,
		util.SQLString(key.SnapshotDate.Format("2006-01-02")),
		util.SQLString(key.DatasetName),
		util.SQLString(key.SourceArchive),
		util.SQLString(key.SourceEntry),
		util.SQLString(string(key.Projection)),
		util.SQLString(key.TransformVersion),
	)
	rows, err := s.Client.QueryJSONEachRow(query)
	if err != nil {
		return nlkbackfill.Checkpoint{}, false, &StoreError{Category: classifyProjectionError(err)}
	}
	if len(rows) == 0 {
		return nlkbackfill.Checkpoint{}, false, nil
	}
	row := rows[0]
	attempts := toUint64(row["attempts"])
	if attempts > uint64(^uint16(0)) {
		return nlkbackfill.Checkpoint{}, false, &StoreError{Category: "checkpoint_contract"}
	}
	loadedKey := nlkbackfill.CheckpointKey{
		SnapshotDate:     parseDate(util.ToString(row["dataset_snapshot_date"])),
		DatasetName:      strings.TrimSpace(util.ToString(row["dataset_name"])),
		SourceArchive:    strings.TrimSpace(util.ToString(row["source_archive"])),
		SourceEntry:      strings.TrimSpace(util.ToString(row["source_entry"])),
		Projection:       nlkbackfill.Projection(strings.TrimSpace(util.ToString(row["projection"]))),
		TransformVersion: strings.TrimSpace(util.ToString(row["transform_version"])),
	}
	checkpoint := nlkbackfill.Checkpoint{
		CheckpointUUID:  strings.TrimSpace(util.ToString(row["checkpoint_uuid"])),
		Version:         toUint64(row["version"]),
		CheckpointKey:   loadedKey,
		Status:          strings.TrimSpace(util.ToString(row["status"])),
		NextRecordIndex: toUint64(row["next_record_index"]),
		RangeStartIndex: toUint64(row["range_start_index"]),
		RangeEndIndex:   toUint64(row["range_end_index"]),
		Attempts:        uint16(attempts),
		ErrorCategory:   strings.TrimSpace(util.ToString(row["error_category"])),
		ErrorMessage:    strings.TrimSpace(util.ToString(row["error_message"])),
		UpdatedAt:       parseTime(util.ToString(row["updated_at"])),
	}
	if parsed := parseTime(util.ToString(row["started_at"])); !parsed.IsZero() {
		checkpoint.StartedAt = &parsed
	}
	if parsed := parseTime(util.ToString(row["completed_at"])); !parsed.IsZero() {
		checkpoint.CompletedAt = &parsed
	}
	return checkpoint, true, nil
}

func (s *ClickHouseStore) ExecuteProjectionRange(
	ctx context.Context,
	projection nlkbackfill.Projection,
	transformVersion string,
	entry nlkbackfill.RawEntry,
	recordRange nlkbackfill.RecordRange,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	target, ok := nlkbackfill.ProjectionTarget(projection)
	if !ok {
		return &StoreError{Category: "configuration"}
	}
	query, err := nlkbackfill.BuildProjectionSQL(
		projection,
		transformVersion,
		entry,
		recordRange,
		s.Config.RawTable,
		target,
	)
	if err != nil {
		return &StoreError{Category: nlkbackfill.ErrorCategory(err)}
	}
	// Projection INSERT SELECT is intentionally attempted exactly once. A
	// timeout, UNKNOWN_STATUS, replica, or Keeper result is ambiguous and must
	// leave the durable next_record_index unchanged for operator-led recovery.
	if err := s.Client.ExecSingleAttempt(query); err != nil {
		return &StoreError{Category: classifyProjectionError(err)}
	}
	return nil
}

func (s *ClickHouseStore) SaveProjectionCheckpoint(
	ctx context.Context,
	state nlkbackfill.Checkpoint,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	row := map[string]any{
		"checkpoint_uuid":       state.CheckpointUUID,
		"version":               state.Version,
		"dataset_snapshot_date": state.SnapshotDate.Format("2006-01-02"),
		"dataset_name":          state.DatasetName,
		"source_archive":        state.SourceArchive,
		"source_entry":          state.SourceEntry,
		"projection":            string(state.Projection),
		"transform_version":     state.TransformVersion,
		"status":                state.Status,
		"next_record_index":     state.NextRecordIndex,
		"range_start_index":     state.RangeStartIndex,
		"range_end_index":       state.RangeEndIndex,
		"attempts":              state.Attempts,
		"error_category":        state.ErrorCategory,
		"error_message":         boundedError(state.ErrorMessage),
		"started_at":            nullableTime(state.StartedAt),
		"completed_at":          nullableTime(state.CompletedAt),
		"updated_at":            util.FormatCHDateTime64Millis(state.UpdatedAt),
	}
	token := stableDedupToken(
		s.Config.ServiceCheckpointTable,
		state.SnapshotDate.Format("2006-01-02"),
		state.DatasetName,
		state.SourceArchive,
		state.SourceEntry,
		string(state.Projection),
		state.TransformVersion,
		strconv.FormatUint(state.Version, 10),
	)
	if err := s.Client.InsertJSONEachRowDurable(
		s.Config.ServiceCheckpointTable,
		[]map[string]any{row},
		token,
	); err != nil {
		return &StoreError{Category: classifyProjectionError(err)}
	}
	return nil
}

func classifyProjectionError(err error) string {
	if err == nil {
		return ""
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
	case strings.Contains(message, "http status=429"), strings.Contains(message, "http status=500"),
		strings.Contains(message, "http status=502"), strings.Contains(message, "http status=503"),
		strings.Contains(message, "http status=504"):
		return "unknown_status"
	default:
		return "execution_failed"
	}
}

func projectionWriteTargetList() []string {
	out := make([]string, 0, len(nlkbackfill.DefaultProjections())*2)
	for _, projection := range nlkbackfill.DefaultProjections() {
		target, _ := nlkbackfill.ProjectionTarget(projection)
		localTarget, _ := nlkbackfill.ProjectionLocalTarget(projection)
		out = append(out, target, localTarget)
	}
	return out
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func parseDate(value string) time.Time {
	parsed, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(value), util.KST())
	if err != nil {
		return time.Time{}
	}
	return parsed
}
