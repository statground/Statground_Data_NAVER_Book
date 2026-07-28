package nlkstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/nlkimport"
	"statground_naver_book_go/internal/util"
)

var tableIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$`)

const existingRawIndexLookupChunkSize = 5000

type Config struct {
	RawTable             string
	RawLocalTable        string
	RunTable             string
	RunLocalTable        string
	CheckpointTable      string
	CheckpointLocal      string
	RunLatestView        string
	CheckpointLatestView string
	RequireHTTPS         bool
}

func ConfigFromEnv() Config {
	return Config{
		RawTable:             envx.String("NLK_RAW_TABLE", "Data_Book_NLK_Raw.nlk_resource_raw"),
		RawLocalTable:        envx.String("NLK_RAW_LOCAL_TABLE", "Data_Book_NLK_Raw.nlk_resource_raw_local"),
		RunTable:             envx.String("NLK_IMPORT_RUN_TABLE", "Data_Book_NLK_Log.nlk_import_run_log"),
		RunLocalTable:        envx.String("NLK_IMPORT_RUN_LOCAL_TABLE", "Data_Book_NLK_Log.nlk_import_run_log_local"),
		CheckpointTable:      envx.String("NLK_IMPORT_CHECKPOINT_TABLE", "Data_Book_NLK_Log.nlk_import_entry_checkpoint"),
		CheckpointLocal:      envx.String("NLK_IMPORT_CHECKPOINT_LOCAL_TABLE", "Data_Book_NLK_Log.nlk_import_entry_checkpoint_local"),
		RunLatestView:        envx.String("NLK_IMPORT_RUN_LATEST_VIEW", "Data_Book_NLK_Log.v_nlk_import_run_latest"),
		CheckpointLatestView: envx.String("NLK_IMPORT_CHECKPOINT_LATEST_VIEW", "Data_Book_NLK_Log.v_nlk_import_entry_checkpoint_latest"),
		RequireHTTPS:         boolEnv("NLK_REQUIRE_CLICKHOUSE_HTTPS", true),
	}
}

type ClickHouseStore struct {
	Client *ch.Client
	Config Config
}

func NewClickHouse(client *ch.Client, config Config) (*ClickHouseStore, error) {
	if client == nil {
		return nil, &StoreError{Category: "configuration"}
	}
	for _, table := range []string{
		config.RawTable,
		config.RawLocalTable,
		config.RunTable,
		config.RunLocalTable,
		config.CheckpointTable,
		config.CheckpointLocal,
		config.RunLatestView,
		config.CheckpointLatestView,
	} {
		if !tableIdentifierPattern.MatchString(strings.TrimSpace(table)) {
			return nil, &StoreError{Category: "configuration"}
		}
	}
	return &ClickHouseStore{Client: client, Config: config}, nil
}

type StoreError struct {
	Category string
}

func (e *StoreError) Error() string {
	category := strings.TrimSpace(e.Category)
	if category == "" {
		category = "unknown"
	}
	return "NLK ClickHouse operation failed category=" + category
}

func (s *ClickHouseStore) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateConnectionBoundary(); err != nil {
		return err
	}
	for _, table := range []string{
		s.Config.RawTable,
		s.Config.RawLocalTable,
		s.Config.RunTable,
		s.Config.RunLocalTable,
		s.Config.CheckpointTable,
		s.Config.CheckpointLocal,
		s.Config.RunLatestView,
		s.Config.CheckpointLatestView,
	} {
		if err := s.tableExists(ctx, table); err != nil {
			return err
		}
	}
	for _, table := range []string{
		s.Config.RawTable,
		s.Config.RawLocalTable,
		s.Config.RunTable,
		s.Config.RunLocalTable,
		s.Config.CheckpointTable,
		s.Config.CheckpointLocal,
	} {
		if err := s.checkGrant(ctx, "INSERT", table); err != nil {
			return err
		}
	}
	for _, table := range []string{
		s.Config.RawTable,
		s.Config.RawLocalTable,
		s.Config.RunTable,
		s.Config.RunLocalTable,
		s.Config.CheckpointTable,
		s.Config.CheckpointLocal,
		s.Config.RunLatestView,
		s.Config.CheckpointLatestView,
	} {
		if err := s.checkGrant(ctx, "SELECT", table); err != nil {
			return err
		}
	}
	return nil
}

func (s *ClickHouseStore) LoadCheckpoint(ctx context.Context, key nlkimport.CheckpointKey) (nlkimport.Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return nlkimport.Checkpoint{}, false, err
	}
	sql := fmt.Sprintf(`
		SELECT
			toString(checkpoint_uuid) AS checkpoint_uuid,
			toString(run_uuid) AS run_uuid,
			version,
			dataset_name,
			entry_crc32,
			entry_uncompressed_bytes,
			status,
			next_record_index,
			last_resource_id,
			records_parsed,
			records_inserted,
			records_rejected,
			attempts,
			content_hash,
			error_code,
			error_message,
			started_at,
			completed_at,
			updated_at,
			source
		FROM %s
		WHERE dataset_snapshot_date = toDate(%s)
		  AND source_archive = %s
		  AND source_entry = %s
		LIMIT 1
	`, s.Config.CheckpointLatestView,
		util.SQLString(key.SnapshotDate.Format("2006-01-02")),
		util.SQLString(key.Archive),
		util.SQLString(key.Entry),
	)
	rows, err := s.Client.QueryJSONEachRow(sql)
	if err != nil {
		return nlkimport.Checkpoint{}, false, &StoreError{Category: classifyError(err)}
	}
	if len(rows) == 0 {
		return nlkimport.Checkpoint{}, false, nil
	}
	row := rows[0]
	checkpoint := nlkimport.Checkpoint{
		CheckpointUUID:      strings.TrimSpace(util.ToString(row["checkpoint_uuid"])),
		RunUUID:             strings.TrimSpace(util.ToString(row["run_uuid"])),
		Version:             toUint64(row["version"]),
		DatasetSnapshotDate: key.SnapshotDate,
		DatasetName:         strings.TrimSpace(util.ToString(row["dataset_name"])),
		SourceArchive:       key.Archive,
		SourceEntry:         key.Entry,
		EntryCRC32:          strings.TrimSpace(util.ToString(row["entry_crc32"])),
		EntryUncompressed:   toUint64(row["entry_uncompressed_bytes"]),
		Status:              strings.TrimSpace(util.ToString(row["status"])),
		NextRecordIndex:     toUint64(row["next_record_index"]),
		LastResourceID:      strings.TrimSpace(util.ToString(row["last_resource_id"])),
		RecordsParsed:       toUint64(row["records_parsed"]),
		RecordsInserted:     toUint64(row["records_inserted"]),
		RecordsRejected:     toUint64(row["records_rejected"]),
		Attempts:            uint16(toUint64(row["attempts"])),
		ContentHash:         strings.TrimSpace(util.ToString(row["content_hash"])),
		ErrorCode:           strings.TrimSpace(util.ToString(row["error_code"])),
		ErrorMessage:        strings.TrimSpace(util.ToString(row["error_message"])),
		Source:              strings.TrimSpace(util.ToString(row["source"])),
	}
	if parsed := parseTime(util.ToString(row["started_at"])); !parsed.IsZero() {
		checkpoint.StartedAt = &parsed
	}
	if parsed := parseTime(util.ToString(row["completed_at"])); !parsed.IsZero() {
		checkpoint.CompletedAt = &parsed
	}
	checkpoint.UpdatedAt = parseTime(util.ToString(row["updated_at"]))
	return checkpoint, true, nil
}

func (s *ClickHouseStore) InsertRawRows(ctx context.Context, rows []map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	token, err := rawLineageDedupToken(s.Config.RawTable, rows)
	if err != nil {
		return &StoreError{Category: "contract"}
	}
	if err := s.Client.InsertJSONEachRowDurable(s.Config.RawTable, rows, token); err != nil {
		return &StoreError{Category: classifyError(err)}
	}
	return nil
}

func (s *ClickHouseStore) ExistingRawRecordIndexes(
	ctx context.Context,
	lineage nlkimport.RawLineage,
	indexes []uint64,
) (map[uint64]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	indexes = uniqueSortedIndexes(indexes)
	out := make(map[uint64]struct{}, len(indexes))
	if len(indexes) == 0 {
		return out, nil
	}
	for start := 0; start < len(indexes); start += existingRawIndexLookupChunkSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+existingRawIndexLookupChunkSize, len(indexes))
		values := make([]string, 0, end-start)
		for _, index := range indexes[start:end] {
			values = append(values, strconv.FormatUint(index, 10))
		}
		sql := fmt.Sprintf(`
			SELECT source_record_index
			FROM %s
			WHERE dataset_snapshot_date = toDate(%s)
			  AND dataset_name = %s
			  AND source_archive = %s
			  AND source_entry = %s
			  AND source_record_index IN (%s)
			GROUP BY source_record_index
			SETTINGS max_threads = 1, max_execution_time = 30
		`, s.Config.RawTable,
			util.SQLString(lineage.SnapshotDate.Format("2006-01-02")),
			util.SQLString(lineage.DatasetName),
			util.SQLString(lineage.Archive),
			util.SQLString(lineage.Entry),
			strings.Join(values, ", "),
		)
		rows, err := s.Client.QueryJSONEachRow(sql)
		if err != nil {
			return nil, &StoreError{Category: classifyError(err)}
		}
		for _, row := range rows {
			out[toUint64(row["source_record_index"])] = struct{}{}
		}
	}
	return out, nil
}

func (s *ClickHouseStore) SaveRun(ctx context.Context, state nlkimport.RunState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	row := map[string]any{
		"run_uuid":              state.RunUUID,
		"version":               state.Version,
		"status":                state.Status,
		"dataset_snapshot_date": state.DatasetSnapshotDate.Format("2006-01-02"),
		"dataset_updated_at":    util.FormatCHDateTime64Millis(state.DatasetUpdatedAt),
		"source_url":            state.SourceURL,
		"license_name":          state.LicenseName,
		"license_url":           state.LicenseURL,
		"attribution":           state.Attribution,
		"archives_total":        state.ArchivesTotal,
		"archives_completed":    state.ArchivesCompleted,
		"entries_total":         state.EntriesTotal,
		"entries_completed":     state.EntriesCompleted,
		"records_parsed":        state.RecordsParsed,
		"records_inserted":      state.RecordsInserted,
		"records_rejected":      state.RecordsRejected,
		"bytes_compressed":      state.BytesCompressed,
		"bytes_uncompressed":    state.BytesUncompressed,
		"importer_version":      state.ImporterVersion,
		"error_code":            state.ErrorCode,
		"error_message":         boundedError(state.ErrorMessage),
		"started_at":            util.FormatCHDateTime64Millis(state.StartedAt),
		"finished_at":           nullableTime(state.FinishedAt),
		"heartbeat_at":          util.FormatCHDateTime64Millis(state.HeartbeatAt),
		"source":                state.Source,
	}
	token := stableDedupToken(
		s.Config.RunTable,
		"run",
		state.RunUUID,
		strconv.FormatUint(state.Version, 10),
	)
	if err := s.Client.InsertJSONEachRowDurable(s.Config.RunTable, []map[string]any{row}, token); err != nil {
		return &StoreError{Category: classifyError(err)}
	}
	return nil
}

func (s *ClickHouseStore) SaveCheckpoint(ctx context.Context, state nlkimport.Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	row := map[string]any{
		"checkpoint_uuid":          state.CheckpointUUID,
		"run_uuid":                 state.RunUUID,
		"version":                  state.Version,
		"dataset_snapshot_date":    state.DatasetSnapshotDate.Format("2006-01-02"),
		"dataset_name":             state.DatasetName,
		"source_archive":           state.SourceArchive,
		"source_entry":             state.SourceEntry,
		"entry_crc32":              state.EntryCRC32,
		"entry_uncompressed_bytes": state.EntryUncompressed,
		"status":                   state.Status,
		"next_record_index":        state.NextRecordIndex,
		"last_resource_id":         state.LastResourceID,
		"records_parsed":           state.RecordsParsed,
		"records_inserted":         state.RecordsInserted,
		"records_rejected":         state.RecordsRejected,
		"attempts":                 state.Attempts,
		"content_hash":             state.ContentHash,
		"error_code":               state.ErrorCode,
		"error_message":            boundedError(state.ErrorMessage),
		"started_at":               nullableTime(state.StartedAt),
		"completed_at":             nullableTime(state.CompletedAt),
		"updated_at":               util.FormatCHDateTime64Millis(state.UpdatedAt),
		"source":                   state.Source,
	}
	token := stableDedupToken(
		s.Config.CheckpointTable,
		"checkpoint",
		state.DatasetSnapshotDate.Format("2006-01-02"),
		state.SourceArchive,
		state.SourceEntry,
		strconv.FormatUint(state.Version, 10),
	)
	if err := s.Client.InsertJSONEachRowDurable(s.Config.CheckpointTable, []map[string]any{row}, token); err != nil {
		return &StoreError{Category: classifyError(err)}
	}
	return nil
}

func rawLineageDedupToken(table string, rows []map[string]any) (string, error) {
	if len(rows) == 0 {
		return "", fmt.Errorf("empty raw insert")
	}
	lineages := make([]string, 0, len(rows))
	for _, row := range rows {
		snapshot, snapshotOK := requiredString(row, "dataset_snapshot_date")
		archive, archiveOK := requiredString(row, "source_archive")
		entry, entryOK := requiredString(row, "source_entry")
		indexes := rawRecordIndex(row)
		if !snapshotOK || !archiveOK || !entryOK || len(indexes) != 1 {
			return "", fmt.Errorf("invalid raw lineage")
		}
		lineages = append(lineages, strings.Join([]string{
			snapshot,
			archive,
			entry,
			strconv.FormatUint(indexes[0], 10),
		}, "\x1f"))
	}
	sort.Strings(lineages)
	return stableDedupToken(append([]string{table, "raw_lineage"}, lineages...)...), nil
}

func requiredString(row map[string]any, key string) (string, bool) {
	value, found := row[key]
	if !found || value == nil {
		return "", false
	}
	cleaned := strings.TrimSpace(fmt.Sprint(value))
	return cleaned, cleaned != ""
}

func stableDedupToken(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x1e")))
	return fmt.Sprintf("%x", sum[:])
}

func rawRecordIndex(row map[string]any) []uint64 {
	switch value := row["source_record_index"].(type) {
	case uint64:
		return []uint64{value}
	case uint:
		return []uint64{uint64(value)}
	case int:
		if value >= 0 {
			return []uint64{uint64(value)}
		}
	case int64:
		if value >= 0 {
			return []uint64{uint64(value)}
		}
	}
	return nil
}

func uniqueSortedIndexes(indexes []uint64) []uint64 {
	if len(indexes) == 0 {
		return nil
	}
	out := append([]uint64(nil), indexes...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	write := 1
	for read := 1; read < len(out); read++ {
		if out[read] == out[write-1] {
			continue
		}
		out[write] = out[read]
		write++
	}
	return out[:write]
}

func (s *ClickHouseStore) tableExists(ctx context.Context, table string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	exists, err := s.Client.TableExists(table)
	if err != nil {
		return &StoreError{Category: classifyError(err)}
	}
	if !exists {
		return &StoreError{Category: "contract"}
	}
	return nil
}

func (s *ClickHouseStore) checkGrant(ctx context.Context, privilege, table string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.Client.Exec(fmt.Sprintf("CHECK GRANT %s ON %s", privilege, table)); err != nil {
		return &StoreError{Category: classifyError(err)}
	}
	return nil
}

func (s *ClickHouseStore) validateConnectionBoundary() error {
	host := strings.TrimSpace(s.Client.Host)
	parsedHost := host
	protocol := strings.ToLower(strings.TrimSpace(s.Client.Protocol))
	if parsed, err := url.Parse(host); err == nil && parsed.Host != "" {
		parsedHost = parsed.Hostname()
		protocol = strings.ToLower(parsed.Scheme)
	} else if splitHost, _, err := net.SplitHostPort(host); err == nil {
		parsedHost = splitHost
	}
	parsedHost = strings.Trim(parsedHost, "[]")
	if parsedHost == "" || parsedHost == "localhost" || parsedHost == "0.0.0.0" || parsedHost == "::" {
		return &StoreError{Category: "connection_boundary"}
	}
	if ip := net.ParseIP(parsedHost); ip != nil && ip.IsLoopback() {
		return &StoreError{Category: "connection_boundary"}
	}
	if s.Config.RequireHTTPS && protocol != "https" {
		return &StoreError{Category: "connection_boundary"}
	}
	return nil
}

func classifyError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"timeout", "deadline", "connection reset", "connection refused",
		"not initialized", "keeper", "coordination", "readonly",
		"http status=429", "http status=500", "http status=502",
		"http status=503", "http status=504",
	} {
		if strings.Contains(message, marker) {
			return "transient"
		}
	}
	return "contract"
}

func boundedError(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	const maxBytes = 256
	if len(value) > maxBytes {
		value = value[:maxBytes]
	}
	return value
}

func nullableTime(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return util.FormatCHDateTime64Millis(*value)
}

func parseTime(value string) time.Time {
	value = strings.TrimSpace(value)
	for _, layout := range []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	} {
		if parsed, err := time.ParseInLocation(layout, value, util.KST()); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func toUint64(value any) uint64 {
	parsed := util.ToInt64(value)
	if parsed < 0 {
		return 0
	}
	return uint64(parsed)
}

func boolEnv(name string, fallback bool) bool {
	defaultValue := "false"
	if fallback {
		defaultValue = "true"
	}
	switch strings.ToLower(strings.TrimSpace(envx.String(name, defaultValue))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
