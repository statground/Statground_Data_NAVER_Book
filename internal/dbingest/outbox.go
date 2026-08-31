package dbingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/util"
)

const (
	maxOutboxRows            = 2000
	maxOutboxRowsJSONBytes   = 16 * 1024 * 1024
	outboxPersistenceTimeout = 30 * time.Second
)

var (
	writerClickHouseCodePattern = regexp.MustCompile(`\bcode=([0-9]+)\b`)
	lowerSHA256Pattern          = regexp.MustCompile(`^[0-9a-f]{64}$`)
	outboxUUIDPattern           = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

type operationError struct {
	operation string
	category  string
	reason    string
}

func (e *operationError) Error() string {
	message := fmt.Sprintf("NAVER ClickHouse operation failed operation=%s category=%s", e.operation, e.category)
	if e.reason != "" {
		message += " reason=" + e.reason
	}
	return message
}

// IsDurabilityError lets orchestration keep provider/network failures
// best-effort without ever suppressing a target-plus-outbox persistence
// failure.
func IsDurabilityError(err error) bool {
	var target *operationError
	return errors.As(err, &target)
}

func (w *Writer) insertRowsWithOutbox(ctx context.Context, operation, targetTable, targetLocalTable string, rows []map[string]any) error {
	if w == nil || w.Client == nil {
		return &operationError{operation: operation, category: "clickhouse_contract", reason: "not_configured"}
	}
	if len(rows) == 0 {
		return nil
	}
	canonicalRows, rowsJSON, token, err := encodeOutboxRows(targetTable, rows)
	if err != nil {
		return &operationError{operation: operation, category: "clickhouse_contract", reason: "payload_rejected"}
	}
	if err := w.Client.InsertJSONEachRowSynchronousContext(ctx, targetTable, canonicalRows, token); err == nil {
		return nil
	} else if !retryableWriterError(err) && !ch.IsAmbiguousInsertError(err) {
		return writerOperationError(operation, err)
	} else {
		outboxCtx, cancel := context.WithTimeout(context.Background(), outboxPersistenceTimeout)
		defer cancel()
		if outboxErr := w.enqueueOutbox(outboxCtx, targetTable, targetLocalTable, rowsJSON, len(rows), token, err); outboxErr != nil {
			return writerOperationError("enqueue_outbox", outboxErr)
		}
	}
	return nil
}

func encodeOutboxRows(targetTable string, rows []map[string]any) ([]map[string]any, string, string, error) {
	if len(rows) < 1 || len(rows) > maxOutboxRows {
		return nil, "", "", fmt.Errorf("outbox row count is outside the bounded contract")
	}
	columns, canonicalRows, payload, err := ch.CanonicalJSONEachRow(rows)
	if err != nil {
		return nil, "", "", err
	}
	rowsJSON, err := ch.JSONEachRowPayloadAsArray(payload)
	if err != nil {
		return nil, "", "", err
	}
	if len(rowsJSON) == 0 || len(rowsJSON) > maxOutboxRowsJSONBytes || len(payload) > maxOutboxRowsJSONBytes {
		return nil, "", "", fmt.Errorf("outbox rows payload is outside the bounded size")
	}
	token := ch.JSONEachRowDeduplicationToken(targetTable, columns, payload)
	return canonicalRows, string(rowsJSON), token, nil
}

func decodeOutboxRows(rowsJSON string, expectedCount int) ([]map[string]any, error) {
	if len(rowsJSON) == 0 || len(rowsJSON) > maxOutboxRowsJSONBytes || expectedCount < 1 || expectedCount > maxOutboxRows {
		return nil, fmt.Errorf("outbox rows payload is outside the bounded contract")
	}
	decoder := json.NewDecoder(strings.NewReader(rowsJSON))
	decoder.UseNumber()
	var rows []map[string]any
	if err := decoder.Decode(&rows); err != nil {
		return nil, err
	}
	if len(rows) != expectedCount {
		return nil, fmt.Errorf("outbox row count mismatch")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("outbox payload contains trailing values")
	}
	return rows, nil
}

func (w *Writer) enqueueOutbox(ctx context.Context, targetTable, targetLocalTable, rowsJSON string, rowCount int, token string, sourceErr error) error {
	row := map[string]any{
		"outbox_uuid":         util.UUIDv7(),
		"created_at":          util.FormatCHDateTime64Millis(util.NowKST()),
		"target_table":        targetTable,
		"target_local_table":  targetLocalTable,
		"rows_json":           rowsJSON,
		"row_count":           rowCount,
		"deduplication_token": token,
		"source_error":        safeWriterErrorReason(sourceErr),
	}
	// This target is an endpoint-local MergeTree, never a Distributed table.
	return w.Client.InsertJSONEachRowContext(ctx, w.Cfg.OutboxTable, []map[string]any{row})
}

func (w *Writer) replayOutbox(ctx context.Context) error {
	limit := w.Cfg.OutboxReplayLimit
	if limit < 1 || limit > 100 {
		limit = 25
	}
	outboxTable, err := ch.QualifiedTableIdentifier(w.Cfg.OutboxTable, w.Client.Database)
	if err != nil {
		return &operationError{operation: "replay_outbox", category: "clickhouse_contract", reason: "query_rejected"}
	}
	query := fmt.Sprintf(`
        SELECT
            toString(outbox_uuid) AS outbox_uuid,
            target_table,
            target_local_table,
            rows_json,
            row_count,
            deduplication_token
        FROM %s
        WHERE replayed_at IS NULL
        ORDER BY created_at ASC, outbox_uuid ASC
        LIMIT %d
        SETTINGS max_threads = 1, max_execution_time = 10
    `, outboxTable, limit+1)
	pending, err := w.Client.QueryJSONEachRowContext(ctx, query)
	if err != nil {
		return writerOperationError("replay_outbox", err)
	}
	if len(pending) == 0 {
		return nil
	}
	overLimit := len(pending) > limit
	if overLimit {
		pending = pending[:limit]
	}
	health := make(map[string]bool)
	replayedUUIDs := make([]string, 0, len(pending))
	markThenReturn := func(replayErr error) error {
		if len(replayedUUIDs) > 0 {
			if markErr := w.markOutboxReplayed(ctx, replayedUUIDs); markErr != nil {
				return writerOperationError("mark_outbox", markErr)
			}
		}
		return replayErr
	}
	for _, record := range pending {
		outboxUUID := strings.TrimSpace(util.ToString(record["outbox_uuid"]))
		targetTable := strings.TrimSpace(util.ToString(record["target_table"]))
		targetLocalTable := strings.TrimSpace(util.ToString(record["target_local_table"]))
		token := strings.TrimSpace(util.ToString(record["deduplication_token"]))
		rowsJSON := util.ToString(record["rows_json"])
		rowCount := int(util.ToInt64(record["row_count"]))
		if !outboxUUIDPattern.MatchString(outboxUUID) || !lowerSHA256Pattern.MatchString(token) ||
			!w.allowedReplayTarget(targetTable, targetLocalTable) {
			return markThenReturn(&operationError{operation: "replay_outbox", category: "clickhouse_contract", reason: "query_rejected"})
		}
		rows, decodeErr := decodeOutboxRows(rowsJSON, rowCount)
		if decodeErr != nil {
			return markThenReturn(&operationError{operation: "replay_outbox", category: "clickhouse_contract", reason: "payload_rejected"})
		}
		canonicalRows, _, recomputedToken, encodeErr := encodeOutboxRows(targetTable, rows)
		if encodeErr != nil || recomputedToken != token {
			return markThenReturn(&operationError{operation: "replay_outbox", category: "clickhouse_contract", reason: "payload_rejected"})
		}
		rows = canonicalRows
		healthy, checked := health[targetLocalTable]
		if !checked {
			healthy, err = w.localReplicaHealthy(ctx, targetLocalTable)
			if err != nil {
				return markThenReturn(writerOperationError("replay_outbox", err))
			}
			health[targetLocalTable] = healthy
		}
		if !healthy {
			return markThenReturn(&operationError{operation: "replay_outbox", category: "clickhouse_transient", reason: "replica_unhealthy"})
		}
		state, reconcileErr := w.reconcileOutboxRows(ctx, targetTable, targetLocalTable, rows)
		if reconcileErr != nil {
			return markThenReturn(writerOperationError("reconcile_outbox", reconcileErr))
		}
		if len(state.MissingRows) > 0 {
			missingRows, _, missingToken, subsetErr := encodeOutboxRows(targetTable, state.MissingRows)
			if subsetErr != nil {
				return markThenReturn(&operationError{operation: "replay_outbox", category: "clickhouse_contract", reason: "payload_rejected"})
			}
			if err := w.Client.InsertJSONEachRowSynchronousContext(ctx, targetTable, missingRows, missingToken); err != nil {
				return markThenReturn(writerOperationError("replay_outbox", err))
			}
			confirmed, confirmErr := w.reconcileOutboxRows(ctx, targetTable, targetLocalTable, rows)
			if confirmErr != nil {
				return markThenReturn(writerOperationError("confirm_outbox", confirmErr))
			}
			if len(confirmed.MissingRows) != 0 {
				return markThenReturn(&operationError{operation: "confirm_outbox", category: "clickhouse_transient", reason: "request_failed"})
			}
		}
		replayedUUIDs = append(replayedUUIDs, outboxUUID)
	}
	if err := markThenReturn(nil); err != nil {
		return err
	}
	if overLimit {
		return &operationError{operation: "replay_outbox", category: "clickhouse_transient", reason: "backlog_limit"}
	}
	return nil
}

func (w *Writer) reconcileOutboxRows(ctx context.Context, targetTable, targetLocalTable string, rows []map[string]any) (ch.ReconcileResult, error) {
	spec := ch.ReconcileSpec{Cluster: "statground_cluster", LocalTable: targetLocalTable, MaxRows: maxOutboxRows}
	switch targetTable {
	case w.Cfg.RawTable:
		spec.KeyColumns = []ch.ReconcileKeyColumn{
			{Name: "created_at", Kind: ch.ReconcileDateTime64},
			{Name: "provider", Kind: ch.ReconcileString},
			{Name: "isbn", Kind: ch.ReconcileString},
		}
		spec.VersionColumn = "version"
	case w.Cfg.CollectLogTable:
		spec.KeyColumns = []ch.ReconcileKeyColumn{{Name: "event_uuid", Kind: ch.ReconcileUUID}}
	case w.Cfg.PublisherCacheTable:
		spec.KeyColumns = []ch.ReconcileKeyColumn{
			{Name: "collected_at", Kind: ch.ReconcileDateTime64},
			{Name: "run_uuid", Kind: ch.ReconcileUUID},
			{Name: "publisher", Kind: ch.ReconcileString},
		}
	default:
		return ch.ReconcileResult{}, fmt.Errorf("target is not allowlisted for reconciliation")
	}
	return w.Client.ReconcileJSONEachRowContext(ctx, spec, rows)
}

func (w *Writer) allowedReplayTarget(targetTable, targetLocalTable string) bool {
	allowed := map[string]string{
		w.Cfg.RawTable:            w.Cfg.RawLocalTable,
		w.Cfg.CollectLogTable:     w.Cfg.CollectLogLocalTable,
		w.Cfg.PublisherCacheTable: w.Cfg.PublisherCacheLocal,
	}
	wantLocal, ok := allowed[targetTable]
	return ok && wantLocal == targetLocalTable
}

func (w *Writer) localReplicaHealthy(ctx context.Context, localTable string) (bool, error) {
	database, table := ch.SplitQualifiedTable(localTable, w.Client.Database)
	query := fmt.Sprintf(`
        SELECT
            count() AS replicas,
            countIf(
                is_readonly = 0
                AND is_session_expired = 0
                AND parts_to_check = 0
                AND queue_size <= %d
				AND absolute_delay <= %d
            ) AS writable
        FROM system.replicas
        WHERE database = %s
          AND table = %s
        SETTINGS max_threads = 1, max_execution_time = 5
    `, w.Cfg.OutboxMaxReplicaQueue, w.Cfg.OutboxMaxReplicaDelaySeconds, util.SQLString(database), util.SQLString(table))
	row, err := w.Client.QuerySingleRowContext(ctx, query)
	if err != nil {
		return false, err
	}
	return util.ToInt64(row["replicas"]) == 1 && util.ToInt64(row["writable"]) == 1, nil
}

func (w *Writer) markOutboxReplayed(ctx context.Context, outboxUUIDs []string) error {
	if len(outboxUUIDs) == 0 {
		return nil
	}
	outboxTable, err := ch.QualifiedTableIdentifier(w.Cfg.OutboxTable, w.Client.Database)
	if err != nil {
		return err
	}
	values := make([]string, 0, len(outboxUUIDs))
	for _, outboxUUID := range outboxUUIDs {
		if !outboxUUIDPattern.MatchString(outboxUUID) {
			return fmt.Errorf("invalid outbox UUID")
		}
		values = append(values, "toUUID("+util.SQLString(outboxUUID)+")")
	}
	// This recovery-only path is not used by healthy first inserts. One
	// synchronous mutation marks the whole bounded replay run (at most 100 rows,
	// 25 by default); replay never creates one mutation per outbox row.
	query := fmt.Sprintf(`
        ALTER TABLE %s
        UPDATE replayed_at = now64(3, 'Asia/Seoul')
        WHERE outbox_uuid IN (%s)
        SETTINGS mutations_sync = 1
    `, outboxTable, strings.Join(values, ", "))
	return w.Client.ExecContext(ctx, query)
}

func writerOperationError(operation string, err error) error {
	category := "clickhouse_contract"
	if retryableWriterError(err) || ch.IsAmbiguousInsertError(err) {
		category = "clickhouse_transient"
	}
	return &operationError{operation: operation, category: category, reason: safeWriterErrorReason(err)}
}

func safeWriterErrorReason(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToLower(err.Error())
	if code := writerClickHouseErrorCode(message); code != 0 {
		switch code {
		case 497, 516:
			return "auth_or_permission"
		case 6, 27, 47, 60, 62, 81:
			return "query_rejected"
		}
	}
	for marker, reason := range map[string]string{
		"http status=400": "query_rejected",
		"http status=401": "auth_or_permission",
		"http status=403": "auth_or_permission",
		"http status=404": "object_unavailable",
		"http status=408": "read_timeout",
		"http status=429": "query_admission",
		"http status=500": "server_unavailable",
		"http status=502": "server_unavailable",
		"http status=503": "server_unavailable",
		"http status=504": "server_unavailable",
	} {
		if strings.Contains(message, marker) {
			return reason
		}
	}
	for _, marker := range []string{"timeout", "deadline", "context canceled"} {
		if strings.Contains(message, marker) {
			return "transport_timeout"
		}
	}
	for _, marker := range []string{"connection reset", "connection refused", "connection aborted", "broken pipe", "unexpected eof"} {
		if strings.Contains(message, marker) {
			return "transport_interrupted"
		}
	}
	return "request_failed"
}

func retryableWriterError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary()) {
		return true
	}
	message := strings.ToLower(err.Error())
	if code := writerClickHouseErrorCode(message); code != 0 {
		switch code {
		case 6, 27, 47, 60, 62, 81, 497, 516:
			return false
		case 159, 164, 202, 203, 209, 210, 225, 241, 242, 243, 244, 252, 254, 255,
			265, 279, 285, 286, 289, 297, 319, 364, 369, 384, 394, 410, 415, 416,
			425, 439, 473, 519, 574, 667, 677, 692, 700, 722, 733, 734, 735, 738,
			745, 749, 762, 999:
			return true
		}
	}
	for _, marker := range []string{
		"timeout", "deadline", "connection reset", "connection refused",
		"connection aborted", "broken pipe", "unexpected eof",
		"not initialized", "keeper", "coordination", "readonly", "read-only",
		"temporarily unavailable", "http status=408", "http status=429",
		"http status=500", "http status=502", "http status=503", "http status=504",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func writerClickHouseErrorCode(message string) int {
	matches := writerClickHouseCodePattern.FindStringSubmatch(strings.ToLower(message))
	if len(matches) != 2 {
		return 0
	}
	code, _ := strconv.Atoi(matches[1])
	return code
}
