package kakaostore

import (
	"context"
	"errors"
	"fmt"

	"statground_naver_book_go/internal/util"
)

type sourceVisibilityClient interface {
	QueryJSONEachRowContext(context.Context, string) ([]map[string]any, error)
}

// ConfirmCollectionSource observes the durable source only. Accepted outbox
// delivery is not proof that raw rows are visible to the native refresh owner.
// This gate never replays an outbox or mutates its rows.
func ConfirmCollectionSource(ctx context.Context, client sourceVisibilityClient, rawTable, outboxTable, runUUID string, expectedRows int, expectedLatest int64) (latest, observed int64, err error) {
	if !tableIdentifierPattern.MatchString(rawTable) || !tableIdentifierPattern.MatchString(outboxTable) || !uuidPattern.MatchString(runUUID) || expectedRows < 0 || expectedLatest < 0 {
		return 0, 0, errors.New("book collection source contract invalid")
	}
	pending, err := client.QueryJSONEachRowContext(ctx, "SELECT 1 AS pending FROM "+outboxTable+" WHERE replayed_at IS NULL LIMIT 1 SETTINGS max_threads=1,max_execution_time=5")
	if err != nil || len(pending) != 0 {
		return 0, 0, &StoreError{Operation: "source_confirmation", Category: "clickhouse_transient", Reason: "outbox_not_drained"}
	}
	query := fmt.Sprintf("SELECT uniqExact(uuid) AS confirmed_rows,ifNull(toUnixTimestamp64Milli(max(collected_at)),0) AS confirmed_source_millis,toUnixTimestamp64Milli(now64(3)) AS observed_millis FROM %s WHERE run_uuid=toUUID(%s) SETTINGS max_threads=1,max_execution_time=10", rawTable, util.SQLString(runUUID))
	rows, err := client.QueryJSONEachRowContext(ctx, query)
	if err != nil || len(rows) != 1 || rows[0]["confirmed_rows"] == nil || rows[0]["confirmed_source_millis"] == nil || rows[0]["observed_millis"] == nil {
		return 0, 0, &StoreError{Operation: "source_confirmation", Category: "clickhouse_transient", Reason: "source_visibility_unverified"}
	}
	confirmedRows, countOK := util.NonnegativeInteger(rows[0]["confirmed_rows"])
	latest, latestOK := util.NonnegativeInteger(rows[0]["confirmed_source_millis"])
	observed, observedOK := util.NonnegativeInteger(rows[0]["observed_millis"])
	if !countOK || !latestOK || !observedOK || confirmedRows != int64(expectedRows) || latest != expectedLatest || observed <= 0 || latest > observed || expectedRows > 0 && latest == 0 {
		return 0, 0, &StoreError{Operation: "source_confirmation", Category: "clickhouse_transient", Reason: "source_visibility_unverified"}
	}
	return latest, observed, nil
}
