package bookcatalogpublish

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/nlkbackfill"
	"statground_naver_book_go/internal/util"
)

const InputTable = "Data_Book_Service.v_book_public_catalog_input"
const SnapshotTable = "Data_Book_Service.book_public_catalog_snapshot"
const MarkerTable = "Data_Book_Service.book_public_catalog_published_batch"
const rawCheckpoints = "Data_Book_NLK_Log.v_nlk_import_entry_checkpoint_latest"
const projectionCheckpoints = "Data_Book_NLK_Log.v_nlk_service_projection_checkpoint_latest"
const bibliographyTable = "Data_Book_NLK_Service.v_bibliography_current"
const readSettings = "max_threads = 2, max_execution_time = 600, max_memory_usage = 2147483648, max_bytes_before_external_group_by = 268435456, max_bytes_before_external_sort = 268435456"
const columns = "catalog_key, record_id, canonical_isbn, isbn, uuid, title, author, publisher, pubdate, description, image, link, updated_at, collected_at, resource_type, kdc, subjects, languages, publication_places, series, extents, browse_key, search_text, row_fingerprint"

type ClickHouse struct {
	Client   *ch.Client
	Endpoint string
}

func (s *ClickHouse) query(ctx context.Context, sql string) ([]map[string]any, error) {
	if strings.HasPrefix(strings.TrimSpace(sql), "SELECT ") {
		// A startup host probe cannot fence later requests through a changing
		// proxy. An uncorrelated scalar subquery runs the guard on the initiating
		// endpoint, before distributed source reads are planned on other shards.
		sql = "SELECT * FROM (" + sql + ") WHERE " + s.hostGuard() + " = 0"
	}
	rows, err := s.Client.QueryJSONEachRowContext(ctx, sql)
	if err != nil {
		return nil, fail("database_read")
	}
	return rows, nil
}

func (s *ClickHouse) hostGuard() string {
	return "(SELECT throwIf(hostName() != " + util.SQLString(s.Endpoint) + ", 'book_catalog_endpoint_mismatch'))"
}
func (s *ClickHouse) Validate(ctx context.Context) error {
	if s.Client == nil || s.Endpoint == "" {
		return fail("configuration")
	}
	protocol := s.Client.Protocol
	if strings.Contains(s.Client.Host, "://") {
		u, e := url.Parse(s.Client.Host)
		if e != nil || u.User != nil {
			return fail("endpoint")
		}
		protocol = u.Scheme
	}
	if protocol != "https" {
		return fail("https_required")
	}
	if s.Client.ValidateDirectEndpointHostnameContext(ctx, s.Endpoint) != nil {
		return fail("endpoint_identity")
	}
	for _, table := range []string{InputTable, SnapshotTable, MarkerTable, rawCheckpoints, projectionCheckpoints, bibliographyTable} {
		exists, e := s.Client.TableExistsContext(ctx, table)
		if e != nil || !exists {
			return fail("missing_dependency")
		}
		rows, e := s.query(ctx, "CHECK GRANT SELECT ON "+table)
		if e != nil || len(rows) != 1 || uintValue(rows[0]["result"]) != 1 {
			return fail("select_grant")
		}
	}
	for _, table := range []string{SnapshotTable, MarkerTable} {
		rows, e := s.query(ctx, "CHECK GRANT INSERT ON "+table)
		if e != nil || len(rows) != 1 || uintValue(rows[0]["result"]) != 1 {
			return fail("insert_grant")
		}
	}
	rows, e := s.query(ctx, "SELECT name, engine FROM system.tables WHERE database = 'Data_Book_Service' AND name IN ('book_public_catalog_snapshot', 'book_public_catalog_published_batch') AND is_temporary = 0 SETTINGS max_threads = 1, max_execution_time = 10")
	if e != nil || len(rows) != 2 {
		return fail("target_engine")
	}
	for _, row := range rows {
		if util.ToString(row["engine"]) != "MergeTree" {
			return fail("target_engine")
		}
	}
	return nil
}
func uintValue(value any) uint64 { v, _ := strconv.ParseUint(util.ToString(value), 10, 64); return v }
func exactUint(value any) (uint64, error) {
	s, ok := value.(string)
	if !ok {
		return 0, fail("integer_encoding")
	}
	n, e := strconv.ParseUint(s, 10, 64)
	if e != nil {
		return 0, fail("integer_encoding")
	}
	return n, nil
}
func (s *ClickHouse) Coverage(ctx context.Context, c Config) error {
	lineages, e := c.Manifest.Lineages(c.SnapshotDate)
	if e != nil || len(lineages) != ExpectedEntries {
		return fail("manifest_coverage")
	}
	date := util.SQLString(c.SnapshotDate.Format("2006-01-02"))
	rows, e := s.query(ctx, "SELECT dataset_name, source_archive, source_entry, source_revision, status, content_hash, entry_crc32, toString(entry_uncompressed_bytes) AS bytes, toString(next_record_index) AS next, toString(records_parsed) AS parsed, toString(records_inserted) AS inserted, toString(records_rejected) AS rejected FROM "+rawCheckpoints+" WHERE dataset_snapshot_date = toDate("+date+") LIMIT 209 SETTINGS max_threads = 1, max_execution_time = 30")
	if e != nil || len(rows) != ExpectedEntries {
		return fail("raw_coverage")
	}
	raw := map[string]map[string]any{}
	for _, row := range rows {
		key := util.ToString(row["source_archive"]) + "\x00" + util.ToString(row["source_entry"])
		if raw[key] != nil {
			return fail("raw_duplicate")
		}
		raw[key] = row
	}
	projectionRows, e := s.query(ctx, "SELECT dataset_name, source_archive, source_entry, `projection`, status, toString(next_record_index) AS next FROM "+projectionCheckpoints+" WHERE dataset_snapshot_date = toDate("+date+") AND transform_version = "+util.SQLString(c.TransformVersion)+" LIMIT 1457 SETTINGS max_threads = 1, max_execution_time = 30")
	if e != nil || len(projectionRows) > ExpectedEntries*7 {
		return fail("projection_coverage")
	}
	projected := map[string]map[string]any{}
	for _, row := range projectionRows {
		key := util.ToString(row["source_archive"]) + "\x00" + util.ToString(row["source_entry"]) + "\x00" + util.ToString(row["projection"])
		if projected[key] != nil {
			return fail("projection_duplicate")
		}
		projected[key] = row
	}
	for _, lineage := range lineages {
		key := lineage.SourceArchive + "\x00" + lineage.SourceEntry
		row := raw[key]
		bytes, be := exactUint(row["bytes"])
		next, ne := exactUint(row["next"])
		parsed, pe := exactUint(row["parsed"])
		inserted, ie := exactUint(row["inserted"])
		rejected, re := exactUint(row["rejected"])
		if be != nil || ne != nil || pe != nil || ie != nil || re != nil || bytes != lineage.UncompressedBytes || parsed == 0 || parsed != inserted || rejected != 0 || next != parsed || util.ToString(row["dataset_name"]) != lineage.DatasetName || util.ToString(row["status"]) != "succeeded" || lineage.SourceRevision == "" || util.ToString(row["source_revision"]) != lineage.SourceRevision || !digestPattern.MatchString(util.ToString(row["content_hash"])) || !crcPattern.MatchString(util.ToString(row["entry_crc32"])) {
			return fail("raw_coverage")
		}
		for _, projection := range nlkbackfill.DefaultProjections() {
			applies, e := nlkbackfill.ProjectionAppliesToDataset(projection, lineage.DatasetName)
			if e != nil {
				return fail("projection_contract")
			}
			if !applies {
				continue
			}
			p := projected[key+"\x00"+string(projection)]
			pnext, e := exactUint(p["next"])
			if e != nil || pnext != next || util.ToString(p["dataset_name"]) != lineage.DatasetName || util.ToString(p["status"]) != "succeeded" {
				return fail("projection_coverage")
			}
		}
	}
	// File/projection completion cannot compensate for a stale or incomplete
	// merged ISBN catalog. Every eligible NLK canonical ISBN must reach input.
	missing, e := s.query(ctx, `SELECT toString(count()) AS missing FROM
 (SELECT DISTINCT arrayJoin(arrayFilter(x -> notEmpty(x), canonical_isbns)) AS canonical_isbn
 FROM `+bibliographyTable+` WHERE resource_type IN ('book','offline_material','online_material','audiovisual','government_publication','serial','thesis') AND (notEmpty(title) OR notEmpty(label))) AS n
 LEFT ANTI JOIN (SELECT canonical_isbn FROM `+InputTable+` WHERE notEmpty(canonical_isbn)) AS c USING canonical_isbn
 SETTINGS `+readSettings+`, distributed_product_mode = 'global'`)
	if e != nil || len(missing) != 1 {
		return fail("isbn_coverage")
	}
	count, e := exactUint(missing[0]["missing"])
	if e != nil || count != 0 {
		return fail("isbn_coverage")
	}
	return nil
}
func spanWhere(after, through string) string {
	where := "1"
	if after != "" {
		where += " AND catalog_key > " + util.SQLString(after)
	}
	if through != "" {
		where += " AND catalog_key <= " + util.SQLString(through)
	}
	return where
}
func batchWhere(state State) string {
	return fmt.Sprintf("batch_uuid = toUUID(%s) AND generation = %d", util.SQLString(state.BatchUUID), state.Generation)
}
func statsSQL(table, where string) string {
	return `SELECT toString(count()) AS rows, toString(uniqExact(catalog_key)) AS unique,
 toString(countIf(notEmpty(canonical_isbn))) AS isbn, toString(countIf(notEmpty(record_id))) AS bibliography,
 toString(sumWithOverflow(row_fingerprint)) AS sum, toString(groupBitXor(row_fingerprint)) AS xor
 FROM ` + table + " WHERE " + where + " SETTINGS " + readSettings
}
func (s *ClickHouse) stats(ctx context.Context, table, where string) (Stats, error) {
	rows, e := s.query(ctx, statsSQL(table, where))
	if e != nil || len(rows) != 1 {
		return Stats{}, fail("stats_read")
	}
	var st Stats
	for key, target := range map[string]*uint64{"rows": &st.Rows, "unique": &st.Unique, "isbn": &st.ISBN, "bibliography": &st.Bibliography, "sum": &st.Sum, "xor": &st.Xor} {
		n, e := exactUint(rows[0][key])
		if e != nil {
			return st, e
		}
		*target = n
	}
	return st, nil
}
func (s *ClickHouse) SourceStats(ctx context.Context, after, through string) (Stats, error) {
	return s.stats(ctx, InputTable, spanWhere(after, through))
}
func (s *ClickHouse) TargetStats(ctx context.Context, state State, after, through string) (Stats, error) {
	return s.stats(ctx, SnapshotTable, batchWhere(state)+" AND "+spanWhere(after, through))
}
func (s *ClickHouse) NextKey(ctx context.Context, after string, limit int) (string, error) {
	if limit < 1 || limit > MaxChunkSize {
		return "", fail("chunk_bound")
	}
	rows, e := s.query(ctx, fmt.Sprintf("SELECT max(catalog_key) AS upper FROM (SELECT catalog_key FROM %s WHERE %s ORDER BY catalog_key LIMIT %d) SETTINGS %s", InputTable, spanWhere(after, ""), limit, readSettings))
	if e != nil || len(rows) != 1 {
		return "", fail("boundary_read")
	}
	return util.ToString(rows[0]["upper"]), nil
}
func (s *ClickHouse) generationFree(ctx context.Context, state State) error {
	rows, e := s.query(ctx, fmt.Sprintf("SELECT toString(count()) AS conflicts FROM %s WHERE generation = %d AND batch_uuid != toUUID(%s) SETTINGS max_threads = 1, max_execution_time = 30", SnapshotTable, state.Generation, util.SQLString(state.BatchUUID)))
	if e != nil || len(rows) != 1 {
		return fail("generation_identity")
	}
	n, e := exactUint(rows[0]["conflicts"])
	if e != nil || n != 0 {
		return fail("generation_identity")
	}
	return nil
}
func (s *ClickHouse) InsertChunk(ctx context.Context, state State, span Span) error {
	if state.Endpoint != s.Endpoint || s.Endpoint == "" {
		return fail("endpoint_identity")
	}
	if !span.Expected.valid() || span.Expected.Rows > MaxChunkSize || span.Through <= span.After {
		return fail("chunk_bound")
	}
	if e := s.generationFree(ctx, state); e != nil {
		return e
	}
	dedup := token("book_public_catalog_v1", state.Endpoint, state.BatchUUID, strconv.FormatUint(state.Generation, 10), span.After, span.Through, fmt.Sprint(span.Expected))
	sql := fmt.Sprintf("INSERT INTO %s (batch_uuid, generation, %s) SELECT toUUID(%s), toUInt64(%d), %s FROM %s WHERE %s AND %s = 0 ORDER BY catalog_key LIMIT %d SETTINGS %s, insert_deduplicate = 1, insert_deduplication_token = %s, async_insert = 0", SnapshotTable, columns, util.SQLString(state.BatchUUID), state.Generation, columns, InputTable, spanWhere(span.After, span.Through), s.hostGuard(), span.Expected.Rows, readSettings, util.SQLString(dedup))
	if s.Client.ExecContext(ctx, sql) != nil {
		return fail("chunk_insert")
	}
	return nil
}
func (s *ClickHouse) Latest(ctx context.Context) (uint64, string, error) {
	rows, e := s.query(ctx, "SELECT toString(generation) AS generation, toString(batch_uuid) AS batch_uuid FROM "+MarkerTable+" ORDER BY generation DESC, published_at DESC, batch_uuid DESC LIMIT 1 SETTINGS max_threads = 1, max_execution_time = 30")
	if e != nil {
		return 0, "", e
	}
	if len(rows) == 0 {
		return 0, "", nil
	}
	n, e := exactUint(rows[0]["generation"])
	return n, util.ToString(rows[0]["batch_uuid"]), e
}
func (s *ClickHouse) MarkerExists(ctx context.Context, state State) (bool, error) {
	rows, e := s.query(ctx, fmt.Sprintf("SELECT toString(row_count) AS rows, toString(isbn_row_count) AS isbn, toString(bibliography_row_count) AS bibliography, toString(fingerprint_sum) AS sum, toString(fingerprint_xor) AS xor, source_manifest_sha256 AS manifest, toString(expected_entry_count) AS expected, toString(completed_entry_count) AS completed FROM %s WHERE %s LIMIT 2 SETTINGS max_threads = 1, max_execution_time = 30", MarkerTable, batchWhere(state)))
	if e != nil {
		return false, e
	}
	if len(rows) == 0 {
		return false, nil
	}
	if len(rows) != 1 {
		return false, fail("marker_duplicate")
	}
	row := rows[0]
	for key, want := range map[string]uint64{"rows": state.Before.Rows, "isbn": state.Before.ISBN, "bibliography": state.Before.Bibliography, "sum": state.Before.Sum, "xor": state.Before.Xor, "expected": ExpectedEntries, "completed": ExpectedEntries} {
		got, e := exactUint(row[key])
		if e != nil || got != want {
			return false, fail("marker_parity")
		}
	}
	if util.ToString(row["manifest"]) != state.ManifestSHA256 {
		return false, fail("marker_identity")
	}
	return true, nil
}
func (s *ClickHouse) Publish(ctx context.Context, state State) error {
	if state.Endpoint != s.Endpoint || s.Endpoint == "" || !state.Before.valid() {
		return fail("endpoint_identity")
	}
	if e := s.generationFree(ctx, state); e != nil {
		return e
	}
	// Validate the candidate on the very endpoint executing this INSERT. A
	// preceding HTTP readback may have reached a different backend. The marker
	// cannot be emitted for a partial, duplicated, changed, or absent candidate.
	// SQL-owned fsync settings provide local disk durability; quorum does not
	// apply to these endpoint-local MergeTree tables.
	sql := fmt.Sprintf(`INSERT INTO %s
 (batch_uuid, generation, row_count, isbn_row_count, bibliography_row_count,
 fingerprint_sum, fingerprint_xor, source_manifest_sha256, expected_entry_count,
 completed_entry_count, published_at)
 SELECT toUUID(%s), toUInt64(%d), toUInt64(%d), toUInt64(%d), toUInt64(%d),
 toUInt64(%d), toUInt64(%d), %s, toUInt32(%d), toUInt32(%d),
 parseDateTime64BestEffort(%s, 3, 'Asia/Seoul')
 FROM
 (SELECT count() AS actual_rows, uniqExact(catalog_key) AS actual_unique,
 countIf(notEmpty(canonical_isbn)) AS actual_isbn,
 countIf(notEmpty(record_id)) AS actual_bibliography,
 sumWithOverflow(row_fingerprint) AS actual_sum,
 groupBitXor(row_fingerprint) AS actual_xor
 FROM %s WHERE %s)
 WHERE %s = 0
 AND throwIf(actual_rows != %d OR actual_unique != %d OR actual_isbn != %d
 OR actual_bibliography != %d OR actual_sum != %d OR actual_xor != %d,
 'book_catalog_target_parity') = 0
 SETTINGS %s, async_insert = 0, insert_deduplicate = 1, insert_deduplication_token = %s`,
		MarkerTable, util.SQLString(state.BatchUUID), state.Generation, state.Before.Rows,
		state.Before.ISBN, state.Before.Bibliography, state.Before.Sum, state.Before.Xor,
		util.SQLString(state.ManifestSHA256), ExpectedEntries, ExpectedEntries, util.SQLString(state.PublishedAt),
		SnapshotTable, batchWhere(state), s.hostGuard(), state.Before.Rows, state.Before.Unique,
		state.Before.ISBN, state.Before.Bibliography, state.Before.Sum, state.Before.Xor,
		readSettings, util.SQLString(token("book_public_catalog_marker_v1", state.BatchUUID, state.ManifestSHA256)))
	if s.Client.ExecContext(ctx, sql) != nil {
		return fail("marker_insert")
	}
	return nil
}
