package nlkstore

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"statground_naver_book_go/internal/nlkimport"
	"statground_naver_book_go/internal/util"
)

// This is the SQL-owned statground_cluster raw topology, not the four-copy
// source byte archive. Topology changes must update this contract explicitly.
var rawReplicaPeers = map[string]string{
	"clickhouse-s1-r1": "clickhouse-s1-r2",
	"clickhouse-s1-r2": "clickhouse-s1-r1",
	"clickhouse-s2-r1": "clickhouse-s2-r2",
	"clickhouse-s2-r2": "clickhouse-s2-r1",
}

type rawExpectedIdentity struct{ resourceID, contentHash string }

// VerifiedRawRecordIndexes acknowledges only source-identical rows present on
// both replicas of one shard. The caller rebuilds expected hashes from the
// validated source file; timestamps and run IDs are not part of that hash.
// An absent row can be inserted. Partial replication, conflicting versions, or
// unavailable replicas are ambiguous and must not advance a checkpoint.
func (s *ClickHouseStore) VerifiedRawRecordIndexes(ctx context.Context, lineage nlkimport.RawLineage, sourceRows []map[string]any) (map[uint64]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Config.RawLocalTable != "Data_Book_NLK_Raw.nlk_resource_raw_local" {
		return nil, &StoreError{Category: "raw_reconcile_configuration"}
	}
	expected, indexes, err := rawExpectedRows(lineage, sourceRows)
	if err != nil {
		return nil, err
	}
	verified := make(map[uint64]struct{}, len(expected))
	for start := 0; start < len(indexes); start += existingRawIndexLookupChunkSize {
		end := min(start+existingRawIndexLookupChunkSize, len(indexes))
		chunk := indexes[start:end]
		rows, err := s.Client.QueryJSONEachRowContext(ctx, rawReplicaReadbackSQL(lineage, chunk))
		if err != nil {
			return nil, &StoreError{Category: "raw_reconcile_read_failed"}
		}
		chunkExpected := make(map[uint64]rawExpectedIdentity, len(chunk))
		for _, index := range chunk {
			chunkExpected[index] = expected[index]
		}
		accepted, err := verifyRawReplicaRows(chunkExpected, rows)
		if err != nil {
			return nil, err
		}
		for index := range accepted {
			verified[index] = struct{}{}
		}
	}
	return verified, nil
}

func rawExpectedRows(lineage nlkimport.RawLineage, rows []map[string]any) (map[uint64]rawExpectedIdentity, []uint64, error) {
	fail := func() (map[uint64]rawExpectedIdentity, []uint64, error) {
		return nil, nil, &StoreError{Category: "raw_reconcile_source_invalid"}
	}
	if lineage.SnapshotDate.IsZero() || lineage.DatasetName == "" || lineage.Archive == "" || lineage.Entry == "" {
		return fail()
	}
	expected := make(map[uint64]rawExpectedIdentity, len(rows))
	indexes := make([]uint64, 0, len(rows))
	for _, row := range rows {
		for key, value := range map[string]string{
			"dataset_snapshot_date": lineage.SnapshotDate.Format("2006-01-02"),
			"dataset_name":          lineage.DatasetName, "source_archive": lineage.Archive, "source_entry": lineage.Entry,
		} {
			if row[key] != value {
				return fail()
			}
		}
		index := rawRecordIndex(row)
		resourceID, resourceOK := row["resource_id"].(string)
		hash, hashOK := row["content_hash"].(string)
		decoded, decodeErr := hex.DecodeString(hash)
		if len(index) != 1 || !resourceOK || resourceID == "" || !hashOK || len(decoded) != 32 || decodeErr != nil || hash != strings.ToLower(hash) {
			return fail()
		}
		if _, duplicate := expected[index[0]]; duplicate {
			return fail()
		}
		expected[index[0]] = rawExpectedIdentity{resourceID, hash}
		indexes = append(indexes, index[0])
	}
	return expected, uniqueSortedIndexes(indexes), nil
}

func rawReplicaReadbackSQL(lineage nlkimport.RawLineage, indexes []uint64) string {
	values := make([]string, len(indexes))
	for i, index := range indexes {
		values[i] = strconv.FormatUint(index, 10)
	}
	return fmt.Sprintf(`SELECT DISTINCT hostName() AS hostname,
    toString(source_record_index) AS record_index_text, resource_id, content_hash
FROM clusterAllReplicas('statground_cluster', Data_Book_NLK_Raw.nlk_resource_raw_local)
WHERE dataset_snapshot_date = toDate(%s)
  AND dataset_name = %s AND source_archive = %s AND source_entry = %s
  AND source_record_index IN (%s)
SETTINGS skip_unavailable_shards = 0, max_threads = 2, max_execution_time = 30,
    timeout_overflow_mode = 'throw', max_memory_usage = 268435456,
    max_result_rows = %d, max_result_bytes = 16777216, result_overflow_mode = 'throw'`,
		util.SQLString(lineage.SnapshotDate.Format("2006-01-02")), util.SQLString(lineage.DatasetName),
		util.SQLString(lineage.Archive), util.SQLString(lineage.Entry), strings.Join(values, ", "), 4*len(indexes)+1)
}

func verifyRawReplicaRows(expected map[uint64]rawExpectedIdentity, rows []map[string]any) (map[uint64]struct{}, error) {
	fail := func() (map[uint64]struct{}, error) {
		return nil, &StoreError{Category: "raw_reconcile_mismatch"}
	}
	copies := make(map[uint64]map[string]struct{}, len(expected))
	for _, row := range rows {
		indexString, ok := row["record_index_text"].(string)
		index, err := strconv.ParseUint(indexString, 10, 64)
		if !ok || err != nil {
			return fail()
		}
		identity, wanted := expected[index]
		host, hostOK := row["hostname"].(string)
		if !wanted || !hostOK || rawReplicaPeers[host] == "" || row["resource_id"] != identity.resourceID || row["content_hash"] != identity.contentHash {
			return fail()
		}
		if copies[index] == nil {
			copies[index] = make(map[string]struct{}, 2)
		}
		copies[index][host] = struct{}{}
	}
	accepted := make(map[uint64]struct{}, len(copies))
	for index, hosts := range copies {
		if len(hosts) != 2 {
			return fail()
		}
		for host := range hosts {
			if _, found := hosts[rawReplicaPeers[host]]; !found {
				return fail()
			}
		}
		accepted[index] = struct{}{}
	}
	return accepted, nil
}
