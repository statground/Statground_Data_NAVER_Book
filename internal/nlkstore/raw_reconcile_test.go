package nlkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/nlkimport"
)

func rawTestLineage() nlkimport.RawLineage {
	return nlkimport.RawLineage{SnapshotDate: time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC), DatasetName: "book", Archive: "book.zip", Entry: "book/book_0.rdf"}
}

func rawTestSource(index uint64) map[string]any {
	l := rawTestLineage()
	return map[string]any{
		"dataset_snapshot_date": l.SnapshotDate.Format("2006-01-02"), "dataset_name": l.DatasetName,
		"source_archive": l.Archive, "source_entry": l.Entry, "source_record_index": index,
		"resource_id": fmt.Sprintf("urn:book:%d", index), "content_hash": strings.Repeat("a", 64),
	}
}

func rawTestCopy(index uint64, host string) map[string]any {
	s := rawTestSource(index)
	return map[string]any{"record_index_text": strconv.FormatUint(index, 10), "hostname": host, "resource_id": s["resource_id"], "content_hash": s["content_hash"]}
}

func TestRawReplicaVerificationRequiresBothMatchingCopies(t *testing.T) {
	expected, _, err := rawExpectedRows(rawTestLineage(), []map[string]any{rawTestSource(0), rawTestSource(1)})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		rows []map[string]any
		want int
		fail bool
	}{
		{"absent", nil, 0, false},
		{"first shard", []map[string]any{rawTestCopy(0, "clickhouse-s1-r1"), rawTestCopy(0, "clickhouse-s1-r2")}, 1, false},
		{"both shards", []map[string]any{rawTestCopy(0, "clickhouse-s1-r1"), rawTestCopy(0, "clickhouse-s1-r2"), rawTestCopy(1, "clickhouse-s2-r1"), rawTestCopy(1, "clickhouse-s2-r2")}, 2, false},
		{"one copy", []map[string]any{rawTestCopy(0, "clickhouse-s1-r1")}, 0, true},
		{"duplicate same copy", []map[string]any{rawTestCopy(0, "clickhouse-s1-r1"), rawTestCopy(0, "clickhouse-s1-r1")}, 0, true},
		{"different shards", []map[string]any{rawTestCopy(0, "clickhouse-s1-r1"), rawTestCopy(0, "clickhouse-s2-r2")}, 0, true},
		{"extra shard", []map[string]any{rawTestCopy(0, "clickhouse-s1-r1"), rawTestCopy(0, "clickhouse-s1-r2"), rawTestCopy(0, "clickhouse-s2-r1")}, 0, true},
		{"unknown host", []map[string]any{rawTestCopy(0, "other")}, 0, true},
		{"unexpected index", []map[string]any{rawTestCopy(2, "clickhouse-s1-r1")}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := verifyRawReplicaRows(expected, tc.rows)
			if (err != nil) != tc.fail || len(got) != tc.want {
				t.Fatalf("accepted=%d error=%v", len(got), err)
			}
		})
	}
	for _, key := range []string{"record_index_text", "resource_id", "content_hash", "hostname"} {
		t.Run("conflicting "+key, func(t *testing.T) {
			rows := []map[string]any{rawTestCopy(0, "clickhouse-s1-r1"), rawTestCopy(0, "clickhouse-s1-r2")}
			rows[1][key] = "changed"
			if _, err := verifyRawReplicaRows(expected, rows); err == nil {
				t.Fatal("conflict accepted")
			}
		})
	}
}

func TestRawExpectedRowsRejectsWrongSourceEvidence(t *testing.T) {
	for _, key := range []string{"dataset_snapshot_date", "dataset_name", "source_archive", "source_entry", "source_record_index", "resource_id", "content_hash"} {
		t.Run(key, func(t *testing.T) {
			row := rawTestSource(0)
			row[key] = ""
			if _, _, err := rawExpectedRows(rawTestLineage(), []map[string]any{row}); err == nil {
				t.Fatal("invalid source accepted")
			}
		})
	}
	if _, _, err := rawExpectedRows(rawTestLineage(), []map[string]any{rawTestSource(0), rawTestSource(0)}); err == nil {
		t.Fatal("duplicate source index accepted")
	}
}

func TestVerifiedRawIndexesChunksReadOnlyBoundedQueries(t *testing.T) {
	var sizes []int
	client := &ch.Client{Host: "http://clickhouse.test", HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(request.Body)
		query := string(body)
		for _, part := range []string{"SELECT DISTINCT hostName() AS hostname", "clusterAllReplicas('statground_cluster', Data_Book_NLK_Raw.nlk_resource_raw_local)", "dataset_snapshot_date = toDate('2026-05-29')", "dataset_name = 'book'", "source_archive = 'book.zip'", "source_entry = 'book/book_0.rdf'", "skip_unavailable_shards = 0", "max_execution_time = 30", "timeout_overflow_mode = 'throw'", "max_memory_usage = 268435456", "result_overflow_mode = 'throw'"} {
			if !strings.Contains(query, part) {
				t.Fatalf("missing bounded contract: %s", part)
			}
		}
		if strings.Contains(query, "INSERT") || strings.Contains(query, "system.") {
			t.Fatal("unexpected mutation or broader metadata read")
		}
		part := strings.Split(strings.Split(query, "source_record_index IN (")[1], ")")[0]
		values := strings.Split(part, ", ")
		sizes = append(sizes, len(values))
		index, _ := strconv.ParseUint(values[0], 10, 64)
		var response strings.Builder
		for _, host := range []string{"clickhouse-s1-r1", "clickhouse-s1-r2"} {
			b, _ := json.Marshal(rawTestCopy(index, host))
			response.Write(b)
			response.WriteByte('\n')
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response.String())), Request: request}, nil
	})}}
	store, _ := NewClickHouse(client, ConfigFromEnv())
	source := make([]map[string]any, 10001)
	for i := range source {
		source[i] = rawTestSource(uint64(len(source) - 1 - i))
	}
	got, err := store.VerifiedRawRecordIndexes(context.Background(), rawTestLineage(), source)
	if err != nil || len(got) != 3 || fmt.Sprint(sizes) != "[5000 5000 1]" {
		t.Fatalf("got=%d sizes=%v error=%v", len(got), sizes, err)
	}
}

func TestVerifiedRawIndexesFailClosedOnUnavailableReplica(t *testing.T) {
	client := &ch.Client{Host: "http://clickhouse.test", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("injected unavailable replica") })}}
	store, _ := NewClickHouse(client, ConfigFromEnv())
	if got, err := store.VerifiedRawRecordIndexes(context.Background(), rawTestLineage(), []map[string]any{rawTestSource(0)}); err == nil || got != nil {
		t.Fatal("unavailable read accepted")
	}
}

func TestRawReplicaQueryNative(t *testing.T) {
	binary := os.Getenv("NLK_TEST_CLICKHOUSE_LOCAL")
	if binary == "" {
		t.Skip("isolated ClickHouse fixture not requested")
	}
	query := rawReplicaReadbackSQL(rawTestLineage(), []uint64{0, 1})
	if dir := os.Getenv("NLK_TEST_RAW_PROOF_DIR"); dir != "" {
		if err := os.WriteFile(filepath.Join(dir, "raw-reconcile.sql"), []byte(query), 0600); err != nil {
			t.Fatal(err)
		}
	}
	query = strings.ReplaceAll(query, "hostName()", "hostname")
	query = strings.ReplaceAll(query, "clusterAllReplicas('statground_cluster', Data_Book_NLK_Raw.nlk_resource_raw_local)", "fixture")
	schema := `CREATE TABLE fixture(hostname String,dataset_snapshot_date Date,dataset_name String,source_archive String,source_entry String,source_record_index UInt64,resource_id String,content_hash String) ENGINE=Memory;`
	var values []string
	for _, host := range []string{"clickhouse-s1-r1", "clickhouse-s1-r2"} {
		values = append(values, fmt.Sprintf("('%s','2026-05-29','book','book.zip','book/book_0.rdf',0,'urn:book:0','%s')", host, strings.Repeat("a", 64)))
	}
	// Same index in a different source entry must not be mistaken for an ACK.
	values = append(values, "('clickhouse-s1-r1','2026-05-29','book','book.zip','other.rdf',1,'wrong','wrong')")
	sql := schema + "INSERT INTO fixture VALUES " + strings.Join(values, ",") + ";" + query + " FORMAT JSONEachRow;"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "local", "--path", t.TempDir(), "--max_threads", "2", "--background_schedule_pool_size", "2", "--multiquery", "--query", sql)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("native query: %v %s", err, output)
	}
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	expected, _, _ := rawExpectedRows(rawTestLineage(), []map[string]any{rawTestSource(0), rawTestSource(1)})
	verified, err := verifyRawReplicaRows(expected, rows)
	if err != nil || len(verified) != 1 {
		t.Fatalf("native accepted=%d error=%v", len(verified), err)
	}
}
