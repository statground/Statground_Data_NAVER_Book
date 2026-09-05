package bookcatalogpublish

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/nlkbackfill"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func testClickHouse(t *testing.T, handler func(string) []map[string]any) *ClickHouse {
	t.Helper()
	client := &ch.Client{Host: "database.test", Port: 443, Protocol: "https", HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		rows := handler(string(b))
		var out strings.Builder
		for _, row := range rows {
			if strings.HasPrefix(string(b), "CHECK GRANT") {
				if strings.Contains(string(b), "FORMAT") || r.URL.Query().Get("default_format") != "TabSeparated" {
					t.Fatal("CHECK GRANT used an unsupported SQL FORMAT clause")
				}
				fmt.Fprintln(&out, row["result"])
				continue
			}
			b, e := json.Marshal(row)
			if e != nil {
				t.Fatal(e)
			}
			out.Write(b)
			out.WriteByte('\n')
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(out.String())), Header: make(http.Header), Request: r}, nil
	})}}
	return &ClickHouse{Client: client, Endpoint: "Clickhouse_1"}
}
func TestCoverageVerifiesEveryManifestRevisionAndProjection(t *testing.T) {
	for _, fault := range []string{"", "missing_file", "revision", "missing_projection", "projection_index", "isbn"} {
		t.Run(fault, func(t *testing.T) {
			cfg := configFixture(t)
			lineages, err := cfg.Manifest.Lineages(cfg.SnapshotDate)
			if err != nil {
				t.Fatal(err)
			}
			var raw, projected []map[string]any
			for _, l := range lineages {
				raw = append(raw, map[string]any{"dataset_name": l.DatasetName, "source_archive": l.SourceArchive, "source_entry": l.SourceEntry, "source_revision": l.SourceRevision, "status": "succeeded", "content_hash": strings.Repeat("b", 64), "entry_crc32": "1234abcd", "bytes": fmt.Sprint(l.UncompressedBytes), "next": "100", "parsed": "100", "inserted": "100", "rejected": "0"})
				for _, p := range nlkbackfill.DefaultProjections() {
					applies, _ := nlkbackfill.ProjectionAppliesToDataset(p, l.DatasetName)
					if applies {
						projected = append(projected, map[string]any{"dataset_name": l.DatasetName, "source_archive": l.SourceArchive, "source_entry": l.SourceEntry, "projection": string(p), "status": "succeeded", "next": "100"})
					}
				}
			}
			switch fault {
			case "missing_file":
				raw = raw[:len(raw)-1]
			case "revision":
				raw[0]["source_revision"] = "changed"
			case "missing_projection":
				projected = projected[1:]
			case "projection_index":
				projected[0]["next"] = "99"
			}
			driver := testClickHouse(t, func(sql string) []map[string]any {
				switch {
				case strings.Contains(sql, "FROM "+rawCheckpoints):
					return raw
				case strings.Contains(sql, "FROM "+projectionCheckpoints):
					return projected
				case strings.Contains(sql, "LEFT ANTI JOIN"):
					if !strings.Contains(sql, "offline_material") || !strings.Contains(sql, "online_material") {
						t.Fatal("bibliography material types were replaced by dataset labels")
					}
					n := "0"
					if fault == "isbn" {
						n = "1"
					}
					return []map[string]any{{"missing": n}}
				default:
					t.Fatalf("unexpected query: %s", sql)
					return nil
				}
			})
			err = driver.Coverage(context.Background(), cfg)
			if (fault == "") != (err == nil) {
				t.Fatalf("fault=%s error=%v", fault, err)
			}
		})
	}
}
func TestDriverChecksExactPhysicalEndpointAndLocalTables(t *testing.T) {
	var calls int
	driver := testClickHouse(t, func(sql string) []map[string]any {
		calls++
		switch {
		case strings.HasPrefix(sql, "SELECT hostName() AS value"):
			return []map[string]any{{"value": "Clickhouse_1"}}
		case strings.HasPrefix(sql, "EXISTS TABLE"):
			return []map[string]any{{"result": 1}}
		case strings.HasPrefix(sql, "CHECK GRANT"):
			return []map[string]any{{"result": 1}}
		case strings.Contains(sql, "FROM system.tables"):
			return []map[string]any{{"name": "book_public_catalog_snapshot", "engine": "MergeTree"}, {"name": "book_public_catalog_published_batch", "engine": "MergeTree"}}
		default:
			t.Fatalf("unexpected query %s", sql)
			return nil
		}
	})
	if err := driver.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := calls
	driver.Client.Protocol = "http"
	if err := driver.Validate(context.Background()); SafeCategory(err) != "https_required" || calls != before {
		t.Fatal("plaintext reached database")
	}
	driver.Client.Protocol = "https"
	driver.Endpoint = "Clickhouse_2"
	if err := driver.Validate(context.Background()); SafeCategory(err) != "endpoint_identity" {
		t.Fatal("wrong physical endpoint accepted")
	}
}
func TestInsertSQLBindsStableIdentityAndCopiesFullSource(t *testing.T) {
	var inserts []string
	driver := testClickHouse(t, func(sql string) []map[string]any {
		if strings.Contains(sql, "AS conflicts") {
			return []map[string]any{{"conflicts": "0"}}
		}
		if strings.HasPrefix(sql, "INSERT INTO") {
			inserts = append(inserts, sql)
			return nil
		}
		t.Fatalf("unexpected query %s", sql)
		return nil
	})
	state := State{Endpoint: "Clickhouse_1", BatchUUID: "00000000-0000-7000-8000-000000000001", Generation: 100, ManifestSHA256: strings.Repeat("a", 64), Before: Stats{Rows: 1, Unique: 1, ISBN: 1, Sum: 9, Xor: 9}, PublishedAt: "2026-09-06 12:00:00.000"}
	span := Span{Through: "isbn:9781234567890", Expected: state.Before}
	for i := 0; i < 2; i++ {
		if err := driver.InsertChunk(context.Background(), state, span); err != nil {
			t.Fatal(err)
		}
	}
	if inserts[0] != inserts[1] {
		t.Fatal("stable chunk lineage changed SQL/dedup token")
	}
	for _, want := range []string{SnapshotTable, InputTable, "batch_uuid, generation, " + columns, "catalog_key <= 'isbn:9781234567890'", "ORDER BY catalog_key LIMIT 1", "async_insert = 0", "insert_deduplication_token", "max_memory_usage = 2147483648"} {
		if !strings.Contains(inserts[0], want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(inserts[0], "INTERVAL") || strings.Contains(inserts[0], "fsync_after_insert") || strings.Contains(inserts[0], "INSERT INTO "+MarkerTable) {
		t.Fatal("chunk truncates corpus or uses table-only setting as query setting")
	}
	if err := driver.Publish(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	marker := inserts[len(inserts)-1]
	for _, want := range []string{MarkerTable, "expected_entry_count", "completed_entry_count", "toUInt32(208), toUInt32(208)", "'" + state.ManifestSHA256 + "'", "actual_sum != 9", "actual_xor != 9", "FROM " + SnapshotTable, "actual_rows != 1", "actual_unique != 1", "actual_isbn != 1", "actual_bibliography != 0", "book_catalog_endpoint_mismatch", "book_catalog_target_parity"} {
		if !strings.Contains(marker, want) {
			t.Errorf("marker missing %s", want)
		}
	}
}

func TestPrivateHTTPOverrideRetainsGrantAndEndpointValidation(t *testing.T) {
	for _, test := range []struct {
		name, host, endpoint, denied, want string
	}{
		{"private", "10.0.0.8", "Clickhouse_1", "", ""},
		{"public_denied", "8.8.8.8", "Clickhouse_1", "", "https_required"},
		{"wrong_node", "10.0.0.8", "Clickhouse_2", "", "endpoint_identity"},
		{"select_denied_200", "10.0.0.8", "Clickhouse_1", "SELECT", "select_grant"},
		{"insert_denied_200", "10.0.0.8", "Clickhouse_1", "INSERT", "insert_grant"},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			driver := testClickHouse(t, func(sql string) []map[string]any {
				calls++
				switch {
				case strings.HasPrefix(sql, "SELECT hostName() AS value"):
					return []map[string]any{{"value": "Clickhouse_1"}}
				case strings.HasPrefix(sql, "EXISTS TABLE"):
					return []map[string]any{{"result": 1}}
				case strings.HasPrefix(sql, "CHECK GRANT"):
					result := 1
					if test.denied != "" && strings.HasPrefix(sql, "CHECK GRANT "+test.denied) {
						result = 0
					}
					return []map[string]any{{"result": result}}
				case strings.Contains(sql, "FROM system.tables"):
					return []map[string]any{{"engine": "MergeTree"}, {"engine": "MergeTree"}}
				default:
					t.Fatal("unexpected database operation")
					return nil
				}
			})
			driver.Client.Protocol, driver.Client.Host = "http", test.host
			driver.Endpoint, driver.AllowPrivateHTTP = test.endpoint, true
			err := driver.Validate(context.Background())
			if (err == nil) != (test.want == "") || err != nil && SafeCategory(err) != test.want {
				t.Fatalf("want=%s err=%v", test.want, err)
			}
			if test.want == "https_required" && calls != 0 {
				t.Fatal("public HTTP reached transport")
			}
		})
	}
}

func TestPublisherRejectsEndpointSwitchAfterSuccessfulPreflight(t *testing.T) {
	for _, operation := range []string{"source_read", "target_read", "latest_read", "marker_read", "snapshot_insert", "marker_insert"} {
		t.Run(operation, func(t *testing.T) {
			actualHost := "Clickhouse_1"
			acceptedWrites := 0
			switchOnWrite := false
			client := &ch.Client{Host: "database.test", Port: 443, Protocol: "https"}
			client.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(r.Body)
				statement := string(body)
				isWrite := strings.HasPrefix(statement, "INSERT INTO")
				if isWrite && switchOnWrite {
					actualHost = "Clickhouse_2"
				}
				// Model two HTTP backends: only a guard inside the very request
				// being executed can reject a switch after the earlier host probe.
				if actualHost != "Clickhouse_1" && strings.Contains(statement, "(SELECT throwIf(hostName() != 'Clickhouse_1', 'book_catalog_endpoint_mismatch'))") {
					return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("Code: 395. book_catalog_endpoint_mismatch")), Header: make(http.Header), Request: r}, nil
				}
				var rows []map[string]any
				switch {
				case strings.HasPrefix(statement, "SELECT hostName() AS value"):
					rows = []map[string]any{{"value": actualHost}}
				case strings.HasPrefix(statement, "EXISTS TABLE"), strings.HasPrefix(statement, "CHECK GRANT"):
					rows = []map[string]any{{"result": 1}}
				case strings.Contains(statement, "FROM system.tables"):
					rows = []map[string]any{{"name": "book_public_catalog_snapshot", "engine": "MergeTree"}, {"name": "book_public_catalog_published_batch", "engine": "MergeTree"}}
				case strings.Contains(statement, "AS conflicts"):
					rows = []map[string]any{{"conflicts": "0"}}
				case isWrite:
					acceptedWrites++
				case strings.Contains(statement, "AS unique"):
					rows = []map[string]any{{"rows": "1", "unique": "1", "isbn": "1", "bibliography": "0", "sum": "9", "xor": "9"}}
				}
				var response strings.Builder
				for _, row := range rows {
					if strings.HasPrefix(statement, "CHECK GRANT") {
						fmt.Fprintln(&response, row["result"])
						continue
					}
					encoded, _ := json.Marshal(row)
					response.Write(encoded)
					response.WriteByte('\n')
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response.String())), Header: make(http.Header), Request: r}, nil
			})}
			store := &ClickHouse{Client: client, Endpoint: "Clickhouse_1"}
			if err := store.Validate(context.Background()); err != nil {
				t.Fatal(err)
			}
			state := State{Endpoint: "Clickhouse_1", BatchUUID: "00000000-0000-7000-8000-000000000001", Generation: 100, ManifestSHA256: strings.Repeat("a", 64), Before: Stats{Rows: 1, Unique: 1, ISBN: 1, Sum: 9, Xor: 9}, PublishedAt: "2026-09-06 12:00:00.000"}
			var err error
			if strings.HasSuffix(operation, "insert") {
				switchOnWrite = true
			} else {
				actualHost = "Clickhouse_2"
			}
			switch operation {
			case "source_read":
				_, err = store.SourceStats(context.Background(), "", "")
			case "target_read":
				_, err = store.TargetStats(context.Background(), state, "", "")
			case "latest_read":
				_, _, err = store.Latest(context.Background())
			case "marker_read":
				_, err = store.MarkerExists(context.Background(), state)
			case "snapshot_insert":
				err = store.InsertChunk(context.Background(), state, Span{Through: "isbn:9781234567890", Expected: state.Before})
			case "marker_insert":
				err = store.Publish(context.Background(), state)
			}
			if err == nil || acceptedWrites != 0 {
				t.Fatalf("endpoint switch accepted: err=%v writes=%d", err, acceptedWrites)
			}
		})
	}
}

func TestMarkerRechecksEveryTargetFingerprintInExecutingStatement(t *testing.T) {
	state := State{Endpoint: "Clickhouse_1", BatchUUID: "00000000-0000-7000-8000-000000000001", Generation: 100, ManifestSHA256: strings.Repeat("a", 64), Before: Stats{Rows: 5, Unique: 5, ISBN: 3, Bibliography: 2, Sum: 18446744073709551614, Xor: 18446744073709551613}, PublishedAt: "2026-09-06 12:00:00.000"}
	store := testClickHouse(t, func(statement string) []map[string]any {
		if strings.Contains(statement, "AS conflicts") {
			return []map[string]any{{"conflicts": "0"}}
		}
		if !strings.HasPrefix(statement, "INSERT INTO "+MarkerTable) {
			t.Fatalf("unexpected request %s", statement)
		}
		for _, required := range []string{
			"FROM " + SnapshotTable + " WHERE " + batchWhere(state),
			"actual_rows != 5", "actual_unique != 5", "actual_isbn != 3", "actual_bibliography != 2",
			"actual_sum != 18446744073709551614", "actual_xor != 18446744073709551613",
			"throwIf(actual_rows", "'book_catalog_target_parity') = 0",
			"(SELECT throwIf(hostName() != 'Clickhouse_1', 'book_catalog_endpoint_mismatch')) = 0",
		} {
			if !strings.Contains(statement, required) {
				t.Errorf("marker can bypass executing-node parity: missing %s", required)
			}
		}
		if strings.Contains(statement, "FORMAT JSONEachRow") {
			t.Fatal("unconditional marker insert remains")
		}
		return nil
	})
	if err := store.Publish(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	state.Endpoint = "Clickhouse_2"
	if err := store.Publish(context.Background(), state); SafeCategory(err) != "endpoint_identity" {
		t.Fatal("foreign endpoint journal accepted")
	}
}
