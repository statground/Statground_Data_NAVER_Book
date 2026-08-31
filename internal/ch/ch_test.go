package ch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCanonicalJSONEachRowIsReplayStable(t *testing.T) {
	when := time.Date(2026, 9, 1, 0, 1, 2, 345000000, time.UTC)
	rows := []map[string]any{
		{"when": when, "html": "<book>&", "version": uint64(7)},
		{"html": "plain"},
	}

	columns, canonical, payload, err := CanonicalJSONEachRow(rows)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(columns, ","); got != "html,version,when" {
		t.Fatalf("columns=%q", got)
	}
	if got := canonical[0]["when"]; got != "2026-09-01 09:01:02.345" {
		t.Fatalf("canonical time=%v", got)
	}
	if canonical[1]["version"] != nil || canonical[1]["when"] != nil {
		t.Fatalf("missing union columns were not made explicit: %#v", canonical[1])
	}
	if !bytes.Contains(payload, []byte(`"html":"<book>&"`)) || bytes.Contains(payload, []byte(`\u003c`)) {
		t.Fatalf("payload did not preserve JSONEachRow HTML encoding: %s", payload)
	}

	replayColumns, replayRows, replayPayload, err := CanonicalJSONEachRow(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(replayColumns, ",") != strings.Join(columns, ",") || !bytes.Equal(replayPayload, payload) {
		t.Fatalf("canonical payload changed on replay\nfirst=%s\nreplay=%s", payload, replayPayload)
	}
	if len(replayRows) != len(canonical) {
		t.Fatalf("replay row count=%d", len(replayRows))
	}
	if JSONEachRowDeduplicationToken("db.target", columns, payload) !=
		JSONEachRowDeduplicationToken("db.target", replayColumns, replayPayload) {
		t.Fatal("canonical replay changed the deduplication token")
	}
	rowsJSON, err := JSONEachRowPayloadAsArray(payload)
	if err != nil {
		t.Fatal(err)
	}
	wantRowsJSON := "[" + strings.ReplaceAll(strings.TrimSuffix(string(payload), "\n"), "\n", ",") + "]"
	if string(rowsJSON) != wantRowsJSON || !strings.Contains(string(rowsJSON), `"html":"<book>&"`) {
		t.Fatalf("stored row array is not byte-equivalent to JSONEachRow: %s", rowsJSON)
	}
}

func TestCanonicalDigestKeepsTestAndProductionSourceLineage(t *testing.T) {
	rows := []map[string]any{
		{"event_uuid": "01900000-0000-7000-8000-000000000001", "source": "testgo", "value": "test payload"},
		{"event_uuid": "01900000-0000-7000-8000-000000000002", "source": "production", "value": "production payload"},
	}
	columns, canonical, payload, err := CanonicalJSONEachRow(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(canonical) != 2 || canonical[0]["source"] != "testgo" || canonical[1]["source"] != "production" {
		t.Fatalf("test/production source lineage was filtered or rewritten: %#v", canonical)
	}
	withSources := JSONEachRowDeduplicationToken("db.target", columns, payload)
	rows[0]["source"] = "production"
	changedColumns, _, changedPayload, err := CanonicalJSONEachRow(rows)
	if err != nil {
		t.Fatal(err)
	}
	withoutTestSource := JSONEachRowDeduplicationToken("db.target", changedColumns, changedPayload)
	if withSources == withoutTestSource {
		t.Fatal("source lineage did not participate in the canonical deduplication digest")
	}
}

func TestTableExistsContextHonorsCancellationBeforeTransport(t *testing.T) {
	calls := 0
	client := &Client{
		Host:     "clickhouse.example.invalid",
		Port:     8123,
		Database: "Data_Book_KAKAO_Raw",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("transport should not be called")
		})},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.TableExistsContext(ctx, "Data_Book_KAKAO_Raw.kakao_book_raw")
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("error=%v calls=%d, want canceled before transport", err, calls)
	}
}

func TestValidateDirectEndpointHostnameIsExactAndFailClosed(t *testing.T) {
	tests := []struct {
		name        string
		expected    string
		response    string
		wantErr     string
		wantRequest int
	}{
		{name: "empty", expected: "", wantErr: "is required"},
		{name: "surrounding whitespace", expected: " Clickhouse_S1_R1", wantErr: "is required"},
		{name: "gateway identity", expected: "Clickhouse_Cluster_Gateway", wantErr: "physical ClickHouse node"},
		{name: "different node", expected: "Clickhouse_S1_R1", response: "Clickhouse_S2_R1", wantErr: "hostname mismatch", wantRequest: 1},
		{name: "case mismatch", expected: "Clickhouse_S1_R1", response: "clickhouse_s1_r1", wantErr: "hostname mismatch", wantRequest: 1},
		{name: "multiple rows", expected: "Clickhouse_S1_R1", response: "Clickhouse_S1_R1\nClickhouse_S1_R1", wantErr: "hostname mismatch", wantRequest: 1},
		{name: "exact", expected: "Clickhouse_S1_R1", response: "Clickhouse_S1_R1", wantRequest: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			client := &Client{
				Host: "http://clickhouse.test",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					requests++
					body, err := io.ReadAll(request.Body)
					if err != nil {
						t.Fatal(err)
					}
					if !strings.Contains(string(body), "SELECT hostName() AS value") {
						t.Fatalf("unexpected endpoint identity query: %s", body)
					}
					var response strings.Builder
					for _, value := range strings.Split(test.response, "\n") {
						response.WriteString(`{"value":`)
						encoded, _ := json.Marshal(value)
						response.Write(encoded)
						response.WriteString("}\n")
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader(response.String())),
						Request:    request,
					}, nil
				})},
			}
			err := client.ValidateDirectEndpointHostnameContext(context.Background(), test.expected)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateDirectEndpointHostnameContext() error=%v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error=%v, want %q", err, test.wantErr)
			}
			if requests != test.wantRequest {
				t.Fatalf("requests=%d, want %d", requests, test.wantRequest)
			}
		})
	}
}

func TestClientHTTPErrorKeepsOnlyStatusAndClickHouseCode(t *testing.T) {
	client := &Client{
		Host:     "clickhouse.example.invalid",
		Port:     8123,
		Database: "Data_Book_KAKAO_Raw",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("Code: 60. DB::Exception: secret internal table")),
				Request:    request,
			}, nil
		})},
	}
	err := client.Exec("SELECT 1")
	if err == nil || err.Error() != "clickhouse http status=500 code=60" {
		t.Fatalf("error=%v, want sanitized status and code", err)
	}
}

func TestClientHTTPErrorBodyIsBoundedAndNeverExposed(t *testing.T) {
	secret := strings.Repeat("secret-row-value", 10000)
	client := &Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("Code: 319. " + secret)),
				Request:    request,
			}, nil
		})},
	}
	err := client.Exec("INSERT INTO db.target SELECT 1")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("error=%T %v, want *HTTPError", err, err)
	}
	if httpErr.Code != 319 || !httpErr.Truncated || strings.Contains(err.Error(), "secret-row-value") {
		t.Fatalf("bounded sanitized error=%#v %q", httpErr, err)
	}
}

func TestSynchronousInsertDoesNotRetryUnknownInsertStatus(t *testing.T) {
	requests := 0
	client := &Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("Code: 319. DB::Exception: UNKNOWN_STATUS_OF_INSERT")),
				Request:    request,
			}, nil
		})},
	}
	err := client.InsertJSONEachRowSynchronous(
		"db.target",
		[]map[string]any{{"id": "one"}},
		strings.Repeat("a", 64),
	)
	if !IsAmbiguousInsertError(err) {
		t.Fatalf("error=%v, want ambiguous insert", err)
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want one non-retried target attempt", requests)
	}
}

func TestSynchronousInsertDoesNotFollowHTTPRedirect(t *testing.T) {
	requests := 0
	redirected := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.URL.Path == "/redirected" {
			redirected++
			writer.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(writer, request, "/redirected", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client := &Client{Host: server.URL, HTTPClient: server.Client()}
	err := client.InsertJSONEachRowSynchronous(
		"db.target",
		[]map[string]any{{"id": "one"}},
		strings.Repeat("a", 64),
	)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("error=%T %v, want redirect response without replay", err, err)
	}
	if requests != 1 || redirected != 0 {
		t.Fatalf("requests=%d redirected=%d, want exactly one target exchange", requests, redirected)
	}
}

func TestSynchronousInsertTreatsPostStartCancellationAsAmbiguous(t *testing.T) {
	requests := 0
	client := &Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			<-request.Context().Done()
			return nil, request.Context().Err()
		})},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := client.InsertJSONEachRowSynchronousContext(
		ctx,
		"db.target",
		[]map[string]any{{"id": "one"}},
		strings.Repeat("a", 64),
	)
	if !errors.Is(err, context.DeadlineExceeded) || !IsAmbiguousInsertError(err) {
		t.Fatalf("error=%v, want ambiguous context deadline", err)
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want one target attempt", requests)
	}
}

func TestClickHouseCancellationCodesAreAmbiguousForInsert(t *testing.T) {
	for _, code := range []int{319, 394, 677, 734, 735} {
		if !IsAmbiguousInsertError(&HTTPError{StatusCode: http.StatusInternalServerError, Code: code}) {
			t.Errorf("ClickHouse code=%d was not treated as an ambiguous insert", code)
		}
	}
	if IsAmbiguousInsertError(&HTTPError{StatusCode: http.StatusInternalServerError, Code: 60}) {
		t.Fatal("UNKNOWN_TABLE was treated as an ambiguous insert")
	}
}

func TestReconcileJSONEachRowConvergesAllNonePartialAndSuperseded(t *testing.T) {
	first := map[string]any{
		"event_uuid": "01900000-0000-7000-8000-000000000001",
		"source":     "test",
		"value":      "first",
		"version":    1,
	}
	second := map[string]any{
		"event_uuid": "01900000-0000-7000-8000-000000000002",
		"source":     "prod",
		"value":      "second",
		"version":    1,
	}
	tests := []struct {
		name           string
		actual         []map[string]any
		wantMissing    int
		wantAccepted   int
		wantSuperseded int
		wantErr        string
	}{
		{name: "all", actual: []map[string]any{first, first, second, second}, wantAccepted: 2},
		{name: "none", wantMissing: 2},
		{name: "partial", actual: []map[string]any{first, first}, wantMissing: 1, wantAccepted: 1},
		{name: "superseded", actual: []map[string]any{
			{"event_uuid": first["event_uuid"], "source": "prod", "value": "newer", "version": 2},
			second,
		}, wantAccepted: 2, wantSuperseded: 1},
		{name: "equal version mismatch", actual: []map[string]any{
			{"event_uuid": first["event_uuid"], "source": "prod", "value": "different", "version": 1},
			second,
		}, wantErr: "equal-version payload mismatch"},
		{name: "conflicting duplicate identity", actual: []map[string]any{
			first,
			{"event_uuid": first["event_uuid"], "source": "prod", "value": "different", "version": 1},
			second,
		}, wantErr: "conflicting duplicate payloads"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			client := &Client{
				Host:     "http://clickhouse.test",
				Database: "db",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					requests++
					body, readErr := io.ReadAll(request.Body)
					if readErr != nil {
						t.Fatal(readErr)
					}
					query := string(body)
					for _, required := range []string{
						"clusterAllReplicas('statground_cluster', 'db', 'target_local')",
						"toUUID('01900000-0000-7000-8000-000000000001')",
						"`source`",
						"max_execution_time = 15",
					} {
						if !strings.Contains(query, required) {
							t.Fatalf("reconciliation query missing %q: %s", required, query)
						}
					}
					var response strings.Builder
					for _, row := range test.actual {
						encoded, marshalErr := json.Marshal(row)
						if marshalErr != nil {
							t.Fatal(marshalErr)
						}
						response.Write(encoded)
						response.WriteByte('\n')
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader(response.String())),
						Request:    request,
					}, nil
				})},
			}
			result, err := client.ReconcileJSONEachRowContext(context.Background(), ReconcileSpec{
				Cluster:       "statground_cluster",
				LocalTable:    "db.target_local",
				KeyColumns:    []ReconcileKeyColumn{{Name: "event_uuid", Kind: ReconcileUUID}},
				VersionColumn: "version",
				MaxRows:       10,
			}, []map[string]any{first, second})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error=%v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if requests != 1 || len(result.MissingRows) != test.wantMissing || result.AcceptedRows != test.wantAccepted || result.SupersededRows != test.wantSuperseded {
				t.Fatalf("requests=%d result=%#v", requests, result)
			}
		})
	}
}

func TestReconcileJSONEachRowRejectsDuplicateExpectedIdentityBeforeQuery(t *testing.T) {
	requests := 0
	client := &Client{
		Host:     "http://clickhouse.test",
		Database: "db",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			return nil, errors.New("unexpected request")
		})},
	}
	row := map[string]any{
		"event_uuid": "01900000-0000-7000-8000-000000000001",
		"version":    1,
	}
	_, err := client.ReconcileJSONEachRowContext(context.Background(), ReconcileSpec{
		LocalTable:    "db.target_local",
		KeyColumns:    []ReconcileKeyColumn{{Name: "event_uuid", Kind: ReconcileUUID}},
		VersionColumn: "version",
	}, []map[string]any{row, row})
	if err == nil || !strings.Contains(err.Error(), "duplicate logical identity") || requests != 0 {
		t.Fatalf("error=%v requests=%d", err, requests)
	}
}

func TestReconcileJSONEachRowPreservesUInt64VersionPrecision(t *testing.T) {
	const version = uint64(9007199254740993)
	row := map[string]any{
		"event_uuid": "01900000-0000-7000-8000-000000000001",
		"version":    version,
	}
	client := &Client{
		Host:     "http://clickhouse.test",
		Database: "db",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"event_uuid":"01900000-0000-7000-8000-000000000001","version":9007199254740993}` + "\n",
				)),
				Request: request,
			}, nil
		})},
	}
	result, err := client.ReconcileJSONEachRowContext(context.Background(), ReconcileSpec{
		LocalTable:    "db.target_local",
		KeyColumns:    []ReconcileKeyColumn{{Name: "event_uuid", Kind: ReconcileUUID}},
		VersionColumn: "version",
	}, []map[string]any{row})
	if err != nil || result.AcceptedRows != 1 || len(result.MissingRows) != 0 {
		t.Fatalf("result=%#v error=%v", result, err)
	}
}

func TestClientBaseURLUsesProtocolAndPath(t *testing.T) {
	c := &Client{
		Host:     "clickhouse.example.com",
		Port:     9440,
		Protocol: "https",
		HTTPPath: "/clickhouse",
	}

	got := c.baseURL()
	want := "https://clickhouse.example.com:9440/clickhouse/"
	if got != want {
		t.Fatalf("baseURL() = %q, want %q", got, want)
	}
}

func TestClientBaseURLAcceptsFullURLHost(t *testing.T) {
	c := &Client{
		Host: "https://clickhouse.example.com/proxy",
		Port: 8123,
	}

	got := c.baseURL()
	want := "https://clickhouse.example.com/proxy/"
	if got != want {
		t.Fatalf("baseURL() = %q, want %q", got, want)
	}
}

func TestNewFromEnvAcceptsClickHousePrefixedNames(t *testing.T) {
	t.Setenv("CLICKHOUSE_HOST", "clickhouse.example.com")
	t.Setenv("CLICKHOUSE_PORT", "9440")
	t.Setenv("CLICKHOUSE_USER", "book_user")
	t.Setenv("CLICKHOUSE_PASSWORD", "secret")
	t.Setenv("CLICKHOUSE_DATABASE", "book_db")
	t.Setenv("CLICKHOUSE_PROTOCOL", "https")
	t.Setenv("CLICKHOUSE_HTTP_URL_PATH", "clickhouse")

	c, err := NewFromEnv()
	if err != nil {
		t.Fatalf("NewFromEnv() error = %v", err)
	}
	if c.Host != "clickhouse.example.com" || c.Port != 9440 || c.User != "book_user" || c.Database != "book_db" {
		t.Fatalf("unexpected client from CLICKHOUSE_* env: %+v", c)
	}

	got := c.baseURL()
	want := "https://clickhouse.example.com:9440/clickhouse/"
	if got != want {
		t.Fatalf("baseURL() = %q, want %q", got, want)
	}
}

func TestSplitQualifiedTable(t *testing.T) {
	db, table := SplitQualifiedTable("Data_Book_NAVER_Log.naver_collect_log", "Data_Book_NAVER_Raw")
	if db != "Data_Book_NAVER_Log" || table != "naver_collect_log" {
		t.Fatalf("qualified split = %s.%s", db, table)
	}

	db, table = SplitQualifiedTable("naver_book_raw", "Data_Book_NAVER_Raw")
	if db != "Data_Book_NAVER_Raw" || table != "naver_book_raw" {
		t.Fatalf("unqualified split = %s.%s", db, table)
	}
}

func TestQualifiedTableIdentifierValidatesAndQuotes(t *testing.T) {
	tests := []struct {
		name            string
		raw             string
		defaultDatabase string
		want            string
		wantErr         bool
	}{
		{
			name:            "qualified",
			raw:             "Data_Book_NAVER_Log.naver_collect_log",
			defaultDatabase: "ignored",
			want:            "`Data_Book_NAVER_Log`.`naver_collect_log`",
		},
		{
			name:            "default database",
			raw:             "naver_book_raw",
			defaultDatabase: "Data_Book_NAVER_Raw",
			want:            "`Data_Book_NAVER_Raw`.`naver_book_raw`",
		},
		{name: "missing database", raw: "naver_book_raw", wantErr: true},
		{name: "too many parts", raw: "cluster.database.table", defaultDatabase: "default", wantErr: true},
		{name: "quoted input", raw: "`database`.`table`", defaultDatabase: "default", wantErr: true},
		{name: "SQL injection", raw: "database.table; DROP TABLE x", defaultDatabase: "default", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := QualifiedTableIdentifier(test.raw, test.defaultDatabase)
			if test.wantErr {
				if err == nil {
					t.Fatalf("QualifiedTableIdentifier(%q, %q) = %q, want error", test.raw, test.defaultDatabase, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("QualifiedTableIdentifier(%q, %q) error = %v", test.raw, test.defaultDatabase, err)
			}
			if got != test.want {
				t.Fatalf("QualifiedTableIdentifier(%q, %q) = %q, want %q", test.raw, test.defaultDatabase, got, test.want)
			}
		})
	}
}

func TestTableExistsUsesExactExistsQuery(t *testing.T) {
	var requests int
	var body string
	client := &Client{
		Host:     "http://clickhouse.test",
		Database: "Data_Book_NAVER_Raw",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body = string(payload)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("{\"result\":1}\n")),
				Request:    request,
			}, nil
		})},
	}

	exists, err := client.TableExists("naver_book_raw")
	if err != nil {
		t.Fatalf("TableExists() error = %v", err)
	}
	if !exists {
		t.Fatal("TableExists() = false, want true")
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
	if !strings.Contains(body, "EXISTS TABLE `Data_Book_NAVER_Raw`.`naver_book_raw`") {
		t.Fatalf("unexpected TableExists query: %s", body)
	}
	if strings.Contains(body, "system.tables") {
		t.Fatalf("TableExists queried system.tables: %s", body)
	}
}

func TestTableExistsRejectsUnsafeIdentifierBeforeRequest(t *testing.T) {
	requests := 0
	client := &Client{
		Database: "Data_Book_NAVER_Raw",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			return nil, nil
		})},
	}
	if _, err := client.TableExists("naver_book_raw; DROP TABLE x"); err == nil {
		t.Fatal("TableExists() accepted an unsafe identifier")
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want 0", requests)
	}
}

func TestExecSingleAttemptDoesNotRetryAmbiguousFailure(t *testing.T) {
	requests := 0
	client := &Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			return &http.Response{
				StatusCode: http.StatusGatewayTimeout,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    request,
			}, nil
		})},
	}
	if err := client.ExecSingleAttempt("INSERT INTO db.target SELECT 1"); err == nil {
		t.Fatal("ExecSingleAttempt() error = nil, want ambiguous failure")
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want exactly 1", requests)
	}
}

func TestInsertJSONEachRowDurableUsesFixedForegroundQuorumSettings(t *testing.T) {
	var body string
	client := &Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body = string(payload)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    request,
			}, nil
		})},
	}
	token := strings.Repeat("a", 64)
	if err := client.InsertJSONEachRowDurable("Data_Book_NLK_Raw.nlk_resource_raw", []map[string]any{{
		"resource_id": "urn:test:1",
		"version":     uint64(1),
	}}, token); err != nil {
		t.Fatalf("InsertJSONEachRowDurable() error=%v", err)
	}
	for _, expected := range []string{
		"insert_deduplicate = 1",
		"insert_deduplication_token = '" + token + "'",
		"distributed_foreground_insert = 1",
		"insert_quorum = 2",
		"insert_quorum_parallel = 1",
		"insert_quorum_timeout = 600000",
		"parallel_view_processing = 1",
		"receive_timeout = 660",
		"send_timeout = 660",
		"load_balancing = 'first_or_random'",
		"load_balancing_first_offset = 0",
		"prefer_localhost_replica = 0",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("durable insert body missing %q: %s", expected, body)
		}
	}
}

func TestInsertJSONEachRowSynchronousUsesDeterministicForegroundDelivery(t *testing.T) {
	requests := 0
	body := ""
	client := &Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body = string(payload)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    request,
			}, nil
		})},
	}
	token := strings.Repeat("a", 64)
	if err := client.InsertJSONEachRowSynchronous("db.target", []map[string]any{{"value": 1}}, token); err != nil {
		t.Fatalf("InsertJSONEachRowSynchronous() error=%v", err)
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want 1", requests)
	}
	for _, required := range []string{
		"insert_distributed_sync = 1",
		"insert_deduplicate = 1",
		"insert_deduplication_token = '" + token + "'",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("synchronous insert is missing %q: %s", required, body)
		}
	}
	if strings.Contains(body, "insert_quorum") {
		t.Fatalf("synchronous operational insert unexpectedly enabled quorum settings: %s", body)
	}
}

func TestInsertJSONEachRowSynchronousRejectsNonSHA256Token(t *testing.T) {
	client := &Client{}
	if err := client.InsertJSONEachRowSynchronous("db.target", []map[string]any{{"value": 1}}, "volatile"); err == nil {
		t.Fatal("expected invalid synchronous token error")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestInsertJSONEachRowDurableRejectsNonSHA256Token(t *testing.T) {
	client := &Client{}
	err := client.InsertJSONEachRowDurable("db.table", []map[string]any{{"value": 1}}, "volatile")
	if err == nil || err.Error() != "invalid durable insert deduplication token" {
		t.Fatalf("unexpected error=%v", err)
	}
}
