package ch

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

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
