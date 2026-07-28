package kakaostore

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
)

func TestConfigFromEnvDefaults(t *testing.T) {
	for _, name := range []string{
		"KAKAO_RAW_TABLE",
		"KAKAO_RAW_LOCAL_TABLE",
		"KAKAO_COLLECT_LOG_TABLE",
		"KAKAO_COLLECT_LOG_LOCAL_TABLE",
		"KAKAO_API_CALL_LOG_TABLE",
		"KAKAO_API_CALL_LOG_LOCAL_TABLE",
		"KAKAO_QUERY_FRONTIER_TABLE",
		"KAKAO_QUERY_FRONTIER_LOCAL_TABLE",
		"KAKAO_BOOK_CURRENT_VIEW",
		"KAKAO_REQUIRE_CLICKHOUSE_HTTPS",
	} {
		t.Setenv(name, "")
	}
	config := ConfigFromEnv()
	if config.RawTable != "Data_Book_KAKAO_Raw.kakao_book_raw" ||
		config.CallLogTable != "Data_Book_KAKAO_Log.kakao_api_call_log" ||
		config.FrontierTable != "Data_Book_KAKAO_Log.kakao_query_frontier" ||
		config.CurrentView != "Data_Book_Service.v_book_provider_latest_current" ||
		!config.RequireHTTPS {
		t.Fatalf("unexpected default config: %#v", config)
	}
}

func TestNewClickHouseRejectsUnsafeIdentifier(t *testing.T) {
	config := ConfigFromEnv()
	config.RawTable = "raw; DROP TABLE x"
	if _, err := NewClickHouse(testClient(), config); err == nil {
		t.Fatal("expected unsafe table identifier rejection")
	}
}

func TestConnectionBoundaryRejectsLoopbackAndPlainHTTP(t *testing.T) {
	config := ConfigFromEnv()
	store, err := NewClickHouse(testClient(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.validateConnectionBoundary(); err == nil {
		t.Fatal("expected loopback/plain HTTP rejection")
	}

	store.Client.Host = "clickhouse.example.invalid"
	if err := store.validateConnectionBoundary(); err == nil {
		t.Fatal("expected plain HTTP rejection")
	}
	store.Client.Protocol = "https"
	if err := store.validateConnectionBoundary(); err != nil {
		t.Fatalf("HTTPS host rejected: %v", err)
	}
}

func TestParseTimeAndAllowlist(t *testing.T) {
	if got := parseTime("2026-07-26 12:34:56.000"); got.IsZero() {
		t.Fatal("ClickHouse time was not parsed")
	}
	if got := allowedErrorCategory("raw upstream error"); got != "unknown" {
		t.Fatalf("unexpected error category=%q", got)
	}
	if got := nullableTime(time.Time{}); got != nil {
		t.Fatalf("zero nullable time=%v", got)
	}
}

func TestQuotaStopBlockedUsesConservativeOperationalWindows(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	quotaStop := QuotaStop{Found: true, Category: "quota_exhausted", StoppedAt: now.Add(-23 * time.Hour)}
	if !QuotaStopBlocked(quotaStop, now, 24*time.Hour, 30*time.Minute) {
		t.Fatal("quota exhaustion must remain blocked for the configured 24-hour hold")
	}
	if QuotaStopBlocked(quotaStop, now.Add(2*time.Hour), 24*time.Hour, 30*time.Minute) {
		t.Fatal("expired quota hold remained blocked")
	}
	rateStop := QuotaStop{Found: true, Category: "rate_limited", StoppedAt: now.Add(-10 * time.Minute)}
	if !QuotaStopBlocked(rateStop, now, 24*time.Hour, 30*time.Minute) {
		t.Fatal("recent rate limit must remain blocked")
	}
	if QuotaStopBlocked(rateStop, now.Add(30*time.Minute), 24*time.Hour, 30*time.Minute) {
		t.Fatal("expired rate-limit hold remained blocked")
	}
}

func TestValidatePreflightsDistributedLocalTablesAndGrants(t *testing.T) {
	requests := 0
	client := testClient()
	client.Host = "clickhouse.example.invalid"
	client.Protocol = "https"
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		responseBody := ""
		if strings.Contains(string(body), "EXISTS TABLE") {
			responseBody = "{\"result\":1}\n"
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(responseBody)),
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Validate(context.Background()); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	// 8 writable tables plus the serving view, 8 INSERT grants, and 3 SELECT grants.
	if requests != 20 {
		t.Fatalf("preflight requests=%d, want 20", requests)
	}
}

func TestExistingContentHashesUsesBoundedServingViewLookups(t *testing.T) {
	var queries []string
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		queries = append(queries, string(body))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	isbns := make([]string, 501)
	for index := range isbns {
		isbns[index] = "isbn-" + strconv.Itoa(index)
	}
	if _, err := store.ExistingContentHashes(context.Background(), isbns); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 {
		t.Fatalf("lookup queries=%d, want 2 bounded batches", len(queries))
	}
	for _, query := range queries {
		if !strings.Contains(query, "FROM Data_Book_Service.v_book_provider_latest_current") ||
			!strings.Contains(query, "provider = 'kakao'") {
			t.Fatalf("lookup did not use the Kakao serving view: %s", query)
		}
		if strings.Contains(query, "Data_Book_KAKAO_Raw.kakao_book_raw") {
			t.Fatalf("lookup scanned the raw table: %s", query)
		}
	}
}

type storeRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn storeRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func testClient() *ch.Client {
	return &ch.Client{
		Host:     "127.0.0.1",
		Port:     8123,
		Protocol: "http",
		Database: "Data_Book_KAKAO_Raw",
	}
}
