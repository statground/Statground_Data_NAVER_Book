package kakaostore

import (
	"context"
	"errors"
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
		"KAKAO_PROVIDER_LATEST_TABLE",
		"KAKAO_BOOK_CURRENT_VIEW",
		"KAKAO_CLICKHOUSE_RAW_WRITE_TIMEOUT_SECONDS",
		"KAKAO_REQUIRE_CLICKHOUSE_HTTPS",
	} {
		t.Setenv(name, "")
	}
	config := ConfigFromEnv()
	if config.RawTable != "Data_Book_KAKAO_Raw.kakao_book_raw" ||
		config.CallLogTable != "Data_Book_KAKAO_Log.kakao_api_call_log" ||
		config.FrontierTable != "Data_Book_KAKAO_Log.kakao_query_frontier" ||
		config.ProviderLatestTable != "Data_Book_Service.book_provider_latest" ||
		config.RawWriteTimeout != 660*time.Second ||
		!config.RequireHTTPS {
		t.Fatalf("unexpected default config: %#v", config)
	}
}

func TestNewClickHouseScopesLongTimeoutToRawInsertClient(t *testing.T) {
	client := testClient()
	client.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	config := ConfigFromEnv()
	config.RawWriteTimeout = 7 * time.Minute
	store, err := NewClickHouse(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if store.RawInsertClient == store.Client || store.RawInsertClient.HTTPClient == store.Client.HTTPClient {
		t.Fatal("raw insert timeout must use cloned clients")
	}
	if store.RawInsertClient.HTTPClient.Timeout != 7*time.Minute {
		t.Fatalf("raw insert timeout=%s, want 7m", store.RawInsertClient.HTTPClient.Timeout)
	}
	if store.Client.HTTPClient.Timeout != 60*time.Second {
		t.Fatalf("read/log client timeout changed to %s", store.Client.HTTPClient.Timeout)
	}
}

func TestRawWriteTimeoutEnvIsBounded(t *testing.T) {
	t.Setenv("KAKAO_CLICKHOUSE_RAW_WRITE_TIMEOUT_SECONDS", "360")
	if got := rawWriteTimeoutFromEnv(); got != 360*time.Second {
		t.Fatalf("raw write timeout=%s, want 6m", got)
	}
	for _, invalid := range []string{"59", "901", "invalid"} {
		t.Setenv("KAKAO_CLICKHOUSE_RAW_WRITE_TIMEOUT_SECONDS", invalid)
		if got := rawWriteTimeoutFromEnv(); got != 660*time.Second {
			t.Fatalf("raw write timeout for %q=%s, want 11m fallback", invalid, got)
		}
	}
}

func TestNewClickHouseRejectsUnsafeIdentifier(t *testing.T) {
	config := ConfigFromEnv()
	config.RawTable = "raw; DROP TABLE x"
	if _, err := NewClickHouse(testClient(), config); err == nil {
		t.Fatal("expected unsafe table identifier rejection")
	}
}

func TestLegacyCurrentViewEnvCannotRedirectProviderLookup(t *testing.T) {
	t.Setenv("KAKAO_BOOK_CURRENT_VIEW", "Data_Book_Service.v_book_provider_latest_current")
	t.Setenv("KAKAO_PROVIDER_LATEST_TABLE", "")
	config := ConfigFromEnv()
	if config.ProviderLatestTable != "Data_Book_Service.book_provider_latest" {
		t.Fatalf("legacy current-view env redirected provider lookup: %#v", config)
	}
	config.ProviderLatestTable = "provider_latest; DROP TABLE x"
	if _, err := NewClickHouse(testClient(), config); err == nil {
		t.Fatal("expected unsafe provider-latest identifier rejection")
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

func TestConnectionBoundaryAllowsExplicitKakaoRemoteIPHTTPOverride(t *testing.T) {
	t.Setenv("KAKAO_REQUIRE_CLICKHOUSE_HTTPS", "false")
	config := ConfigFromEnv()
	if config.RequireHTTPS {
		t.Fatal("explicit Kakao HTTP override was ignored")
	}

	client := testClient()
	client.Host = "192.0.2.10"
	client.Port = 50005
	store, err := NewClickHouse(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.validateConnectionBoundary(); err != nil {
		t.Fatalf("approved remote IP/HTTP endpoint rejected: %v", err)
	}

	store.Client.Host = "127.0.0.1"
	if err := store.validateConnectionBoundary(); err == nil {
		t.Fatal("HTTP override must not allow loopback ClickHouse endpoints")
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

func TestSafeStoreErrorReasonDoesNotExposeResponseBodies(t *testing.T) {
	for raw, want := range map[string]string{
		"clickhouse http status=400 secret query text": "query_rejected",
		"clickhouse http status=403 internal object":   "auth_or_permission",
		"clickhouse http status=408":                   "read_timeout",
		"context deadline exceeded for private host":   "transport_timeout",
		"read: connection reset by peer":               "transport_interrupted",
		"unclassified secret driver failure":           "request_failed",
	} {
		if got := safeStoreErrorReason(errors.New(raw)); got != want {
			t.Fatalf("safeStoreErrorReason(%q)=%q want=%q", raw, got, want)
		}
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
	// 8 writable tables plus provider latest, 8 INSERT grants, and 3 SELECT grants.
	if requests != 20 {
		t.Fatalf("preflight requests=%d, want 20", requests)
	}
}

func TestExistingContentHashesUsesBoundedProviderTableLookups(t *testing.T) {
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
		if !strings.Contains(query, "FROM Data_Book_Service.book_provider_latest") ||
			!strings.Contains(query, "provider = 'kakao'") ||
			!strings.Contains(query, "canonical_isbn IN (") ||
			!strings.Contains(query, "GROUP BY canonical_isbn") ||
			!strings.Contains(query, "argMax(content_hash, tuple(version, updated_at, ingested_at, uuid))") ||
			!strings.Contains(query, "optimize_skip_unused_shards = 1") {
			t.Fatalf("lookup did not use the bounded Kakao provider-table aggregation: %s", query)
		}
		if strings.Contains(query, "v_book_provider_latest_current") ||
			strings.Contains(query, "Data_Book_KAKAO_Raw.kakao_book_raw") {
			t.Fatalf("lookup scanned an unbounded view or raw table: %s", query)
		}
	}
}

func TestExistingContentHashesRetriesReadOnlyHTTP408(t *testing.T) {
	requests := 0
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		status := http.StatusRequestTimeout
		if requests == 2 {
			status = http.StatusOK
		}
		return &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ExistingContentHashes(context.Background(), []string{"9780000000002"}); err != nil {
		t.Fatalf("read-only 408 was not retried: %v", err)
	}
	if requests != 2 {
		t.Fatalf("read-only requests=%d, want 2", requests)
	}
}

func TestExistingContentHashesCapsPersistentHTTP408Retries(t *testing.T) {
	requests := 0
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{
			StatusCode: http.StatusRequestTimeout,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ExistingContentHashes(context.Background(), []string{"9780000000002"})
	if err == nil || !strings.Contains(err.Error(), "category=clickhouse_transient") {
		t.Fatalf("persistent 408 returned unexpected error: %v", err)
	}
	if requests != 3 {
		t.Fatalf("persistent read-only requests=%d, want 3", requests)
	}
}

func TestExistingContentHashesDoesNotRetryHTTP400(t *testing.T) {
	requests := 0
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ExistingContentHashes(context.Background(), []string{"9780000000002"}); err == nil {
		t.Fatal("expected non-retryable query rejection")
	}
	if requests != 1 {
		t.Fatalf("non-retryable requests=%d, want 1", requests)
	}
}

func TestExistingContentHashesStopsRetryWhenContextCanceled(t *testing.T) {
	requests := 0
	ctx, cancel := context.WithCancel(context.Background())
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		cancel()
		return &http.Response{
			StatusCode: http.StatusRequestTimeout,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ExistingContentHashes(ctx, []string{"9780000000002"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup error=%v, want context.Canceled", err)
	}
	if requests != 1 {
		t.Fatalf("canceled read-only requests=%d, want 1", requests)
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
