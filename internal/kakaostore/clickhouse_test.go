package kakaostore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		"KAKAO_DIRECT_OUTBOX_TABLE",
		"KAKAO_OUTBOX_REPLAY_LIMIT",
		"KAKAO_OUTBOX_MAX_REPLICA_QUEUE",
		"KAKAO_OUTBOX_MAX_REPLICA_DELAY_SECONDS",
		"KAKAO_BOOK_CURRENT_VIEW",
		"KAKAO_CLICKHOUSE_RAW_WRITE_TIMEOUT_SECONDS",
		"CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS",
		"CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS",
		"CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME",
		"KAKAO_REQUIRE_CLICKHOUSE_HTTPS",
	} {
		t.Setenv(name, "")
	}
	config := ConfigFromEnv()
	if config.RawTable != "Data_Book_KAKAO_Raw.kakao_book_raw" ||
		config.CallLogTable != "Data_Book_KAKAO_Log.kakao_api_call_log" ||
		config.FrontierTable != "Data_Book_KAKAO_Log.kakao_query_frontier" ||
		config.ProviderLatestTable != "Data_Book_Service.book_provider_latest" ||
		config.OutboxTable != "Data_Book_KAKAO_Log.kakao_direct_insert_outbox" ||
		config.OutboxReplayLimit != 25 ||
		config.OutboxMaxReplicaQueue != 1000 ||
		config.OutboxMaxReplicaDelaySeconds != 900 ||
		config.RawWriteTimeout != 660*time.Second ||
		config.PreflightRetryBudget != 90*time.Second ||
		config.PreflightRetryBackoff != 5*time.Second ||
		config.ExpectedEndpointHostname != "" ||
		!config.RequireHTTPS {
		t.Fatalf("unexpected default config: %#v", config)
	}
}

func TestConfigFromEnvReadsRequiredDirectEndpointHostname(t *testing.T) {
	t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "Clickhouse_S1_R1")
	if got := ConfigFromEnv().ExpectedEndpointHostname; got != "Clickhouse_S1_R1" {
		t.Fatalf("expected endpoint hostname=%q", got)
	}
}

func TestRetryableStoreErrorRecognizesClickHouse261TransientCodes(t *testing.T) {
	for _, code := range []int{286, 364, 574, 692, 733, 745, 749, 762} {
		if !retryableStoreError(fmt.Errorf("clickhouse http status=400 code=%d", code)) {
			t.Errorf("ClickHouse 26.1 transient code=%d was not classified transient", code)
		}
	}
	for _, code := range []int{60, 497, 516} {
		if retryableStoreError(fmt.Errorf("clickhouse http status=500 code=%d", code)) {
			t.Errorf("ClickHouse contract code=%d was classified transient", code)
		}
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

func TestPreflightRetryEnvIsBounded(t *testing.T) {
	t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS", "120")
	t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS", "3")
	config := ConfigFromEnv()
	if config.PreflightRetryBudget != 120*time.Second || config.PreflightRetryBackoff != 3*time.Second {
		t.Fatalf("preflight retry config=%s/%s, want 120s/3s", config.PreflightRetryBudget, config.PreflightRetryBackoff)
	}
	for _, invalid := range []string{"0", "601", "invalid"} {
		t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS", invalid)
		if got := ConfigFromEnv().PreflightRetryBudget; got != 90*time.Second {
			t.Fatalf("preflight budget for %q=%s, want 90s", invalid, got)
		}
	}
}

func TestRetryPreflightRecoversTransientFailure(t *testing.T) {
	store := &ClickHouseStore{Config: Config{PreflightRetryBackoff: time.Millisecond}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	calls := 0
	err := store.retryPreflight(ctx, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("calls=%d error=%v, want recovery on third attempt", calls, err)
	}
}

func TestRetryPreflightStopsAtContextBudget(t *testing.T) {
	store := &ClickHouseStore{Config: Config{PreflightRetryBackoff: 5 * time.Millisecond}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	calls := 0
	err := store.retryPreflight(ctx, func(context.Context) error {
		calls++
		return errors.New("connection refused")
	})
	if err == nil || calls < 2 || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("calls=%d elapsed=%s error=%v, want bounded transient retries", calls, time.Since(started), err)
	}
}

func TestRetryPreflightFailsContractImmediately(t *testing.T) {
	for _, message := range []string{
		"clickhouse http status=401",
		"clickhouse http status=500 code=60",
	} {
		store := &ClickHouseStore{Config: Config{PreflightRetryBackoff: time.Millisecond}}
		calls := 0
		err := store.retryPreflight(context.Background(), func(context.Context) error {
			calls++
			return errors.New(message)
		})
		if err == nil || calls != 1 {
			t.Fatalf("message=%q calls=%d error=%v, want immediate contract failure", message, calls, err)
		}
	}
}

func TestRetryPreflightHonorsCanceledContext(t *testing.T) {
	store := &ClickHouseStore{Config: Config{PreflightRetryBackoff: time.Second}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.retryPreflight(ctx, func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context.Canceled", err)
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
	t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "Clickhouse_S1_R1")
	requests := 0
	var bodies []string
	client := testClient()
	client.Host = "clickhouse.example.invalid"
	client.Protocol = "https"
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(body))
		responseBody := ""
		if strings.Contains(string(body), "SELECT hostName() AS value") {
			responseBody = "{\"value\":\"Clickhouse_S1_R1\"}\n"
		}
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
	// Endpoint identity, ten object checks, 8 target INSERT checks, 9
	// reconciliation SELECT checks, 3 outbox checks, system.replicas SELECT,
	// REMOTE, and one empty replay query.
	if requests != 34 {
		t.Fatalf("preflight requests=%d, want 34", requests)
	}
	if !strings.Contains(bodies[0], "SELECT hostName() AS value") {
		t.Fatalf("endpoint identity was not the first ClickHouse preflight query: %s", bodies[0])
	}
	wantReplicaGrant := "CHECK GRANT " + replicaHealthSelectPrivilege + " ON system.replicas"
	replicaGrantCount := 0
	remoteGrantCount := 0
	for _, body := range bodies {
		if strings.TrimSpace(body) == wantReplicaGrant {
			replicaGrantCount++
		}
		if strings.TrimSpace(body) == "CHECK GRANT REMOTE ON *.*" {
			remoteGrantCount++
		}
	}
	if replicaGrantCount != 1 {
		t.Fatalf("exact replica column grant preflight count=%d, want one query %q", replicaGrantCount, wantReplicaGrant)
	}
	if remoteGrantCount != 1 {
		t.Fatalf("REMOTE grant preflight count=%d, want one", remoteGrantCount)
	}
}

func TestValidateRejectsMissingOrMismatchedEndpointIdentityBeforeObjectQueries(t *testing.T) {
	for _, test := range []struct {
		name     string
		expected string
		actual   string
		wantHTTP int
	}{
		{name: "missing"},
		{name: "gateway", expected: "Clickhouse_Cluster_Gateway"},
		{name: "mismatch", expected: "Clickhouse_S1_R1", actual: "Clickhouse_S2_R1", wantHTTP: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := ConfigFromEnv()
			config.ExpectedEndpointHostname = test.expected
			client := testClient()
			client.Host = "clickhouse.example.invalid"
			client.Protocol = "https"
			requests := 0
			client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				requests++
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(body), "SELECT hostName() AS value") {
					t.Fatalf("object query ran before endpoint identity gate: %s", body)
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("{\"value\":\"" + test.actual + "\"}\n")),
					Request:    request,
				}, nil
			})}
			store, err := NewClickHouse(client, config)
			if err != nil {
				t.Fatal(err)
			}
			err = store.Validate(context.Background())
			if err == nil || !strings.Contains(err.Error(), "operation=preflight_endpoint_identity") || requests != test.wantHTTP {
				t.Fatalf("error=%v requests=%d, want endpoint fail-closed requests=%d", err, requests, test.wantHTTP)
			}
		})
	}
}

func TestInsertCallLogUsesSyncDeliveryAndFallsBackToLocalOutbox(t *testing.T) {
	var targetBodies []string
	var outboxBody string
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		body := string(payload)
		status := http.StatusOK
		responseBody := ""
		if strings.Contains(body, "INSERT INTO Data_Book_KAKAO_Log.kakao_api_call_log ") {
			targetBodies = append(targetBodies, body)
			status = http.StatusInternalServerError
			responseBody = "Code: 242. DB::Exception: Table is in readonly mode"
		} else if strings.Contains(body, "INSERT INTO Data_Book_KAKAO_Log.kakao_direct_insert_outbox ") {
			outboxBody = body
		}
		return &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(responseBody)),
			Request:    request,
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 1, 0, 1, 2, 0, time.UTC)
	err = store.InsertCallLog(context.Background(), CallLog{
		RequestUUID: "01900000-0000-7000-8000-000000000001",
		RunUUID:     "01900000-0000-7000-8000-000000000002",
		Version:     1,
		RequestedAt: now,
		Mode:        "manual",
		QueryHash:   "query-hash",
		Page:        1,
		Size:        50,
		Status:      "reserved",
	})
	if err != nil {
		t.Fatalf("InsertCallLog() error=%v", err)
	}
	if len(targetBodies) != 1 {
		t.Fatalf("target attempts=%d, want one non-retried attempt", len(targetBodies))
	}
	if !strings.Contains(targetBodies[0], "insert_distributed_sync = 1") {
		t.Fatalf("target insert was not synchronous: %s", targetBodies[0])
	}
	for _, required := range []string{
		"INSERT INTO Data_Book_KAKAO_Log.kakao_direct_insert_outbox",
		`"target_table":"Data_Book_KAKAO_Log.kakao_api_call_log"`,
		`"target_local_table":"Data_Book_KAKAO_Log.kakao_api_call_log_local"`,
		`"source_error":"server_unavailable"`,
	} {
		if !strings.Contains(outboxBody, required) {
			t.Fatalf("outbox insert is missing %q: %s", required, outboxBody)
		}
	}
}

func TestCanceledKakaoTargetContextStillUsesIndependentBoundedOutboxContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	requests := 0
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		payload, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		body := string(payload)
		if strings.Contains(body, "INSERT INTO Data_Book_KAKAO_Log.kakao_api_call_log ") {
			cancel()
			return nil, context.Canceled
		}
		if request.Context().Err() != nil {
			t.Fatalf("outbox inherited canceled target context: %v", request.Context().Err())
		}
		if !strings.Contains(body, "INSERT INTO Data_Book_KAKAO_Log.kakao_direct_insert_outbox ") {
			t.Fatalf("unexpected second request: %s", body)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	err = store.InsertCallLog(ctx, CallLog{
		RequestUUID: "01900000-0000-7000-8000-000000000001",
		RunUUID:     "01900000-0000-7000-8000-000000000002",
		Version:     1,
		RequestedAt: time.Date(2026, 9, 1, 0, 1, 2, 0, time.UTC),
		Mode:        "manual",
		QueryHash:   "query-hash",
		Page:        1,
		Size:        50,
		Status:      "reserved",
	})
	if err != nil {
		t.Fatalf("independent Kakao outbox persistence error=%v", err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want target plus independent outbox", requests)
	}
}

func TestReplayOutboxIsHealthGatedAndMarksSuccessfulBatch(t *testing.T) {
	targetRows := []map[string]any{{
		"requested_at": "2026-09-01 09:00:00.000",
		"request_uuid": "01900000-0000-7000-8000-000000000001",
		"version":      1,
	}}
	_, rowsJSON, token, err := encodeOutboxRows("Data_Book_KAKAO_Log.kakao_api_call_log", targetRows)
	if err != nil {
		t.Fatal(err)
	}
	pendingJSON, err := json.Marshal(map[string]any{
		"outbox_uuid":         "01900000-0000-7000-8000-000000000003",
		"target_table":        "Data_Book_KAKAO_Log.kakao_api_call_log",
		"target_local_table":  "Data_Book_KAKAO_Log.kakao_api_call_log_local",
		"rows_json":           rowsJSON,
		"row_count":           1,
		"deduplication_token": token,
	})
	if err != nil {
		t.Fatal(err)
	}
	targetWrites := 0
	marks := 0
	reconciliationQueries := 0
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		body := string(payload)
		responseBody := ""
		switch {
		case strings.Contains(body, "FROM Data_Book_KAKAO_Log.kakao_direct_insert_outbox"):
			if strings.Count(body, "WHERE replayed_at IS NULL") != 1 || strings.Count(body, "\n        WHERE ") != 1 {
				t.Fatalf("pending outbox query must contain exactly one WHERE clause: %s", body)
			}
			responseBody = string(pendingJSON) + "\n"
		case strings.Contains(body, "FROM system.replicas"):
			if !strings.Contains(body, "absolute_delay <= 900") || !strings.Contains(body, "queue_size <= 1000") {
				t.Fatalf("health gate is missing bounded replica delay/queue: %s", body)
			}
			responseBody = `{"replicas":1,"writable":1}` + "\n"
		case strings.Contains(body, "FROM clusterAllReplicas"):
			reconciliationQueries++
			if reconciliationQueries > 1 {
				encoded, marshalErr := json.Marshal(targetRows[0])
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				responseBody = string(encoded) + "\n"
			}
		case strings.Contains(body, "INSERT INTO Data_Book_KAKAO_Log.kakao_api_call_log "):
			targetWrites++
			if !strings.Contains(body, "insert_distributed_sync = 1") || !strings.Contains(body, token) {
				t.Fatalf("replay changed sync/token contract: %s", body)
			}
		case strings.Contains(body, "ALTER TABLE Data_Book_KAKAO_Log.kakao_direct_insert_outbox"):
			marks++
		default:
			t.Fatalf("unexpected replay request: %s", body)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(responseBody)),
			Request:    request,
		}, nil
	})}
	config := ConfigFromEnv()
	store, err := NewClickHouse(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.replayOutbox(context.Background()); err != nil {
		t.Fatalf("replayOutbox() error=%v", err)
	}
	if targetWrites != 1 || marks != 1 {
		t.Fatalf("target writes=%d marks=%d, want 1/1", targetWrites, marks)
	}
}

func TestReplayOutboxRejectsTamperedPayloadTokenBeforeAnyTargetReadOrWrite(t *testing.T) {
	targetRows := []map[string]any{{
		"requested_at": "2026-09-01 09:00:00.000",
		"request_uuid": "01900000-0000-7000-8000-000000000001",
		"version":      1,
	}}
	_, rowsJSON, token, err := encodeOutboxRows("Data_Book_KAKAO_Log.kakao_api_call_log", targetRows)
	if err != nil {
		t.Fatal(err)
	}
	tamperedRowsJSON := strings.Replace(rowsJSON, `"version":1`, `"version":2`, 1)
	if tamperedRowsJSON == rowsJSON {
		t.Fatal("test fixture did not change the persisted payload")
	}
	pendingJSON, err := json.Marshal(map[string]any{
		"outbox_uuid":         "01900000-0000-7000-8000-000000000003",
		"target_table":        "Data_Book_KAKAO_Log.kakao_api_call_log",
		"target_local_table":  "Data_Book_KAKAO_Log.kakao_api_call_log_local",
		"rows_json":           tamperedRowsJSON,
		"row_count":           1,
		"deduplication_token": token,
	})
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		payload, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !strings.Contains(string(payload), "FROM Data_Book_KAKAO_Log.kakao_direct_insert_outbox") {
			t.Fatalf("tampered outbox reached another ClickHouse operation: %s", payload)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(string(pendingJSON) + "\n")),
			Request:    request,
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	err = store.replayOutbox(context.Background())
	if err == nil || !strings.Contains(err.Error(), "reason=payload_rejected") {
		t.Fatalf("tampered payload error=%v, want payload_rejected", err)
	}
	if requests != 1 {
		t.Fatalf("tampered payload requests=%d, want pending-read only", requests)
	}
}

func TestReconcileOutboxRowsMapsEveryKakaoTargetToItsLogicalIdentity(t *testing.T) {
	config := ConfigFromEnv()
	tests := []struct {
		name       string
		target     string
		local      string
		row        map[string]any
		queryParts []string
	}{
		{
			name: "raw", target: config.RawTable, local: config.RawLocalTable,
			row: map[string]any{
				"collected_at": "2026-09-01 09:00:00.000",
				"event_uuid":   "01900000-0000-7000-8000-000000000001",
			},
			queryParts: []string{"kakao_book_raw_local", "`collected_at`", "`event_uuid`"},
		},
		{
			name: "call", target: config.CallLogTable, local: config.CallLocalTable,
			row: map[string]any{
				"requested_at": "2026-09-01 09:00:00.000",
				"request_uuid": "01900000-0000-7000-8000-000000000002",
				"version":      1,
			},
			queryParts: []string{"kakao_api_call_log_local", "`requested_at`", "`request_uuid`", "`version`"},
		},
		{
			name: "collect", target: config.CollectLogTable, local: config.CollectLocalTable,
			row: map[string]any{
				"collected_at": "2026-09-01 09:00:00.000",
				"run_uuid":     "01900000-0000-7000-8000-000000000003",
				"log_uuid":     "01900000-0000-7000-8000-000000000004",
				"version":      1,
			},
			queryParts: []string{"kakao_collect_log_local", "`collected_at`", "`run_uuid`", "`log_uuid`", "`version`"},
		},
		{
			name: "frontier", target: config.FrontierTable, local: config.FrontierLocal,
			row:        map[string]any{"provider": "kakao", "query_hash": "hash", "version": 1},
			queryParts: []string{"kakao_query_frontier_local", "`provider`", "`query_hash`", "`version`"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var query string
			client := testClient()
			client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				body, readErr := io.ReadAll(request.Body)
				if readErr != nil {
					t.Fatal(readErr)
				}
				query = string(body)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    request,
				}, nil
			})}
			store, err := NewClickHouse(client, config)
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.reconcileOutboxRows(context.Background(), test.target, test.local, []map[string]any{test.row})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.MissingRows) != 1 || !strings.Contains(query, "clusterAllReplicas('statground_cluster'") {
				t.Fatalf("result=%#v query=%s", result, query)
			}
			for _, part := range test.queryParts {
				if !strings.Contains(query, part) {
					t.Fatalf("query missing %q: %s", part, query)
				}
			}
		})
	}
}

func TestLocalReplicaHealthRejectsStaleDelay(t *testing.T) {
	var query string
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		query = string(body)
		// A replica whose absolute_delay exceeds the configured predicate is
		// excluded by countIf and therefore returns writable=0.
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("{\"replicas\":1,\"writable\":0}\n")),
			Request:    request,
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := store.localReplicaHealthy(context.Background(), store.Config.FrontierLocal)
	if err != nil {
		t.Fatal(err)
	}
	if healthy {
		t.Fatal("stale replica passed the outbox replay health gate")
	}
	if !strings.Contains(query, "absolute_delay <= 900") || !strings.Contains(query, "queue_size <= 1000") {
		t.Fatalf("replica health query is missing bounded delay/queue predicates: %s", query)
	}
}

func TestReplayOutboxDefersWhenLocalReplicaIsNotHealthy(t *testing.T) {
	_, rowsJSON, token, err := encodeOutboxRows("Data_Book_KAKAO_Log.kakao_query_frontier", []map[string]any{{"version": 1}})
	if err != nil {
		t.Fatal(err)
	}
	pendingJSON, _ := json.Marshal(map[string]any{
		"outbox_uuid":         "01900000-0000-7000-8000-000000000004",
		"target_table":        "Data_Book_KAKAO_Log.kakao_query_frontier",
		"target_local_table":  "Data_Book_KAKAO_Log.kakao_query_frontier_local",
		"rows_json":           rowsJSON,
		"row_count":           1,
		"deduplication_token": token,
	})
	targetWrites := 0
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		payload, _ := io.ReadAll(request.Body)
		body := string(payload)
		responseBody := ""
		switch {
		case strings.Contains(body, "FROM Data_Book_KAKAO_Log.kakao_direct_insert_outbox"):
			responseBody = string(pendingJSON) + "\n"
		case strings.Contains(body, "FROM system.replicas"):
			responseBody = `{"replicas":1,"writable":0}` + "\n"
		case strings.Contains(body, "INSERT INTO Data_Book_KAKAO_Log.kakao_query_frontier "):
			targetWrites++
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(responseBody)), Request: request}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	err = store.replayOutbox(context.Background())
	if err == nil || !strings.Contains(err.Error(), "category=clickhouse_transient") || targetWrites != 0 {
		t.Fatalf("error=%v targetWrites=%d, want health-gated deferral", err, targetWrites)
	}
}

func TestOutboxEncodingUsesCanonicalReplayPayloadAndBoundsRows(t *testing.T) {
	rows := []map[string]any{{
		"requested_at": time.Date(2026, 9, 1, 0, 1, 2, 345000000, time.UTC),
		"query":        "<R>&",
		"version":      uint64(1),
	}}
	canonical, rowsJSON, token, err := encodeOutboxRows("Data_Book_KAKAO_Log.kakao_api_call_log", rows)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := decodeOutboxRows(rowsJSON, 1)
	if err != nil {
		t.Fatal(err)
	}
	columns, _, firstPayload, err := ch.CanonicalJSONEachRow(canonical)
	if err != nil {
		t.Fatal(err)
	}
	replayColumns, _, replayPayload, err := ch.CanonicalJSONEachRow(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstPayload) != string(replayPayload) || strings.Join(columns, ",") != strings.Join(replayColumns, ",") {
		t.Fatalf("Kakao outbox replay changed canonical payload\nfirst=%s\nreplay=%s", firstPayload, replayPayload)
	}
	if token != ch.JSONEachRowDeduplicationToken("Data_Book_KAKAO_Log.kakao_api_call_log", columns, firstPayload) {
		t.Fatalf("token is not bound to the canonical target payload: %s", token)
	}
	if strings.Contains(rowsJSON, "T00:01:02") || !strings.Contains(rowsJSON, "2026-09-01 09:01:02.345") {
		t.Fatalf("stored rows are not ClickHouse-canonical: %s", rowsJSON)
	}
	if _, _, _, err := encodeOutboxRows("Data_Book_KAKAO_Log.kakao_api_call_log", make([]map[string]any, maxOutboxRows+1)); err == nil {
		t.Fatal("oversized Kakao row batch was accepted")
	}
	if _, err := decodeOutboxRows(rowsJSON+`{}`, 1); err == nil {
		t.Fatal("Kakao outbox payload with trailing JSON was accepted")
	}
}

func TestMarkOutboxReplayedUsesOneMutationForBoundedBatch(t *testing.T) {
	var bodies []string
	client := testClient()
	client.HTTPClient = &http.Client{Transport: storeRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(body))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	})}
	store, err := NewClickHouse(client, ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}
	uuids := make([]string, 25)
	for index := range uuids {
		uuids[index] = fmt.Sprintf("01900000-0000-7000-8000-%012d", index)
	}
	if err := store.markOutboxReplayed(context.Background(), uuids); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 {
		t.Fatalf("mark requests=%d, want one mutation", len(bodies))
	}
	if strings.Count(bodies[0], "toUUID(") != len(uuids) || !strings.Contains(bodies[0], "mutations_sync = 1") {
		t.Fatalf("bounded UUIDs were not marked by one synchronous mutation: %s", bodies[0])
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
