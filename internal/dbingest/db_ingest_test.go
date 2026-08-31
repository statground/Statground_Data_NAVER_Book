package dbingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
)

func TestValidateUsesExactExistsTablePreflight(t *testing.T) {
	t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "Clickhouse_S1_R1")
	var queries []string
	client := &ch.Client{
		Host:     "http://clickhouse.test",
		Database: "Data_Book_NAVER_Raw",
		HTTPClient: &http.Client{Transport: dbRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			queries = append(queries, string(payload))
			responseBody := "{\"result\":1}\n"
			if strings.Contains(string(payload), "SELECT hostName() AS value") {
				responseBody = "{\"value\":\"Clickhouse_S1_R1\"}\n"
			}
			if strings.Contains(string(payload), "WHERE replayed_at IS NULL") {
				responseBody = ""
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}
	writer, err := NewFromEnv(client, "naver_book_raw")
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Validate(context.Background()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if len(queries) != 23 {
		t.Fatalf("preflight queries = %d, want 23", len(queries))
	}
	identityQueries := 0
	existsQueries := 0
	grantQueries := 0
	replayQueries := 0
	for _, query := range queries {
		switch {
		case strings.Contains(query, "SELECT hostName() AS value"):
			identityQueries++
		case strings.Contains(query, "EXISTS TABLE `"):
			existsQueries++
			if strings.Contains(query, "system.tables") {
				t.Fatalf("unsafe table preflight query: %s", query)
			}
		case strings.HasPrefix(strings.TrimSpace(query), "CHECK GRANT"):
			grantQueries++
			if strings.TrimSpace(query) == "CHECK GRANT REMOTE ON *.*" {
				continue
			}
			if strings.Contains(query, "system`.`replicas") {
				want := "CHECK GRANT " + replicaHealthSelectPrivilege + " ON `system`.`replicas`"
				if strings.TrimSpace(query) != want {
					t.Fatalf("replica preflight grant=%q, want exact column grant %q", strings.TrimSpace(query), want)
				}
			}
		case strings.Contains(query, "WHERE replayed_at IS NULL"):
			replayQueries++
		default:
			t.Fatalf("unexpected preflight query: %s", query)
		}
	}
	if identityQueries != 1 || existsQueries != 7 || grantQueries != 14 || replayQueries != 1 {
		t.Fatalf("preflight query classes identity=%d exists=%d grants=%d replay=%d", identityQueries, existsQueries, grantQueries, replayQueries)
	}
	if !strings.Contains(queries[0], "SELECT hostName() AS value") {
		t.Fatalf("endpoint identity was not the first preflight query: %s", queries[0])
	}
}

func TestValidateRejectsMissingOrMismatchedEndpointIdentityBeforeObjectQueries(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "")
		requests := 0
		client := &ch.Client{
			Host:     "http://clickhouse.test",
			Database: "Data_Book_NAVER_Raw",
			HTTPClient: &http.Client{Transport: dbRoundTripFunc(func(*http.Request) (*http.Response, error) {
				requests++
				return nil, errors.New("transport must not be reached")
			})},
		}
		writer, err := NewFromEnv(client, "naver_book_raw")
		if err != nil {
			t.Fatal(err)
		}
		err = writer.Validate(context.Background())
		if err == nil || !strings.Contains(err.Error(), "preflight_endpoint_identity") || requests != 0 {
			t.Fatalf("error=%v requests=%d, want fail-closed before transport", err, requests)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "Clickhouse_S1_R1")
		requests := 0
		client := &ch.Client{
			Host:     "http://clickhouse.test",
			Database: "Data_Book_NAVER_Raw",
			HTTPClient: &http.Client{Transport: dbRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				requests++
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(body), "SELECT hostName() AS value") {
					t.Fatalf("object query ran before endpoint identity gate: %s", body)
				}
				return dbResponse(request, http.StatusOK, "{\"value\":\"Clickhouse_S2_R1\"}\n"), nil
			})},
		}
		writer, err := NewFromEnv(client, "naver_book_raw")
		if err != nil {
			t.Fatal(err)
		}
		err = writer.Validate(context.Background())
		if err == nil || !strings.Contains(err.Error(), "preflight_endpoint_identity") || requests != 1 {
			t.Fatalf("error=%v requests=%d, want one mismatched identity query", err, requests)
		}
	})
}

func TestNewFromEnvUsesBoundedEndpointLocalOutboxDefaults(t *testing.T) {
	t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "Clickhouse_S1_R1")
	writer, err := NewFromEnv(&ch.Client{Database: "Data_Book_NAVER_Raw"}, "naver_book_raw")
	if err != nil {
		t.Fatal(err)
	}
	if writer.Cfg.RawLocalTable != "Data_Book_NAVER_Raw.naver_book_raw_local" {
		t.Fatalf("raw local table=%q", writer.Cfg.RawLocalTable)
	}
	if writer.Cfg.CollectLogLocalTable != "Data_Book_NAVER_Log.naver_collect_log_local" {
		t.Fatalf("collect local table=%q", writer.Cfg.CollectLogLocalTable)
	}
	if writer.Cfg.PublisherCacheLocal != "Data_Book_NAVER_Log.aladin_publisher_cache_local" {
		t.Fatalf("publisher local table=%q", writer.Cfg.PublisherCacheLocal)
	}
	if writer.Cfg.OutboxTable != "Data_Book_NAVER_Log.naver_direct_insert_outbox" {
		t.Fatalf("outbox table=%q", writer.Cfg.OutboxTable)
	}
	if writer.Cfg.ExpectedEndpointHostname != "Clickhouse_S1_R1" {
		t.Fatalf("expected endpoint hostname=%q", writer.Cfg.ExpectedEndpointHostname)
	}
	if writer.Cfg.OutboxReplayLimit != 25 || writer.Cfg.OutboxMaxReplicaQueue != 1000 || writer.Cfg.OutboxMaxReplicaDelaySeconds != 900 {
		t.Fatalf("outbox bounds replay=%d queue=%d delay=%d", writer.Cfg.OutboxReplayLimit, writer.Cfg.OutboxMaxReplicaQueue, writer.Cfg.OutboxMaxReplicaDelaySeconds)
	}
}

func TestNewEventBuildsDirectPayload(t *testing.T) {
	t.Setenv("PRODUCER_SOURCE", "test_source")
	t.Setenv("PRODUCER_HOST", "test_host")
	t.Setenv("PRODUCER_IP", "::")

	writer, err := NewFromEnv(&ch.Client{Database: "Data_Book_NAVER_Raw"}, "naver_book_raw")
	if err != nil {
		t.Fatalf("NewFromEnv() error = %v", err)
	}
	ev, err := writer.NewEvent("book.naver.raw.v1", "01900000-0000-7000-8000-000000000000", "https://example.com/book", "2026-07-01 12:00:00.000", map[string]any{
		"isbn": "1234567890",
	})
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if ev.Source != "test_source" || ev.Host != "test_host" || ev.IP != "::" {
		t.Fatalf("unexpected producer fields: %+v", ev)
	}
	if !strings.Contains(ev.Payload, `"isbn":"1234567890"`) {
		t.Fatalf("payload = %s", ev.Payload)
	}
}

type dbRoundTripFunc func(*http.Request) (*http.Response, error)

func (function dbRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestWithTimeoutClonesHTTPClient(t *testing.T) {
	writer, err := NewFromEnv(ch.New("clickhouse.example.com", 8123, "user", "pass", "Data_Book_NAVER_Raw"), "naver_book_raw")
	if err != nil {
		t.Fatalf("NewFromEnv() error = %v", err)
	}
	clone := writer.WithTimeout(2500 * time.Millisecond)
	if clone == writer {
		t.Fatal("WithTimeout should clone writer")
	}
	if clone.Client.HTTPClient.Timeout != 2500*time.Millisecond {
		t.Fatalf("timeout = %s", clone.Client.HTTPClient.Timeout)
	}
	if writer.Client.HTTPClient.Timeout == clone.Client.HTTPClient.Timeout {
		t.Fatal("WithTimeout should not mutate the original writer client")
	}
}

func TestTransientDistributedInsertIsPreservedInLocalOutbox(t *testing.T) {
	var bodies []string
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(body))
		if strings.HasPrefix(string(body), "INSERT INTO naver_book_raw ") {
			return dbResponse(request, http.StatusServiceUnavailable, ""), nil
		}
		return dbResponse(request, http.StatusOK, ""), nil
	})
	writer := testWriter(client)
	rows := []map[string]any{{
		"isbn":         "9780000000001",
		"title":        "<book>&",
		"version":      1,
		"collected_at": time.Date(2026, 9, 1, 0, 1, 2, 345000000, time.UTC),
	}}

	if err := writer.InsertRawRows(rows); err != nil {
		t.Fatalf("InsertRawRows() error=%v", err)
	}
	if len(bodies) != 2 {
		t.Fatalf("request count=%d, want one target attempt and one outbox insert", len(bodies))
	}
	tokenPattern := regexp.MustCompile(`insert_deduplication_token = '([0-9a-f]{64})'`)
	firstToken := tokenPattern.FindStringSubmatch(bodies[0])
	if len(firstToken) != 2 {
		t.Fatalf("target request did not use one deterministic token")
	}
	if !strings.Contains(bodies[0], "insert_distributed_sync = 1") {
		t.Fatalf("target insert is not synchronous: %s", bodies[0])
	}
	if !strings.HasPrefix(bodies[1], "INSERT INTO Data_Book_NAVER_Log.naver_direct_insert_outbox ") {
		t.Fatalf("fallback did not target local outbox: %s", bodies[1])
	}
	if !strings.Contains(bodies[1], `"source_error":"server_unavailable"`) || strings.Contains(bodies[1], "clickhouse.test") {
		t.Fatalf("outbox error is not safely categorized: %s", bodies[1])
	}
	if !strings.Contains(bodies[1], firstToken[1]) {
		t.Fatalf("outbox did not preserve target token")
	}
	targetPayload := strings.SplitN(bodies[0], "FORMAT JSONEachRow\n", 2)
	outboxPayload := strings.SplitN(bodies[1], "FORMAT JSONEachRow\n", 2)
	if len(targetPayload) != 2 || len(outboxPayload) != 2 {
		t.Fatalf("missing JSONEachRow payload target=%q outbox=%q", bodies[0], bodies[1])
	}
	var outboxRecord map[string]any
	decoder := json.NewDecoder(strings.NewReader(outboxPayload[1]))
	decoder.UseNumber()
	if err := decoder.Decode(&outboxRecord); err != nil {
		t.Fatal(err)
	}
	replayRows, err := decodeOutboxRows(outboxRecord["rows_json"].(string), 1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, replayPayload, err := ch.CanonicalJSONEachRow(replayRows)
	if err != nil {
		t.Fatal(err)
	}
	if targetPayload[1] != string(replayPayload) {
		t.Fatalf("first insert and replay payload differ\nfirst=%s\nreplay=%s", targetPayload[1], replayPayload)
	}
	if strings.Contains(outboxRecord["rows_json"].(string), "T00:01:02") ||
		!strings.Contains(outboxRecord["rows_json"].(string), "2026-09-01 09:01:02.345") {
		t.Fatalf("outbox did not store ClickHouse-canonical time: %s", outboxRecord["rows_json"])
	}
}

func TestNonTransientDistributedInsertDoesNotEnterOutbox(t *testing.T) {
	requests := 0
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		requests++
		return dbResponse(request, http.StatusBadRequest, "Code: 62. Syntax error"), nil
	})
	err := testWriter(client).InsertCollectLogRows([]map[string]any{{"log_uuid": "01900000-0000-7000-8000-000000000001"}})
	if err == nil || !strings.Contains(err.Error(), "category=clickhouse_contract") {
		t.Fatalf("expected contract failure, got %v", err)
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want one target attempt and no outbox insert", requests)
	}
}

func TestCanceledTargetContextStillUsesIndependentBoundedOutboxContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	requests := 0
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		requests++
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.HasPrefix(string(body), "INSERT INTO naver_book_raw ") {
			cancel()
			return nil, context.Canceled
		}
		if request.Context().Err() != nil {
			t.Fatalf("outbox inherited canceled target context: %v", request.Context().Err())
		}
		if !strings.HasPrefix(string(body), "INSERT INTO Data_Book_NAVER_Log.naver_direct_insert_outbox ") {
			t.Fatalf("unexpected second request: %s", body)
		}
		return dbResponse(request, http.StatusOK, ""), nil
	})
	err := testWriter(client).InsertRawRowsContext(ctx, []map[string]any{{
		"created_at": "2026-09-01 09:00:00.000", "provider": "naver",
		"isbn": "9780000000001", "version": 1,
	}})
	if err != nil {
		t.Fatalf("independent outbox persistence error=%v", err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want target plus independent outbox", requests)
	}
}

func TestReplayOutboxUsesReplicaGateSameTokenAndMarksSuccess(t *testing.T) {
	replayRow := map[string]any{
		"created_at": "2026-09-01 09:00:00.000", "provider": "naver",
		"isbn": "9780000000001", "version": 1,
	}
	_, rowsJSON, token, err := encodeOutboxRows("naver_book_raw", []map[string]any{replayRow})
	if err != nil {
		t.Fatal(err)
	}
	outboxUUID := "01900000-0000-7000-8000-000000000002"
	var bodies []string
	reconciliationQueries := 0
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		query := string(body)
		bodies = append(bodies, query)
		switch {
		case strings.Contains(query, "WHERE replayed_at IS NULL"):
			if strings.Count(query, "WHERE replayed_at IS NULL") != 1 || strings.Count(query, "\n        WHERE ") != 1 {
				t.Fatalf("pending outbox query must contain exactly one WHERE clause: %s", query)
			}
			return dbResponse(request, http.StatusOK, fmtJSONRow(map[string]any{
				"outbox_uuid": outboxUUID, "target_table": "naver_book_raw",
				"target_local_table": "Data_Book_NAVER_Raw.naver_book_raw_local",
				"rows_json":          rowsJSON, "row_count": 1, "deduplication_token": token,
			})), nil
		case strings.Contains(query, "FROM system.replicas"):
			return dbResponse(request, http.StatusOK, "{\"replicas\":1,\"writable\":1}\n"), nil
		case strings.Contains(query, "FROM clusterAllReplicas"):
			reconciliationQueries++
			if reconciliationQueries == 1 {
				return dbResponse(request, http.StatusOK, ""), nil
			}
			return dbResponse(request, http.StatusOK, fmtJSONRow(replayRow)), nil
		default:
			return dbResponse(request, http.StatusOK, ""), nil
		}
	})

	if err := testWriter(client).replayOutbox(context.Background()); err != nil {
		t.Fatalf("replayOutbox() error=%v", err)
	}
	if len(bodies) != 6 {
		t.Fatalf("requests=%d, want pending, health, reconcile, target, confirm, mark", len(bodies))
	}
	if !strings.Contains(bodies[3], "INSERT INTO naver_book_raw") ||
		!strings.Contains(bodies[3], "insert_distributed_sync = 1") ||
		!strings.Contains(bodies[3], token) {
		t.Fatalf("replay did not preserve synchronous token: %s", bodies[3])
	}
	if !strings.Contains(bodies[5], "ALTER TABLE `Data_Book_NAVER_Log`.`naver_direct_insert_outbox`") ||
		!strings.Contains(bodies[5], outboxUUID) || !strings.Contains(bodies[5], "mutations_sync = 1") {
		t.Fatalf("successful replay was not durably marked: %s", bodies[5])
	}
}

func TestReplayOutboxDefersWhenLocalReplicaIsUnhealthy(t *testing.T) {
	_, rowsJSON, token, err := encodeOutboxRows("naver_book_raw", []map[string]any{{"isbn": "9780000000001"}})
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		requests++
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.Contains(string(body), "WHERE replayed_at IS NULL") {
			return dbResponse(request, http.StatusOK, fmtJSONRow(map[string]any{
				"outbox_uuid":        "01900000-0000-7000-8000-000000000003",
				"target_table":       "naver_book_raw",
				"target_local_table": "Data_Book_NAVER_Raw.naver_book_raw_local",
				"rows_json":          rowsJSON, "row_count": 1, "deduplication_token": token,
			})), nil
		}
		return dbResponse(request, http.StatusOK, "{\"replicas\":1,\"writable\":0}\n"), nil
	})

	err = testWriter(client).replayOutbox(context.Background())
	if err == nil || !strings.Contains(err.Error(), "reason=replica_unhealthy") {
		t.Fatalf("expected unhealthy replica deferral, got %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, unhealthy replay must not target or mark rows", requests)
	}
}

func TestReplayOutboxDrainsOnlyBoundedBatchThenFailsClosed(t *testing.T) {
	replayRow := map[string]any{
		"created_at": "2026-09-01 09:00:00.000", "provider": "naver",
		"isbn": "9780000000001", "version": 1,
	}
	_, rowsJSON, token, err := encodeOutboxRows("naver_book_raw", []map[string]any{replayRow})
	if err != nil {
		t.Fatal(err)
	}
	first := map[string]any{
		"outbox_uuid": "01900000-0000-7000-8000-000000000004", "target_table": "naver_book_raw",
		"target_local_table": "Data_Book_NAVER_Raw.naver_book_raw_local",
		"rows_json":          rowsJSON, "row_count": 1, "deduplication_token": token,
	}
	second := map[string]any{
		"outbox_uuid": "01900000-0000-7000-8000-000000000005", "target_table": "naver_book_raw",
		"target_local_table": "Data_Book_NAVER_Raw.naver_book_raw_local",
		"rows_json":          rowsJSON, "row_count": 1, "deduplication_token": token,
	}
	var bodies []string
	reconciliationQueries := 0
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		query := string(body)
		bodies = append(bodies, query)
		switch {
		case strings.Contains(query, "WHERE replayed_at IS NULL"):
			return dbResponse(request, http.StatusOK, fmtJSONRow(first)+fmtJSONRow(second)), nil
		case strings.Contains(query, "FROM system.replicas"):
			if !strings.Contains(query, "queue_size <= 1000") || !strings.Contains(query, "absolute_delay <= 900") {
				t.Fatalf("health gate is missing bounded replica queue/delay: %s", query)
			}
			return dbResponse(request, http.StatusOK, "{\"replicas\":1,\"writable\":1}\n"), nil
		case strings.Contains(query, "FROM clusterAllReplicas"):
			reconciliationQueries++
			if reconciliationQueries == 1 {
				return dbResponse(request, http.StatusOK, ""), nil
			}
			return dbResponse(request, http.StatusOK, fmtJSONRow(replayRow)), nil
		default:
			return dbResponse(request, http.StatusOK, ""), nil
		}
	})
	writer := testWriter(client)
	writer.Cfg.OutboxReplayLimit = 1

	err = writer.replayOutbox(context.Background())
	if err == nil || !strings.Contains(err.Error(), "reason=backlog_limit") {
		t.Fatalf("expected fail-closed bounded backlog, got %v", err)
	}
	if len(bodies) != 6 {
		t.Fatalf("requests=%d, want pending, health, reconcile, one target, confirm, one mark", len(bodies))
	}
	if !strings.Contains(bodies[5], "01900000-0000-7000-8000-000000000004") ||
		strings.Contains(bodies[5], "01900000-0000-7000-8000-000000000005") {
		t.Fatalf("bounded replay marked the wrong rows: %s", bodies[5])
	}
}

func TestLocalReplicaHealthRejectsStaleDelay(t *testing.T) {
	var query string
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		query = string(body)
		// A replica whose absolute_delay exceeds the configured predicate is
		// excluded by countIf and therefore returns writable=0.
		return dbResponse(request, http.StatusOK, "{\"replicas\":1,\"writable\":0}\n"), nil
	})
	w := testWriter(client)
	healthy, err := w.localReplicaHealthy(context.Background(), w.Cfg.RawLocalTable)
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

func TestOutboxEncodingIsDeterministicAndBounded(t *testing.T) {
	left := []map[string]any{{"isbn": "9780000000001", "version": 1}}
	right := []map[string]any{{"version": 1, "isbn": "9780000000001"}}
	_, _, leftToken, err := encodeOutboxRows("naver_book_raw", left)
	if err != nil {
		t.Fatal(err)
	}
	_, _, rightToken, err := encodeOutboxRows("naver_book_raw", right)
	if err != nil {
		t.Fatal(err)
	}
	if leftToken != rightToken {
		t.Fatalf("same logical batch produced different tokens %s %s", leftToken, rightToken)
	}
	if _, _, _, err := encodeOutboxRows("naver_book_raw", make([]map[string]any, maxOutboxRows+1)); err == nil {
		t.Fatal("oversized row batch was accepted")
	}
}

func TestReplayOutboxRejectsTamperedPayloadTokenBeforeHealthOrTargetQueries(t *testing.T) {
	row := map[string]any{
		"created_at": "2026-09-01 09:00:00.000",
		"provider":   "naver",
		"isbn":       "9780000000001",
		"version":    1,
	}
	_, rowsJSON, token, err := encodeOutboxRows("naver_book_raw", []map[string]any{row})
	if err != nil {
		t.Fatal(err)
	}
	tamperedRowsJSON := strings.Replace(rowsJSON, `"version":1`, `"version":2`, 1)
	if tamperedRowsJSON == rowsJSON {
		t.Fatal("test fixture did not change the persisted payload")
	}
	requests := 0
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		requests++
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !strings.Contains(string(body), "WHERE replayed_at IS NULL") {
			t.Fatalf("tampered outbox reached another ClickHouse operation: %s", body)
		}
		return dbResponse(request, http.StatusOK, fmtJSONRow(map[string]any{
			"outbox_uuid":         "01900000-0000-7000-8000-000000000003",
			"target_table":        "naver_book_raw",
			"target_local_table":  "Data_Book_NAVER_Raw.naver_book_raw_local",
			"rows_json":           tamperedRowsJSON,
			"row_count":           1,
			"deduplication_token": token,
		})), nil
	})
	err = testWriter(client).replayOutbox(context.Background())
	if err == nil || !strings.Contains(err.Error(), "reason=payload_rejected") {
		t.Fatalf("tampered payload error=%v, want payload_rejected", err)
	}
	if requests != 1 {
		t.Fatalf("tampered payload requests=%d, want pending-read only", requests)
	}
}

func TestReconcileOutboxRowsMapsEveryNAVERTargetToItsLogicalIdentity(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		local      string
		row        map[string]any
		queryParts []string
	}{
		{
			name: "raw", target: "naver_book_raw", local: "Data_Book_NAVER_Raw.naver_book_raw_local",
			row: map[string]any{
				"created_at": "2026-09-01 09:00:00.000", "provider": "naver",
				"isbn": "9780000000001", "version": 1,
			},
			queryParts: []string{"`created_at`", "`provider`", "`isbn`", "`version`"},
		},
		{
			name: "collect", target: "Data_Book_NAVER_Log.naver_collect_log", local: "Data_Book_NAVER_Log.naver_collect_log_local",
			row:        map[string]any{"event_uuid": "01900000-0000-7000-8000-000000000001"},
			queryParts: []string{"`event_uuid`"},
		},
		{
			name: "publisher", target: "Data_Book_NAVER_Log.aladin_publisher_cache", local: "Data_Book_NAVER_Log.aladin_publisher_cache_local",
			row: map[string]any{
				"collected_at": "2026-09-01 09:00:00.000",
				"run_uuid":     "01900000-0000-7000-8000-000000000002",
				"publisher":    "publisher",
			},
			queryParts: []string{"`collected_at`", "`run_uuid`", "`publisher`"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var query string
			client := testWriterClient(func(request *http.Request) (*http.Response, error) {
				body, readErr := io.ReadAll(request.Body)
				if readErr != nil {
					t.Fatal(readErr)
				}
				query = string(body)
				return dbResponse(request, http.StatusOK, ""), nil
			})
			result, err := testWriter(client).reconcileOutboxRows(context.Background(), test.target, test.local, []map[string]any{test.row})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.MissingRows) != 1 || !strings.Contains(query, "clusterAllReplicas('statground_cluster'") {
				t.Fatalf("result=%#v query=%s", result, query)
			}
			for _, part := range append([]string{test.local}, test.queryParts...) {
				if !strings.Contains(query, strings.TrimPrefix(part, "Data_Book_NAVER_Raw.")) && !strings.Contains(query, strings.TrimPrefix(part, "Data_Book_NAVER_Log.")) {
					t.Fatalf("query missing %q: %s", part, query)
				}
			}
		})
	}
}

func TestMarkOutboxReplayedUsesOneMutationForBoundedBatch(t *testing.T) {
	var bodies []string
	client := testWriterClient(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(body))
		return dbResponse(request, http.StatusOK, ""), nil
	})
	uuids := make([]string, 25)
	for index := range uuids {
		uuids[index] = fmt.Sprintf("01900000-0000-7000-8000-%012d", index)
	}
	if err := testWriter(client).markOutboxReplayed(context.Background(), uuids); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 {
		t.Fatalf("mark requests=%d, want one mutation", len(bodies))
	}
	if strings.Count(bodies[0], "toUUID(") != len(uuids) || !strings.Contains(bodies[0], "mutations_sync = 1") {
		t.Fatalf("bounded UUIDs were not marked by one synchronous mutation: %s", bodies[0])
	}
}

func testWriter(client *ch.Client) *Writer {
	return &Writer{Client: client, Cfg: Config{
		RawTable: "naver_book_raw", RawLocalTable: "Data_Book_NAVER_Raw.naver_book_raw_local",
		CollectLogTable: "Data_Book_NAVER_Log.naver_collect_log", CollectLogLocalTable: "Data_Book_NAVER_Log.naver_collect_log_local",
		PublisherCacheTable: "Data_Book_NAVER_Log.aladin_publisher_cache", PublisherCacheLocal: "Data_Book_NAVER_Log.aladin_publisher_cache_local",
		OutboxTable: "Data_Book_NAVER_Log.naver_direct_insert_outbox", OutboxReplayLimit: 25, OutboxMaxReplicaQueue: 1000, OutboxMaxReplicaDelaySeconds: 900,
	}}
}

func testWriterClient(roundTrip func(*http.Request) (*http.Response, error)) *ch.Client {
	return &ch.Client{
		Host: "http://clickhouse.test", Database: "Data_Book_NAVER_Raw",
		HTTPClient: &http.Client{Transport: dbRoundTripFunc(roundTrip), Timeout: time.Second},
	}
}

func dbResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

func fmtJSONRow(row map[string]any) string {
	payload, err := json.Marshal(row)
	if err != nil {
		panic(err)
	}
	return string(payload) + "\n"
}

func TestRetryableWriterErrorRecognizesNetworkAndClickHouseCapacity(t *testing.T) {
	if !retryableWriterError(&net.DNSError{IsTimeout: true}) {
		t.Fatal("network timeout was not classified transient")
	}
	if !retryableWriterError(errors.New("clickhouse http status=500 code=241")) {
		t.Fatal("ClickHouse memory/capacity code was not classified transient")
	}
	for _, code := range []int{286, 364, 574, 692, 733, 745, 749, 762} {
		if !retryableWriterError(fmt.Errorf("clickhouse http status=400 code=%d", code)) {
			t.Errorf("ClickHouse 26.1 transient code=%d was not classified transient", code)
		}
	}
	if retryableWriterError(errors.New("clickhouse http status=403 code=497")) {
		t.Fatal("permission failure was classified transient")
	}
}

func TestIsDurabilityErrorRecognizesWrappedWriterFailure(t *testing.T) {
	err := fmt.Errorf("publisher collection: %w", &operationError{
		operation: "enqueue_outbox",
		category:  "clickhouse_transient",
		reason:    "server_unavailable",
	})
	if !IsDurabilityError(err) {
		t.Fatal("wrapped target-plus-outbox failure was not classified as a durability error")
	}
	if IsDurabilityError(errors.New("naver api http 503")) {
		t.Fatal("provider API error was classified as a durability error")
	}
}
