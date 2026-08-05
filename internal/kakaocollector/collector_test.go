package kakaocollector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"statground_naver_book_go/internal/kakaostore"
	"statground_naver_book_go/internal/provider"
	"statground_naver_book_go/internal/provider/kakao"
	"statground_naver_book_go/internal/quota"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type fakeStore struct {
	mu sync.Mutex

	frontier      kakaostore.FrontierSnapshot
	existing      map[string]string
	callLogs      []kakaostore.CallLog
	rawRows       []map[string]any
	collectLogs   []kakaostore.CollectLog
	frontierLogs  []kakaostore.FrontierRecord
	failCallStart bool
}

func (s *fakeStore) Validate(context.Context) error { return nil }
func (s *fakeStore) ObservedCallsToday(context.Context, time.Time) (int, error) {
	return 0, nil
}
func (s *fakeStore) LatestQuotaStop(context.Context) (kakaostore.QuotaStop, error) {
	return kakaostore.QuotaStop{}, nil
}
func (s *fakeStore) LoadFrontier(context.Context, kakaostore.FrontierKey) (kakaostore.FrontierSnapshot, error) {
	return s.frontier, nil
}
func (s *fakeStore) ExistingContentHashes(context.Context, []string) (map[string]string, error) {
	out := make(map[string]string, len(s.existing))
	for key, value := range s.existing {
		out[key] = value
	}
	return out, nil
}
func (s *fakeStore) InsertCallLog(_ context.Context, record kakaostore.CallLog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failCallStart && record.Status == "reserved" {
		return &kakaostore.StoreError{Operation: "insert_call_log", Category: "clickhouse_transient"}
	}
	s.callLogs = append(s.callLogs, record)
	return nil
}
func (s *fakeStore) InsertRawRows(_ context.Context, rows []map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rawRows = append(s.rawRows, rows...)
	return nil
}
func (s *fakeStore) InsertCollectLog(_ context.Context, record kakaostore.CollectLog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collectLogs = append(s.collectLogs, record)
	return nil
}
func (s *fakeStore) InsertFrontier(_ context.Context, record kakaostore.FrontierRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frontierLogs = append(s.frontierLogs, record)
	return nil
}

func TestCollectBoundsPagesPersistsEvidenceAndApplicationIDs(t *testing.T) {
	store := &fakeStore{existing: map[string]string{}}
	httpCalls := 0
	client := testKakaoClient(t, 1, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		httpCalls++
		switch request.URL.Query().Get("page") {
		case "1":
			return jsonResponse(http.StatusOK, `{
			  "meta":{"total_count":2,"pageable_count":2,"is_end":false},
			  "documents":[{"title":"First","isbn":"9780306406157","authors":["A"],"price":1000,"sale_price":900}]
			}`), nil
		case "2":
			return jsonResponse(http.StatusOK, `{
			  "meta":{"total_count":2,"pageable_count":2,"is_end":true},
			  "documents":[{"title":"Second","isbn":"9780131103627","authors":["B"],"price":2000,"sale_price":1800}]
			}`), nil
		default:
			t.Fatalf("unexpected page %q", request.URL.Query().Get("page"))
			return nil, nil
		}
	}))
	collector := testCollector(t, client, store)

	result, err := collector.Collect(context.Background(), Config{
		Mode:     "fixed_keyword",
		Request:  provider.SearchRequest{Query: "statistics", Sort: "latest", Page: 1, Size: 1},
		PageCap:  5,
		Priority: 0.8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if httpCalls != 2 || result.Calls != 2 || result.Fetched != 2 || result.Inserted != 2 || result.NewISBN != 2 {
		t.Fatalf("unexpected result=%+v http_calls=%d", result, httpCalls)
	}
	if len(store.callLogs) != 4 {
		t.Fatalf("call log rows=%d, want pre/completion for two attempts", len(store.callLogs))
	}
	if len(store.rawRows) != 2 {
		t.Fatalf("raw rows=%d", len(store.rawRows))
	}
	for _, row := range store.rawRows {
		if row["run_uuid"] == "" || row["request_uuid"] == "" || row["event_uuid"] == "" ||
			row["search_mode"] != "fixed_keyword" || row["search_query"] != "statistics" ||
			row["canonical_isbn"] == "" || row["content_hash"] == "" {
			t.Fatalf("raw evidence incomplete: %#v", row)
		}
	}
	if len(store.collectLogs) != 1 || store.collectLogs[0].PagesCalled != 2 ||
		len(store.frontierLogs) != 1 || store.frontierLogs[0].CallsLastRun != 2 {
		t.Fatalf("collect/frontier logs incomplete: collect=%+v frontier=%+v", store.collectLogs, store.frontierLogs)
	}
}

func TestCollectLogsEveryRetryAttempt(t *testing.T) {
	store := &fakeStore{existing: map[string]string{}}
	httpCalls := 0
	client := testKakaoClient(t, 2, roundTripFunc(func(*http.Request) (*http.Response, error) {
		httpCalls++
		if httpCalls == 1 {
			return jsonResponse(http.StatusServiceUnavailable, `{"msg":"temporary"}`), nil
		}
		return jsonResponse(http.StatusOK, `{
		  "meta":{"total_count":0,"pageable_count":0,"is_end":true},
		  "documents":[]
		}`), nil
	}))
	collector := testCollector(t, client, store)

	result, err := collector.Collect(context.Background(), Config{
		Mode:    "manual",
		Request: provider.SearchRequest{Query: "book"},
		PageCap: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if httpCalls != 2 || result.Calls != 2 || len(store.callLogs) != 4 {
		t.Fatalf("retry accounting mismatch calls=%d result=%+v logs=%d", httpCalls, result, len(store.callLogs))
	}
	if store.callLogs[1].ErrorCategory != kakao.ErrorUnavailable || !store.callLogs[3].Success {
		t.Fatalf("retry call log categories=%+v", store.callLogs)
	}
}

func TestCollectRateLimitStopsRunAndPersistsHoldEvidence(t *testing.T) {
	store := &fakeStore{existing: map[string]string{}}
	httpCalls := 0
	client := testKakaoClient(t, 3, roundTripFunc(func(*http.Request) (*http.Response, error) {
		httpCalls++
		return jsonResponse(http.StatusTooManyRequests, `{"code":-10,"msg":"limit"}`), nil
	}))
	collector := testCollector(t, client, store)

	result, err := collector.Collect(context.Background(), Config{
		Mode:    "fixed_keyword",
		Request: provider.SearchRequest{Query: "book"},
		PageCap: 4,
	})
	if err == nil || result.ErrorCategory != kakao.ErrorQuotaExhausted {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if httpCalls != 1 || result.Calls != 1 || !collector.Budget.IsExhausted() {
		t.Fatalf("quota stop did not stop immediately: calls=%d result=%+v", httpCalls, result)
	}
	if len(store.callLogs) != 2 || store.callLogs[1].ErrorCategory != kakao.ErrorQuotaExhausted {
		t.Fatalf("quota call log missing: %+v", store.callLogs)
	}
	if len(store.collectLogs) != 1 || store.collectLogs[0].Status != kakao.ErrorQuotaExhausted {
		t.Fatalf("quota collect log missing: %+v", store.collectLogs)
	}
}

func TestCallLogFailurePreventsExternalRequest(t *testing.T) {
	store := &fakeStore{existing: map[string]string{}, failCallStart: true}
	httpCalls := 0
	client := testKakaoClient(t, 1, roundTripFunc(func(*http.Request) (*http.Response, error) {
		httpCalls++
		return jsonResponse(http.StatusOK, `{"meta":{"is_end":true},"documents":[]}`), nil
	}))
	collector := testCollector(t, client, store)

	result, err := collector.Collect(context.Background(), Config{
		Mode:    "manual",
		Request: provider.SearchRequest{Query: "book"},
		PageCap: 1,
	})
	if err == nil || result.ErrorCategory != "clickhouse_transient" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if httpCalls != 0 {
		t.Fatalf("external calls=%d, want 0 after pre-call log failure", httpCalls)
	}
}

func TestRespectFrontierDueSkipsWithoutExternalRequest(t *testing.T) {
	store := &fakeStore{
		existing: map[string]string{},
		frontier: kakaostore.FrontierSnapshot{
			Found:     true,
			State:     quota.FrontierState{Active: true},
			NextDueAt: time.Date(2026, 7, 27, 12, 0, 0, 0, time.FixedZone("KST", 9*60*60)),
		},
	}
	httpCalls := 0
	client := testKakaoClient(t, 1, roundTripFunc(func(*http.Request) (*http.Response, error) {
		httpCalls++
		return jsonResponse(http.StatusOK, `{"meta":{"is_end":true},"documents":[]}`), nil
	}))
	collector := testCollector(t, client, store)

	result, err := collector.Collect(context.Background(), Config{
		Mode:       "fixed_keyword",
		Request:    provider.SearchRequest{Query: "book"},
		PageCap:    1,
		RespectDue: true,
	})
	if err != nil || !result.SkippedDue {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if httpCalls != 0 || len(store.callLogs) != 0 {
		t.Fatalf("not-due frontier made calls=%d logs=%d", httpCalls, len(store.callLogs))
	}
}

func TestErrorStageUsesClosedStoreOperationAllowlist(t *testing.T) {
	for _, stage := range []string{
		"preflight_connection",
		"preflight_table",
		"preflight_grant",
		"observed_calls",
		"latest_quota_stop",
		"load_frontier",
		"insert_call_log",
		"complete_call_log",
		"existing_hashes",
		"insert_raw",
		"insert_collect_log",
		"insert_frontier",
	} {
		err := &kakaostore.StoreError{Operation: stage, Category: "clickhouse_contract"}
		if got := ErrorStage(err); got != stage {
			t.Fatalf("ErrorStage(%q)=%q", stage, got)
		}
	}

	for _, err := range []error{
		fmt.Errorf("raw endpoint and SQL must not escape"),
		&kakaostore.StoreError{Operation: "SELECT secret FROM internal", Category: "clickhouse_contract"},
	} {
		if got := ErrorStage(err); got != "" {
			t.Fatalf("unsafe error stage escaped as %q", got)
		}
	}
}

func testCollector(t *testing.T, client *kakao.Client, store *fakeStore) *Collector {
	t.Helper()
	config := quota.DefaultConfig()
	config.MaxRequestsPerRun = 100
	budget, err := quota.NewBudget(config, 0)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := New(client, store, budget, "01900000-0000-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	fixedNow := time.Date(2026, 7, 26, 12, 0, 0, 0, time.FixedZone("KST", 9*60*60))
	collector.Now = func() time.Time { return fixedNow }
	counter := 1
	collector.NewUUID = func() string {
		value := fmt.Sprintf("01900000-0000-7000-8000-%012d", counter)
		counter++
		return value
	}
	return collector
}

func testKakaoClient(t *testing.T, attempts int, transport http.RoundTripper) *kakao.Client {
	t.Helper()
	client, err := kakao.NewClient(kakao.Config{
		APIKey:     "unit-key",
		HTTPClient: &http.Client{Transport: transport},
		Attempts:   attempts,
		Timeout:    time.Second,
		BackoffMin: time.Millisecond,
		BackoffMax: time.Millisecond,
		Sleep: func(context.Context, time.Duration) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
