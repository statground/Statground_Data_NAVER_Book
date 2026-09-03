package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
)

type refreshRoundTripFunc func(*http.Request) (*http.Response, error)

func (function refreshRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestTriggerRefreshWithRetryFindsSingleCoordinator(t *testing.T) {
	var (
		mu     sync.Mutex
		bodies []string
	)
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body := string(payload)
			mu.Lock()
			bodies = append(bodies, body)
			call := len(bodies)
			mu.Unlock()

			responseBody := ""
			switch call {
			case 1:
				if !strings.Contains(body, "EXISTS TABLE `Data_Book_Service`.`mv_book_catalog_latest_refresh`") ||
					strings.Contains(body, "system.tables") {
					t.Fatalf("unexpected coordinator probe body=%q", body)
				}
				responseBody = "{\"result\":0}\n"
			case 2:
				if !strings.Contains(body, "EXISTS TABLE `Data_Book_Service`.`mv_book_catalog_latest_refresh`") ||
					strings.Contains(body, "system.tables") {
					t.Fatalf("unexpected coordinator probe body=%q", body)
				}
				responseBody = "{\"result\":1}\n"
			case 3:
				if !strings.Contains(body, "FROM system.view_refreshes") {
					t.Fatalf("unexpected refresh admission probe body=%q", body)
				}
				responseBody = "{\"value\":0}\n"
			case 4:
				if body != "SYSTEM REFRESH VIEW Data_Book_Service.mv_book_catalog_latest_refresh" {
					t.Fatalf("unexpected refresh body=%q", body)
				}
			case 5:
				if body != "SYSTEM WAIT VIEW Data_Book_Service.mv_book_catalog_latest_refresh" {
					t.Fatalf("unexpected wait body=%q", body)
				}
			default:
				t.Fatalf("unexpected call %d body=%q", call, body)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}

	if err := triggerRefreshWithRetry(client, "Data_Book_Service.mv_book_catalog_latest_refresh", 3, 0); err != nil {
		t.Fatalf("triggerRefreshWithRetry() error=%v", err)
	}
	if len(bodies) != 5 {
		t.Fatalf("request count=%d, want 5", len(bodies))
	}
}

func TestTriggerRefreshWithRetryFailsWhenCoordinatorIsUnavailable(t *testing.T) {
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body := string(payload)
			if !strings.Contains(body, "EXISTS TABLE `webr_book`.`mv_naver_r_book_catalog_refresh`") ||
				strings.Contains(body, "system.tables") {
				t.Fatalf("unexpected coordinator probe body=%q", body)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("{\"result\":0}\n")),
				Request:    request,
			}, nil
		})},
	}

	err := triggerRefreshWithRetry(client, "webr_book.mv_naver_r_book_catalog_refresh", 2, 0)
	if err == nil || err.Error() != "refresh coordinator unavailable" {
		t.Fatalf("unexpected error=%v", err)
	}
}

func TestTriggerRefreshWithRetrySkipsWhenBookRefreshIsRunning(t *testing.T) {
	var bodies []string
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body := string(payload)
			bodies = append(bodies, body)

			responseBody := ""
			switch len(bodies) {
			case 1:
				if !strings.Contains(body, "EXISTS TABLE `Data_Book_Service`.`mv_book_catalog_latest_refresh`") {
					t.Fatalf("unexpected coordinator probe body=%q", body)
				}
				responseBody = "{\"result\":1}\n"
			case 2:
				if !strings.Contains(body, "FROM system.view_refreshes") {
					t.Fatalf("unexpected refresh admission probe body=%q", body)
				}
				responseBody = "{\"value\":1}\n"
			default:
				t.Fatalf("unexpected call %d body=%q", len(bodies), body)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}

	err := triggerRefreshWithRetry(client, "Data_Book_Service.mv_book_catalog_latest_refresh", 1, 0)
	if !errors.Is(err, errBookRefreshBusy) {
		t.Fatalf("triggerRefreshWithRetry() error=%v, want errBookRefreshBusy", err)
	}
	for _, body := range bodies {
		if strings.HasPrefix(body, "SYSTEM REFRESH VIEW") {
			t.Fatalf("busy guard issued refresh body=%q", body)
		}
	}
}

func TestRestoreBookRefreshSchedulesStartsEveryExactViewInDependencyOrder(t *testing.T) {
	var bodies []string
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body := string(payload)
			bodies = append(bodies, body)

			responseBody := ""
			switch {
			case strings.HasPrefix(body, "EXISTS TABLE "):
				responseBody = "{\"result\":1}\n"
			case strings.HasPrefix(body, "SYSTEM START VIEW "):
			case strings.Contains(body, "FROM system.view_refreshes"):
				responseBody = "{\"value\":\"Scheduled\"}\n"
			default:
				t.Fatalf("expected exact schedule restore, body=%q", body)
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}

	if err := restoreBookRefreshSchedulesWithRetry(client, bookRefreshScheduleViews, 1, 0); err != nil {
		t.Fatalf("restoreBookRefreshSchedulesWithRetry() error=%v", err)
	}
	if len(bodies) != len(bookRefreshScheduleViews)*3 {
		t.Fatalf("request count=%d, want %d", len(bodies), len(bookRefreshScheduleViews)*3)
	}
	for index, refreshView := range bookRefreshScheduleViews {
		probe := bodies[index*3]
		start := bodies[index*3+1]
		status := bodies[index*3+2]
		database, view, _ := strings.Cut(refreshView, ".")
		if !strings.Contains(probe, "EXISTS TABLE `"+database+"`.`"+view+"`") {
			t.Fatalf("probe[%d]=%q, want view=%s", index, probe, refreshView)
		}
		if start != "SYSTEM START VIEW "+refreshView {
			t.Fatalf("start[%d]=%q, want view=%s", index, start, refreshView)
		}
		if !strings.Contains(status, "FROM system.view_refreshes") ||
			!strings.Contains(status, "SELECT status AS value") ||
			!strings.Contains(status, "database = '"+database+"'") ||
			!strings.Contains(status, "view = '"+view+"'") ||
			!strings.Contains(status, "LIMIT 1") {
			t.Fatalf("status[%d]=%q, want exact active-state lookup for view=%s", index, status, refreshView)
		}
	}
}

func TestRestoreBookRefreshSchedulesContinuesAfterOneCoordinatorFailure(t *testing.T) {
	var bodies []string
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body := string(payload)
			bodies = append(bodies, body)

			responseBody := ""
			if strings.HasPrefix(body, "EXISTS TABLE ") {
				if strings.Contains(body, "`webr_book`.`mv_naver_r_book_catalog_refresh`") {
					responseBody = "{\"result\":0}\n"
				} else {
					responseBody = "{\"result\":1}\n"
				}
			} else if strings.Contains(body, "FROM system.view_refreshes") {
				responseBody = "{\"value\":\"Scheduled\"}\n"
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}

	err := restoreBookRefreshSchedulesWithRetry(client, bookRefreshScheduleViews, 1, 0)
	if err == nil || !strings.Contains(err.Error(), "webr_book.mv_naver_r_book_catalog_refresh") {
		t.Fatalf("unexpected restore error=%v", err)
	}
	lastView := bookRefreshScheduleViews[len(bookRefreshScheduleViews)-1]
	foundLastStart := false
	for _, body := range bodies {
		if body == "SYSTEM START VIEW "+lastView {
			foundLastStart = true
		}
	}
	if !foundLastStart {
		t.Fatalf("restore stopped early, bodies=%q", bodies)
	}
}

func TestRestoreBookRefreshSchedulesRejectsHTTPStartWithoutActiveState(t *testing.T) {
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body := string(payload)
			responseBody := ""
			switch {
			case strings.HasPrefix(body, "EXISTS TABLE "):
				responseBody = "{\"result\":1}\n"
			case strings.HasPrefix(body, "SYSTEM START VIEW "):
			case strings.Contains(body, "FROM system.view_refreshes"):
				responseBody = "{\"value\":0}\n"
			default:
				t.Fatalf("unexpected request body=%q", body)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}

	err := restoreBookRefreshSchedulesWithRetry(client, bookRefreshScheduleViews[:1], 1, 0)
	if err == nil || !strings.Contains(err.Error(), "did not reach an active state") {
		t.Fatalf("unexpected restore error=%v", err)
	}
}

func TestRestoreBookRefreshSchedulesAcceptsKnownActiveStates(t *testing.T) {
	for _, status := range []string{"Scheduled", "Scheduling", "Running", "RunningOnAnotherReplica", "WaitingForDependencies"} {
		t.Run(status, func(t *testing.T) {
			client := &ch.Client{
				Host: "http://clickhouse.test",
				HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					payload, err := io.ReadAll(request.Body)
					if err != nil {
						t.Fatal(err)
					}
					body := string(payload)
					responseBody := ""
					switch {
					case strings.HasPrefix(body, "EXISTS TABLE "):
						responseBody = "{\"result\":1}\n"
					case strings.HasPrefix(body, "SYSTEM START VIEW "):
					case strings.Contains(body, "FROM system.view_refreshes"):
						responseBody = "{\"value\":\"" + status + "\"}\n"
					default:
						t.Fatalf("unexpected request body=%q", body)
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader(responseBody)),
						Request:    request,
					}, nil
				})},
			}

			if err := restoreBookRefreshSchedulesWithRetry(client, bookRefreshScheduleViews[:1], 1, 0); err != nil {
				t.Fatalf("restoreBookRefreshSchedulesWithRetry() status=%s error=%v", status, err)
			}
		})
	}
}

func TestRestoreBookRefreshSchedulesUsesIndependentPerViewTimeouts(t *testing.T) {
	var bodies []string
	firstView := bookRefreshScheduleViews[0]
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body := string(payload)
			bodies = append(bodies, body)

			responseBody := ""
			switch {
			case strings.HasPrefix(body, "EXISTS TABLE "):
				responseBody = "{\"result\":1}\n"
			case strings.HasPrefix(body, "SYSTEM START VIEW "):
			case strings.Contains(body, "FROM system.view_refreshes") && strings.Contains(body, "view = 'mv_book_catalog_latest_refresh'"):
				<-request.Context().Done()
				return nil, request.Context().Err()
			case strings.Contains(body, "FROM system.view_refreshes"):
				responseBody = "{\"value\":\"Scheduled\"}\n"
			default:
				t.Fatalf("unexpected request body=%q", body)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}

	err := restoreBookRefreshSchedulesWithRetryTimeout(client, bookRefreshScheduleViews, 1, 0, 100*time.Millisecond)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), firstView) {
		t.Fatalf("unexpected restore error=%v", err)
	}
	for _, refreshView := range bookRefreshScheduleViews {
		found := false
		for _, body := range bodies {
			if body == "SYSTEM START VIEW "+refreshView {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("view did not receive START after prior timeout: view=%s bodies=%q", refreshView, bodies)
		}
	}
}

func TestRunBusyPathRestoresAllSchedules(t *testing.T) {
	var (
		mu     sync.Mutex
		bodies []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			http.Error(writer, "read request body", http.StatusInternalServerError)
			return
		}
		body := string(payload)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()

		switch {
		case strings.HasPrefix(body, "EXISTS TABLE "):
			_, _ = writer.Write([]byte("{\"result\":1}\n"))
		case strings.HasPrefix(body, "SYSTEM START VIEW "):
		case strings.Contains(body, "FROM system.view_refreshes") && strings.Contains(body, "AND ("):
			_, _ = writer.Write([]byte("{\"value\":1}\n"))
		case strings.Contains(body, "FROM system.view_refreshes"):
			_, _ = writer.Write([]byte("{\"value\":\"Running\"}\n"))
		default:
			t.Errorf("unexpected request body=%q", body)
			http.Error(writer, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	t.Setenv("CH_HOST", server.URL)
	t.Setenv("CH_PORT", "8123")
	t.Setenv("CH_USER", "test")
	t.Setenv("CH_PASSWORD", "test")
	t.Setenv("CH_DATABASE", "Data_Book_Service")
	t.Setenv("CH_PROTOCOL", "http")
	t.Setenv("CH_HTTP_URL_PATH", "")
	t.Setenv("CLICKHOUSE_HTTP_URL_PATH", "")
	t.Setenv("WEBR_BOOK_REFRESH_SETTLE_SECONDS", "0")
	t.Setenv("BOOK_REFRESH_COORDINATOR_ATTEMPTS", "1")
	t.Setenv("BOOK_REFRESH_COORDINATOR_RETRY_MILLISECONDS", "0")
	t.Setenv("BOOK_REFRESH_SCHEDULE_RESTORE_VIEW_TIMEOUT_SECONDS", "1")

	if err := run(); err != nil {
		t.Fatalf("run() busy path error=%v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, body := range bodies {
		if strings.HasPrefix(body, "SYSTEM REFRESH VIEW ") {
			t.Fatalf("busy path issued refresh body=%q", body)
		}
	}
	for _, refreshView := range bookRefreshScheduleViews {
		found := false
		for _, body := range bodies {
			if body == "SYSTEM START VIEW "+refreshView {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("busy path did not restore view=%s bodies=%q", refreshView, bodies)
		}
	}
}

func TestTriggerRefreshWithRetryDoesNotRefreshWhenAdmissionQueryFails(t *testing.T) {
	var bodies []string
	client := &ch.Client{
		Host: "http://clickhouse.test",
		HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			body := string(payload)
			bodies = append(bodies, body)
			status := http.StatusOK
			responseBody := "{\"result\":1}\n"
			if strings.Contains(body, "FROM system.view_refreshes") {
				status = http.StatusInternalServerError
				responseBody = "Code: 497. DB::Exception: Not enough privileges"
			}
			return &http.Response{
				StatusCode: status,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	}

	if err := triggerRefreshWithRetry(client, bookRefreshScheduleViews[0], 1, 0); err == nil {
		t.Fatal("admission query failure must fail closed")
	}
	for _, body := range bodies {
		if strings.HasPrefix(body, "SYSTEM REFRESH VIEW") {
			t.Fatalf("admission failure issued refresh body=%q", body)
		}
	}
}
