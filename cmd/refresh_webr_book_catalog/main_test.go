package main

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

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
