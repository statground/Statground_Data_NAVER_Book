package main

import (
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
				if body != "SYSTEM REFRESH VIEW Data_Book_Service.mv_book_catalog_latest_refresh" {
					t.Fatalf("unexpected refresh body=%q", body)
				}
			case 4:
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
	if len(bodies) != 4 {
		t.Fatalf("request count=%d, want 4", len(bodies))
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
