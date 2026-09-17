package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"statground_naver_book_go/internal/ch"
)

func scheduleRows(statuses []string) string {
	var rows strings.Builder
	for index, status := range statuses {
		database, view, _ := strings.Cut(bookRefreshScheduleViews[index], ".")
		row, _ := json.Marshal(map[string]string{"database": database, "view": view, "status": status})
		rows.Write(row)
		rows.WriteByte('\n')
	}
	return rows.String()
}

func TestVerifyBookSchedulesChecksEntireChainReadOnly(t *testing.T) {
	for _, test := range []struct {
		name     string
		statuses []string
		wantErr  string
	}{
		{"healthy waiting successor", []string{"Scheduled", "Running", "Scheduled", "WaitingForDependencies"}, ""},
		{"disabled predecessor", []string{"Scheduled", "Scheduled", "Disabled", "WaitingForDependencies"}, "mirtype_book.mv_naver_language_book_catalog_refresh status=Disabled"},
		{"missing dependency", []string{"Scheduled", "Scheduled", "MissingDependencies", "WaitingForDependencies"}, "status=MissingDependencies"},
		{"missing view", []string{"Scheduled", "Scheduled", "Scheduled"}, "Book refresh schedule missing"},
		{"unknown state", []string{"Scheduled", "Scheduled", "Unknown", "WaitingForDependencies"}, "status=Unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client := &ch.Client{Host: "http://clickhouse.test", HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				payload, _ := io.ReadAll(request.Body)
				body := string(payload)
				if !strings.HasPrefix(strings.TrimSpace(body), "SELECT database, view, status") || !strings.Contains(body, "FROM system.view_refreshes") || strings.Contains(body, "SYSTEM ") || strings.Contains(body, "system.tables") {
					t.Fatalf("verification was not an exact read-only schedule query: %q", body)
				}
				for _, name := range bookRefreshScheduleViews {
					database, view, _ := strings.Cut(name, ".")
					if !strings.Contains(body, "database = '"+database+"' AND view = '"+view+"'") {
						t.Fatalf("schedule query omitted %s: %q", name, body)
					}
				}
				if !strings.Contains(body, "LIMIT 5") || !strings.Contains(body, "max_execution_time = 5") {
					t.Fatalf("schedule query is not bounded: %q", body)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(scheduleRows(test.statuses))), Request: request}, nil
			})}}
			err := verifyBookRefreshSchedules(context.Background(), client, bookRefreshScheduleViews, 3, 0)
			if test.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("error=%v, want %q", err, test.wantErr)
			}
			if calls != 1 {
				t.Fatalf("unhealthy schedule was retried on another endpoint: calls=%d", calls)
			}
		})
	}
}

func TestVerifyBookSchedulesRetriesEmptyCoordinatorOnly(t *testing.T) {
	for _, responseStatus := range []int{http.StatusOK, http.StatusForbidden} {
		calls := 0
		client := &ch.Client{Host: "http://clickhouse.test", HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			rows := ""
			if calls > 1 {
				rows = scheduleRows([]string{"Scheduled", "Scheduled", "Scheduled", "WaitingForDependencies"})
			}
			return &http.Response{StatusCode: responseStatus, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(rows)), Request: request}, nil
		})}}
		err := verifyBookRefreshSchedules(context.Background(), client, bookRefreshScheduleViews, 3, 0)
		if responseStatus == http.StatusOK && (err != nil || calls != 2) {
			t.Fatalf("coordinator discovery: error=%v calls=%d", err, calls)
		}
		if responseStatus == http.StatusForbidden && (err == nil || calls != 1) {
			t.Fatalf("permission failure was not immediate: error=%v calls=%d", err, calls)
		}
	}
}

func TestRunVerifyOnlyNeverStartsOrRefreshesViews(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		body, _ := io.ReadAll(request.Body)
		if !strings.HasPrefix(strings.TrimSpace(string(body)), "SELECT database, view, status") {
			t.Errorf("verify-only issued a mutation or unrelated query: %q", body)
		}
		_, _ = io.WriteString(writer, scheduleRows([]string{"Scheduled", "Scheduled", "Disabled", "WaitingForDependencies"}))
	}))
	defer server.Close()
	for key, value := range map[string]string{
		"CH_HOST": server.URL, "CH_PORT": "1", "CH_USER": "test", "CH_PASSWORD": "test", "CH_DATABASE": "Data_Book_Service", "CH_PROTOCOL": "http", "CH_HTTP_URL_PATH": "",
		"BOOK_REFRESH_VERIFY_ONLY": "true", "WEBR_BOOK_REFRESH_SETTLE_SECONDS": "900",
	} {
		t.Setenv(key, value)
	}
	err := run()
	if err == nil || !strings.Contains(err.Error(), "status=Disabled") || calls != 1 {
		t.Fatalf("verify-only result: error=%v calls=%d", err, calls)
	}
}
