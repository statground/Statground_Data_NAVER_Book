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
		row, _ := json.Marshal(map[string]any{"database": database, "view": view, "status": status, "observed_epoch": 2000000000, "last_success_epoch": 1999999999, "last_refresh_epoch": 1999999999, "has_exception": 0})
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
				for _, metadata := range []string{"toUnixTimestamp(now()) AS observed_epoch", "last_success_time", "last_refresh_time", "notEmpty(exception) AS has_exception"} {
					if !strings.Contains(body, metadata) {
						t.Fatalf("schedule query omitted freshness metadata %q", metadata)
					}
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

func TestVerifyBookSchedulesBoundedProgressAndFreshness(t *testing.T) {
	const now = int64(2000000000)
	for _, test := range []struct {
		name    string
		modify  func([]map[string]any)
		wantErr string
	}{
		{"success at 13 hour boundary", func(rows []map[string]any) { rows[2]["last_success_epoch"] = now - 46800 }, ""},
		{"stale scheduled success", func(rows []map[string]any) { rows[2]["last_success_epoch"] = now - 46801 }, "reason=success_stale"},
		{"missing scheduled success", func(rows []map[string]any) { rows[2]["last_success_epoch"] = 0 }, "reason=success_missing"},
		{"future scheduled success", func(rows []map[string]any) { rows[2]["last_success_epoch"] = now + 1 }, "reason=success_future"},
		{"latest scheduled failure", func(rows []map[string]any) { rows[2]["has_exception"] = 1 }, "reason=latest_failure"},
		{"latest failure takes precedence over stale success", func(rows []map[string]any) {
			rows[2]["has_exception"] = 1
			rows[2]["last_success_epoch"] = now - 46801
		}, "reason=latest_failure"},
		{"stale scheduling success", func(rows []map[string]any) {
			rows[2]["status"] = "Scheduling"
			rows[2]["last_success_epoch"] = now - 46801
		}, "reason=success_stale"},
		{"first running generation with prior failure and waiting successor", func(rows []map[string]any) {
			rows[2]["status"] = "Running"
			rows[2]["last_refresh_epoch"] = now - 7200
			rows[2]["last_success_epoch"] = 0
			rows[2]["has_exception"] = 1
			rows[3]["status"] = "WaitingForDependencies"
			rows[3]["last_success_epoch"] = 0
			rows[3]["has_exception"] = 1
		}, ""},
		{"running on another replica with prior failure", func(rows []map[string]any) {
			rows[2]["status"] = "RunningOnAnotherReplica"
			rows[2]["last_success_epoch"] = 0
			rows[2]["has_exception"] = 1
		}, ""},
		{"running generation exceeds 2 hours", func(rows []map[string]any) {
			rows[2]["status"] = "Running"
			rows[2]["last_refresh_epoch"] = now - 7201
		}, "exceeds 2 hours"},
		{"running generation missing start", func(rows []map[string]any) {
			rows[2]["status"] = "Running"
			rows[2]["last_refresh_epoch"] = 0
		}, "invalid start"},
		{"running generation future start", func(rows []map[string]any) {
			rows[2]["status"] = "Running"
			rows[2]["last_refresh_epoch"] = now + 1
		}, "invalid start"},
		{"waiting without active predecessor or success", func(rows []map[string]any) {
			rows[3]["status"] = "WaitingForDependencies"
			rows[3]["last_success_epoch"] = 0
		}, "reason=success_missing"},
		{"waiting with fresh own success", func(rows []map[string]any) { rows[3]["status"] = "WaitingForDependencies" }, ""},
		{"waiting with stale own success", func(rows []map[string]any) {
			rows[3]["status"] = "WaitingForDependencies"
			rows[3]["last_success_epoch"] = now - 46801
		}, "reason=success_stale"},
		{"waiting with latest failure and no active predecessor", func(rows []map[string]any) {
			rows[3]["status"] = "WaitingForDependencies"
			rows[3]["has_exception"] = 1
		}, "reason=latest_failure"},
		{"waiting chain backed by first running predecessor", func(rows []map[string]any) {
			rows[0]["status"] = "Running"
			rows[0]["last_success_epoch"] = 0
			for index := 1; index < len(rows); index++ {
				rows[index]["status"] = "WaitingForDependencies"
				rows[index]["last_success_epoch"] = 0
			}
		}, ""},
		{"stale predecessor fails whole waiting chain", func(rows []map[string]any) {
			rows[0]["status"] = "Running"
			rows[1]["last_success_epoch"] = now - 46801
			rows[3]["status"] = "WaitingForDependencies"
			rows[3]["last_success_epoch"] = 0
		}, "webr_book.mv_naver_r_book_catalog_refresh"},
		{"missing server clock", func(rows []map[string]any) { delete(rows[2], "observed_epoch") }, "metadata incomplete"},
		{"missing exception metadata", func(rows []map[string]any) { delete(rows[2], "has_exception") }, "metadata incomplete"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var rows []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(scheduleRows([]string{"Scheduled", "Scheduled", "Scheduled", "Scheduled"})), "\n") {
				var row map[string]any
				if err := json.Unmarshal([]byte(line), &row); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, row)
			}
			test.modify(rows)
			var payload strings.Builder
			for _, row := range rows {
				line, _ := json.Marshal(row)
				payload.Write(line)
				payload.WriteByte('\n')
			}
			client := &ch.Client{Host: "http://clickhouse.test", HTTPClient: &http.Client{Transport: refreshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload.String())), Request: request}, nil
			})}}
			err := verifyBookRefreshSchedules(context.Background(), client, bookRefreshScheduleViews, 1, 0)
			if test.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("error=%v, want %q", err, test.wantErr)
			}
		})
	}
}
