package dbingest

import (
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
)

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
