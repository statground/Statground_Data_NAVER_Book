package nlkstore

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"statground_naver_book_go/internal/ch"
)

func TestImportAndBackfillRejectDeniedGrantWithHTTP200(t *testing.T) {
	for _, backfill := range []bool{false, true} {
		client := &ch.Client{Host: "database.invalid", Protocol: "https", HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(r.Body)
			response := "{\"result\":1}\n"
			if strings.HasPrefix(string(body), "CHECK GRANT") {
				response = "0\n"
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response)), Header: make(http.Header), Request: r}, nil
		})}}
		store, err := NewClickHouse(client, ConfigFromEnv())
		if err != nil {
			t.Fatal(err)
		}
		if backfill {
			err = store.ValidateBackfill(context.Background())
		} else {
			err = store.Validate(context.Background())
		}
		if err == nil || err.(*StoreError).Category != "permission" {
			t.Fatalf("backfill=%t denied grant accepted: %v", backfill, err)
		}
	}
}
