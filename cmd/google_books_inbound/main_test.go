package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLookupEmitsOnlyReceiptForExactEdition(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.Method != http.MethodGet || q.Get("q") != "isbn:9780306406157" || q.Get("key") != "fixture-api-key" || q.Get("maxResults") != "10" || strings.Contains(q.Get("fields"), "description") {
			t.Errorf("unexpected API request")
		}
		fmt.Fprint(w, `{"totalItems":2,"items":[{"volumeInfo":{"title":"fixture provider text","industryIdentifiers":[{"type":"ISBN_10","identifier":"0306406152"},{"type":"ISBN_13","identifier":"9780306406157"}]}},{"volumeInfo":{"industryIdentifiers":[{"type":"ISBN_13","identifier":"9781861972712"}]}}]}`)
	}))
	defer api.Close()
	r, err := lookup(context.Background(), api.Client(), api.URL, "0-306-40615-2", "fixture-api-key")
	if err != nil || r.ISBN13 != "9780306406157" || r.ReturnedVolumes != 2 || r.ExactISBNMatches != 1 || len(r.ResponseSHA256) != 64 {
		t.Fatalf("unexpected receipt: %+v, %v", r, err)
	}
	encoded, err := json.Marshal(r)
	if err != nil || strings.Contains(string(encoded), "fixture provider text") || strings.Contains(string(encoded), "fixture-api-key") {
		t.Fatal("receipt exposed provider content or key")
	}
}

func TestLookupRejectsInvalidInputAndRedactsProviderFailure(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, r.URL.String(), http.StatusTooManyRequests)
	}))
	defer api.Close()
	for _, tc := range []struct{ isbn, key, want string }{
		{"invalid-isbn", "fixture-api-key", "invalid ISBN-10 or ISBN-13"},
		{"9780306406157", "", "GOOGLE_BOOKS_API_KEY is required"},
		{"9780306406157", "fixture-api-key", "Google Books returned HTTP 429"},
	} {
		_, err := lookup(context.Background(), api.Client(), api.URL, tc.isbn, tc.key)
		if err == nil || err.Error() != tc.want {
			t.Fatalf("invalid or unredacted error: %v", err)
		}
	}
}
