package naver

import (
	"io"
	"math/rand"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestFetchItemsRetriesTransientStatusWithSharedClient(t *testing.T) {
	t.Setenv("NAVER_API_ATTEMPTS", "3")
	t.Setenv("NAVER_API_BACKOFF_MIN", "0.001")
	t.Setenv("NAVER_API_BACKOFF_MAX", "0.001")

	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if got := r.Header.Get("X-Naver-Client-Id"); got != "id" {
			t.Fatalf("missing client id header: %q", got)
		}
		if calls == 1 {
			return testResponse(http.StatusServiceUnavailable, "temporary"), nil
		}
		return testResponse(http.StatusOK, `{"total":1,"items":[{"title":"R book","isbn":"123"}]}`), nil
	})}

	total, items, err := fetchItemsWithClient(client, "http://naver.test/search", "R", "sim", 1, 10, []APIKey{{ClientID: "id", ClientSecret: "secret"}}, rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("fetchItemsWithClient returned error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want retry then success", calls)
	}
	if total != 1 || len(items) != 1 || items[0].ISBN != "123" {
		t.Fatalf("unexpected response total=%d items=%#v", total, items)
	}
}

func TestFetchItemsDoesNotRetryNonTransientStatus(t *testing.T) {
	t.Setenv("NAVER_API_ATTEMPTS", "3")
	t.Setenv("NAVER_API_BACKOFF_MIN", "0.001")
	t.Setenv("NAVER_API_BACKOFF_MAX", "0.001")

	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return testResponse(http.StatusBadRequest, "bad request"), nil
	})}

	_, _, err := fetchItemsWithClient(client, "http://naver.test/search", "R", "sim", 1, 10, []APIKey{{ClientID: "id", ClientSecret: "secret"}}, rand.New(rand.NewSource(1)))
	if err == nil {
		t.Fatal("expected non-transient status error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want no retry for bad request", calls)
	}
}

func TestFetchItemsRejectsEmptyAPIKeys(t *testing.T) {
	_, _, err := fetchItemsWithClient(nil, "http://naver.test/search", "R", "sim", 1, 10, nil, rand.New(rand.NewSource(1)))
	if err == nil {
		t.Fatal("expected empty API key error")
	}
}

func testResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
