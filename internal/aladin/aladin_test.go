package aladin

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

func TestAladinHTTPStatusErrorCompactsHTML(t *testing.T) {
	err := aladinHTTPStatusError(503, []byte(`<!DOCTYPE HTML><HTML><HEAD><TITLE>Service Unavailable</TITLE></HEAD><BODY><h2>Service Unavailable</h2><p>HTTP Error 503.</p></BODY></HTML>`))
	msg := err.Error()
	if !strings.Contains(msg, "aladin http 503") {
		t.Fatalf("expected status prefix, got %q", msg)
	}
	if strings.Contains(msg, "<HTML>") || strings.Contains(msg, "<BODY>") {
		t.Fatalf("expected compact text error, got %q", msg)
	}
	if !strings.Contains(msg, "Service Unavailable") {
		t.Fatalf("expected status text, got %q", msg)
	}
}

func TestFetchHTMLWithClientRetriesTransientStatus(t *testing.T) {
	t.Setenv("ALADIN_HTTP_ATTEMPTS", "3")
	t.Setenv("ALADIN_HTTP_BACKOFF_MIN", "0.001")
	t.Setenv("ALADIN_HTTP_BACKOFF_MAX", "0.001")

	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if got := r.Header.Get("Origin"); got != "https://www.aladin.co.kr" {
			t.Fatalf("Origin header = %q", got)
		}
		if calls == 1 {
			return aladinTestResponse(http.StatusServiceUnavailable, "temporary"), nil
		}
		return aladinTestResponse(http.StatusOK, `<input name="cnt" value="1"><a href="javascript:Page_Set('2')">끝</a>`), nil
	})}

	payload, err := fetchHTMLWithClient(client, http.MethodGet, "https://www.aladin.co.kr/aladdin/PublisherList.aspx", "", "", rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("fetchHTMLWithClient returned error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want retry then success", calls)
	}
	if !strings.Contains(string(payload), `Page_Set('2')`) {
		t.Fatalf("unexpected payload: %s", string(payload))
	}
}

func TestFetchHTMLWithClientDoesNotRetryBadRequest(t *testing.T) {
	t.Setenv("ALADIN_HTTP_ATTEMPTS", "3")
	t.Setenv("ALADIN_HTTP_BACKOFF_MIN", "0.001")
	t.Setenv("ALADIN_HTTP_BACKOFF_MAX", "0.001")

	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return aladinTestResponse(http.StatusBadRequest, "bad request"), nil
	})}

	_, err := fetchHTMLWithClient(client, http.MethodGet, "https://www.aladin.co.kr/aladdin/PublisherList.aspx", "", "", rand.New(rand.NewSource(1)))
	if err == nil {
		t.Fatal("expected bad request error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want no retry", calls)
	}
}

func aladinTestResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
