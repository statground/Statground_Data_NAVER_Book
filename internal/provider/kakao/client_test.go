package kakao

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"statground_naver_book_go/internal/provider"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func fixture(t *testing.T, name string) string {
	t.Helper()
	payload, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func httpResult(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func testClient(t *testing.T, transport http.RoundTripper, mutate func(*Config)) *Client {
	t.Helper()
	config := Config{
		APIKey:       "unit-key",
		Endpoint:     BookSearchURL,
		HTTPClient:   &http.Client{Transport: transport},
		Timeout:      100 * time.Millisecond,
		Attempts:     1,
		BackoffMin:   time.Millisecond,
		BackoffMax:   time.Millisecond,
		MaxBodyBytes: defaultMaxBodySize,
		Sleep: func(context.Context, time.Duration) error {
			return nil
		},
	}
	if mutate != nil {
		mutate(&config)
	}
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestSearchContractNormalizationAndRequestDeduplication(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		if request.Method != http.MethodGet {
			t.Errorf("method=%s", request.Method)
		}
		if request.URL.Scheme != "https" || request.URL.Host != "dapi.kakao.com" ||
			request.URL.Path != "/v3/search/book" {
			t.Errorf("unexpected URL=%s", request.URL.String())
		}
		query := request.URL.Query()
		if query.Get("query") != "통계학 입문" ||
			query.Get("sort") != "accuracy" ||
			query.Get("page") != "1" ||
			query.Get("size") != "10" {
			t.Errorf("unexpected query=%v", query)
		}
		if _, exists := query["target"]; exists {
			t.Error("empty target must be omitted")
		}
		if got := request.Header.Get("Authorization"); got != "KakaoAK unit-key" {
			t.Errorf("authorization=%q", got)
		}
		return httpResult(http.StatusOK, fixture(t, "search_success.json")), nil
	}), nil)

	searchRequest := provider.SearchRequest{Query: "  통계학   입문  "}
	response, err := client.Search(context.Background(), searchRequest)
	if err != nil {
		t.Fatal(err)
	}
	if client.Name() != ProviderName {
		t.Fatalf("name=%q", client.Name())
	}
	if response.TotalCount != 2 || response.PageableCount != 2 || !response.IsEnd {
		t.Fatalf("unexpected meta=%+v", response)
	}
	if len(response.Documents) != 2 {
		t.Fatalf("documents=%d", len(response.Documents))
	}
	first := response.Documents[0]
	if first.Provider != ProviderName || first.Title != "통계학 입문" ||
		first.ISBN10 != "8983920688" || first.ISBN13 != "9788983920683" ||
		first.CanonicalISBN != "9788983920683" || !first.ISBNValid {
		t.Fatalf("unexpected first document=%+v", first)
	}
	if len(first.Authors) != 2 || first.Authors[1] != "Jane Doe" ||
		len(first.Translators) != 1 {
		t.Fatalf("arrays not preserved: authors=%v translators=%v", first.Authors, first.Translators)
	}
	if first.PublishedAt == nil || first.PublishedAt.Year() != 2026 {
		t.Fatalf("published_at=%v", first.PublishedAt)
	}
	if first.ListPrice == nil || *first.ListPrice != 30_000 ||
		first.SalePrice == nil || *first.SalePrice != 27_000 {
		t.Fatalf("price=%v sale_price=%v", first.ListPrice, first.SalePrice)
	}
	second := response.Documents[1]
	if second.ListPrice == nil || *second.ListPrice != 0 {
		t.Fatalf("zero list price was not preserved: %v", second.ListPrice)
	}
	if second.SalePrice != nil {
		t.Fatalf("negative sale price must normalize to unavailable: %v", second.SalePrice)
	}

	response.Documents[0].Authors[0] = "mutated"
	cached, err := client.Search(context.Background(), provider.SearchRequest{
		Query: "통계학 입문",
		Sort:  "accuracy",
		Page:  1,
		Size:  10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate normalized request caused %d HTTP calls", calls.Load())
	}
	if cached.Documents[0].Authors[0] == "mutated" {
		t.Fatal("cached response was mutated by the caller")
	}
}

func TestSearchCoalescesConcurrentDuplicateRequests(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return httpResult(http.StatusOK, fixture(t, "search_empty.json")), nil
	}), nil)

	type result struct {
		response provider.SearchResponse
		err      error
	}
	results := make(chan result, 2)
	search := func(query string) {
		response, err := client.Search(context.Background(), provider.SearchRequest{Query: query})
		results <- result{response: response, err: err}
	}
	go search("Concurrent   Book")
	<-started
	go search("concurrent book")
	close(release)

	for range 2 {
		got := <-results
		if got.err != nil || len(got.response.Documents) != 0 {
			t.Fatalf("response=%+v err=%v", got.response, got.err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent duplicate requests caused %d HTTP calls", calls.Load())
	}
}

func TestSearchTargetAndBoundaryParameters(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		query := request.URL.Query()
		if query.Get("target") != "isbn" || query.Get("page") != "50" || query.Get("size") != "50" {
			t.Errorf("unexpected query=%v", query)
		}
		return httpResult(http.StatusOK, fixture(t, "search_empty.json")), nil
	}), nil)
	_, err := client.Search(context.Background(), provider.SearchRequest{
		Query:  "9788983920683",
		Sort:   "latest",
		Target: "isbn",
		Page:   50,
		Size:   50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestSearchRejectsInvalidRequestWithoutHTTP(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return httpResult(http.StatusOK, fixture(t, "search_empty.json")), nil
	}), nil)
	tests := []provider.SearchRequest{
		{},
		{Query: "book", Sort: "random"},
		{Query: "book", Target: "author"},
		{Query: "book", Page: -1},
		{Query: "book", Page: 51},
		{Query: "book", Size: -1},
		{Query: "book", Size: 51},
	}
	for _, request := range tests {
		if _, err := client.Search(context.Background(), request); ErrorCategory(err) != ErrorInvalidRequest {
			t.Errorf("request=%+v category=%s err=%v", request, ErrorCategory(err), err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid requests caused %d HTTP calls", calls.Load())
	}
}

func TestSearchRetriesOnlyUnavailableResponses(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var sleeps atomic.Int32
	client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch calls.Add(1) {
		case 1:
			return httpResult(http.StatusServiceUnavailable, `{"msg":"temporary"}`), nil
		case 2:
			return httpResult(http.StatusBadGateway, `{"msg":"temporary"}`), nil
		default:
			return httpResult(http.StatusOK, fixture(t, "search_empty.json")), nil
		}
	}), func(config *Config) {
		config.Attempts = 3
		config.Sleep = func(context.Context, time.Duration) error {
			sleeps.Add(1)
			return nil
		}
	})
	if _, err := client.Search(context.Background(), provider.SearchRequest{Query: "book"}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || sleeps.Load() != 2 {
		t.Fatalf("calls=%d sleeps=%d", calls.Load(), sleeps.Load())
	}
}

func TestSearchStopsImmediatelyFor429AndCodeMinus10(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   int
		body     string
		category string
	}{
		{
			name:     "http 429 rate limit",
			status:   http.StatusTooManyRequests,
			body:     `{"msg":"do-not-log-this-body"}`,
			category: ErrorRateLimited,
		},
		{
			name:     "http 400 code minus ten quota",
			status:   http.StatusBadRequest,
			body:     fixture(t, "error_quota.json"),
			category: ErrorQuotaExhausted,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls.Add(1)
				return httpResult(tc.status, tc.body), nil
			}), func(config *Config) {
				config.Attempts = 4
			})
			_, err := client.Search(context.Background(), provider.SearchRequest{Query: "book"})
			if ErrorCategory(err) != tc.category || !IsQuotaStop(err) {
				t.Fatalf("category=%s err=%v", ErrorCategory(err), err)
			}
			if calls.Load() != 1 {
				t.Fatalf("quota stop retried: calls=%d", calls.Load())
			}
			errorText := err.Error()
			if strings.Contains(errorText, "unit-key") ||
				strings.Contains(errorText, "do-not-log-this-body") ||
				strings.Contains(errorText, "request limit exceeded") {
				t.Fatalf("error leaked sensitive upstream data: %q", errorText)
			}
		})
	}
}

func TestSearchDoesNotRetryAuthOrPermissionErrors(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls.Add(1)
				return httpResult(status, `{"msg":"credential detail"}`), nil
			}), func(config *Config) {
				config.Attempts = 4
			})
			_, err := client.Search(context.Background(), provider.SearchRequest{Query: "book"})
			if err == nil {
				t.Fatal("expected error")
			}
			if calls.Load() != 1 {
				t.Fatalf("status=%d calls=%d", status, calls.Load())
			}
		})
	}
}

func TestSearchRejectsNonJSONAndOversizedBodies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		maxBody int64
	}{
		{name: "non-json", body: "<html>bad gateway</html>", maxBody: 1 << 20},
		{name: "oversized", body: strings.Repeat("x", 65), maxBody: 64},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return httpResult(http.StatusOK, tc.body), nil
			}), func(config *Config) {
				config.MaxBodyBytes = tc.maxBody
			})
			_, err := client.Search(context.Background(), provider.SearchRequest{Query: "book"})
			if ErrorCategory(err) != ErrorContract {
				t.Fatalf("category=%s err=%v", ErrorCategory(err), err)
			}
			if strings.Contains(err.Error(), tc.body) {
				t.Fatalf("response body leaked: %q", err)
			}
		})
	}
}

func TestSearchHonorsPerRequestTimeout(t *testing.T) {
	t.Parallel()

	client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	}), func(config *Config) {
		config.Timeout = 5 * time.Millisecond
	})
	_, err := client.Search(context.Background(), provider.SearchRequest{Query: "book"})
	if ErrorCategory(err) != ErrorTimeout {
		t.Fatalf("category=%s err=%v", ErrorCategory(err), err)
	}
}

func TestSearchRejectsMalformedDatetime(t *testing.T) {
	t.Parallel()

	body := `{
		"meta":{"total_count":1,"pageable_count":1,"is_end":true},
		"documents":[{
			"title":"book","isbn":"9780306406157","datetime":"not-a-date",
			"authors":[],"translators":[]
		}]
	}`
	client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return httpResult(http.StatusOK, body), nil
	}), nil)
	_, err := client.Search(context.Background(), provider.SearchRequest{Query: "book"})
	if ErrorCategory(err) != ErrorContract {
		t.Fatalf("category=%s err=%v", ErrorCategory(err), err)
	}
}

func TestNewClientRequiresKeyWithoutEchoingIt(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Config{})
	if err == nil || !strings.Contains(err.Error(), "KAKAO_REST_API_KEY") {
		t.Fatalf("err=%v", err)
	}
}

func TestTransportErrorsAreSanitized(t *testing.T) {
	t.Parallel()

	client := testClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset by peer with upstream detail")
	}), nil)
	_, err := client.Search(context.Background(), provider.SearchRequest{Query: "book"})
	if ErrorCategory(err) != ErrorNetwork {
		t.Fatalf("category=%s err=%v", ErrorCategory(err), err)
	}
	if strings.Contains(err.Error(), "upstream detail") {
		t.Fatalf("transport error leaked: %q", err)
	}
}
