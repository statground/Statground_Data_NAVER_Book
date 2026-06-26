package naver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"statground_naver_book_go/internal/envx"
)

const BookSearchURL = "https://openapi.naver.com/v1/search/book.json"

type APIKey struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type BookItem struct {
	Title       string `json:"title"`
	Link        string `json:"link"`
	Image       string `json:"image"`
	Author      string `json:"author"`
	Discount    string `json:"discount"`
	Publisher   string `json:"publisher"`
	ISBN        string `json:"isbn"`
	Description string `json:"description"`
	Pubdate     string `json:"pubdate"`
}

type searchResponse struct {
	Total int        `json:"total"`
	Items []BookItem `json:"items"`
}

var (
	bookSearchEndpoint = BookSearchURL
	bookSearchClient   = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
)

func LoadAPIKeysFromEnv() ([]APIKey, error) {
	raw, err := envx.Require("NAVER_API_KEYS")
	if err != nil {
		return nil, err
	}
	var keys []APIKey
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return nil, fmt.Errorf("NAVER_API_KEYS must be JSON like [{\"client_id\":\"...\",\"client_secret\":\"...\"}]: %w", err)
	}
	filtered := make([]APIKey, 0, len(keys))
	for _, key := range keys {
		if strings.TrimSpace(key.ClientID) != "" && strings.TrimSpace(key.ClientSecret) != "" {
			filtered = append(filtered, key)
		}
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("NAVER_API_KEYS does not contain usable client_id/client_secret pairs")
	}
	return filtered, nil
}

func pickKey(keys []APIKey, r *rand.Rand) APIKey {
	if len(keys) == 1 {
		return keys[0]
	}
	if r == nil {
		return keys[rand.Intn(len(keys))]
	}
	return keys[r.Intn(len(keys))]
}

func FetchItems(keyword, sort string, start, display int, keys []APIKey, r *rand.Rand) (int, []BookItem, error) {
	return fetchItemsWithClient(bookSearchClient, bookSearchEndpoint, keyword, sort, start, display, keys, r)
}

func fetchItemsWithClient(client *http.Client, endpoint, keyword, sort string, start, display int, keys []APIKey, r *rand.Rand) (int, []BookItem, error) {
	if len(keys) == 0 {
		return 0, nil, fmt.Errorf("NAVER_API_KEYS does not contain usable client_id/client_secret pairs")
	}
	if client == nil {
		client = bookSearchClient
	}
	if strings.TrimSpace(endpoint) == "" {
		endpoint = BookSearchURL
	}
	q := url.Values{}
	q.Set("query", keyword)
	q.Set("display", fmt.Sprintf("%d", display))
	q.Set("start", fmt.Sprintf("%d", start))
	q.Set("sort", sort)

	attempts := envx.Int("NAVER_API_ATTEMPTS", 3)
	if attempts <= 0 {
		attempts = 1
	}
	backoffMin := secondsEnv("NAVER_API_BACKOFF_MIN", 500*time.Millisecond)
	backoffMax := secondsEnv("NAVER_API_BACKOFF_MAX", 4*time.Second)

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		total, items, retryable, err := fetchItemsOnce(client, endpoint+"?"+q.Encode(), keys, r)
		if err == nil {
			return total, items, nil
		}
		lastErr = err
		if !retryable || attempt == attempts {
			return 0, nil, err
		}
		delay := retryDelay(attempt, backoffMin, backoffMax, r)
		fmt.Printf("[warn] naver api retry attempt=%d/%d reason=%s delay=%s\n", attempt+1, attempts, retryReason(err), delay)
		time.Sleep(delay)
	}
	return 0, nil, lastErr
}

func fetchItemsOnce(client *http.Client, requestURL string, keys []APIKey, r *rand.Rand) (int, []BookItem, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), secondsEnv("NAVER_API_TIMEOUT", 20*time.Second))
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return 0, nil, false, err
	}
	api := pickKey(keys, r)
	req.Header.Set("X-Naver-Client-Id", api.ClientID)
	req.Header.Set("X-Naver-Client-Secret", api.ClientSecret)

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, retryableHTTPError(err), err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, retryableHTTPError(err), err
	}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("naver api http %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
		return 0, nil, retryableHTTPStatus(resp.StatusCode), err
	}
	var out searchResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return 0, nil, false, err
	}
	if out.Items == nil {
		out.Items = []BookItem{}
	}
	return out.Total, out.Items, false, nil
}

func retryableHTTPStatus(status int) bool {
	return status == http.StatusTooManyRequests ||
		status == http.StatusInternalServerError ||
		status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func retryableHTTPError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "temporary")
}

func secondsEnv(name string, fallback time.Duration) time.Duration {
	seconds := envx.Float(name, fallback.Seconds())
	if seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds * float64(time.Second))
}

func retryDelay(attempt int, minDelay, maxDelay time.Duration, r *rand.Rand) time.Duration {
	if minDelay <= 0 {
		minDelay = 500 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 4 * time.Second
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	delay := minDelay
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay/2 {
			delay = maxDelay
			break
		}
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	jitterBase := delay / 4
	if jitterBase <= 0 {
		return delay
	}
	if r == nil {
		return delay + time.Duration(rand.Int63n(int64(jitterBase)))
	}
	return delay + time.Duration(r.Int63n(int64(jitterBase)))
}

func retryReason(err error) string {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "naver api http 429"):
		return "rate_limited"
	case strings.Contains(msg, "naver api http 500"),
		strings.Contains(msg, "naver api http 502"),
		strings.Contains(msg, "naver api http 503"),
		strings.Contains(msg, "naver api http 504"):
		return "unavailable"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "connection reset"), strings.Contains(msg, "connection refused"), strings.Contains(msg, "broken pipe"), strings.Contains(msg, "eof"):
		return "network"
	default:
		return "temporary"
	}
}
