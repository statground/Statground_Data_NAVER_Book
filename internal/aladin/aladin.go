package aladin

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"statground_naver_book_go/internal/envx"
)

var (
	reCntInputA   = regexp.MustCompile(`(?is)<input[^>]*name=["']cnt["'][^>]*value=["']([^"']+)["']`)
	reCntInputB   = regexp.MustCompile(`(?is)<input[^>]*value=["']([^"']+)["'][^>]*name=["']cnt["']`)
	rePageSet     = regexp.MustCompile(`Page_Set\('(\d+)'\)`)
	reEndAnchor   = regexp.MustCompile(`(?is)<a[^>]*href=["']([^"']*Page_Set\('\d+'\)[^"']*)["'][^>]*>\s*끝\s*</a>`)
	rePublisherTD = regexp.MustCompile(`(?is)<td[^>]*class=["'][^"']*c2b_center[^"']*["'][^>]*>(.*?)</td>`)
	reTags        = regexp.MustCompile(`(?is)<[^>]+>`)

	aladinHTTPClient = &http.Client{
		Timeout: 30 * time.Second,
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

func Headers(baseURL string) http.Header {
	h := http.Header{}
	h.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")
	h.Set("Referer", baseURL)
	h.Set("Origin", "https://www.aladin.co.kr")
	return h
}

func ParseCnt(rawHTML string) string {
	if m := reCntInputA.FindStringSubmatch(rawHTML); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	if m := reCntInputB.FindStringSubmatch(rawHTML); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

func ParseLastPage(rawHTML string) int {
	if m := reEndAnchor.FindStringSubmatch(rawHTML); len(m) > 1 {
		if n := parsePageSet(m[1]); n > 0 {
			return n
		}
	}
	matches := rePageSet.FindAllStringSubmatch(rawHTML, -1)
	maxPage := 0
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		n := parseInt(m[1], 0)
		if n > maxPage {
			maxPage = n
		}
	}
	return maxPage
}

func ExtractPublishers(rawHTML string) []string {
	matches := rePublisherTD.FindAllStringSubmatch(rawHTML, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		txt := reTags.ReplaceAllString(m[1], " ")
		txt = html.UnescapeString(txt)
		txt = strings.Join(strings.Fields(txt), " ")
		txt = strings.TrimSpace(txt)
		if txt == "" {
			continue
		}
		if _, ok := seen[txt]; ok {
			continue
		}
		seen[txt] = struct{}{}
		out = append(out, txt)
	}
	return out
}

func parsePageSet(href string) int {
	m := rePageSet.FindStringSubmatch(href)
	if len(m) < 2 {
		return 0
	}
	return parseInt(m[1], 0)
}

func parseInt(s string, def int) int {
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &n); err != nil {
		return def
	}
	return n
}

func FetchPage(page int, cnt string, baseURL string, sleepMin, sleepMax float64, r *rand.Rand) (int, []string, error) {
	sleepRandom(sleepMin, sleepMax, r)
	form := url.Values{}
	form.Set("page", fmt.Sprintf("%d", page))
	form.Set("cnt", cnt)
	payload, err := fetchHTMLWithClient(aladinHTTPClient, http.MethodPost, baseURL, form.Encode(), "application/x-www-form-urlencoded", r)
	if err != nil {
		return 0, nil, err
	}
	return page, ExtractPublishers(string(payload)), nil
}

func DetectCntAndLastPage(baseURL string) (string, int, error) {
	payload, err := fetchHTMLWithClient(aladinHTTPClient, http.MethodGet, baseURL, "", "", nil)
	if err != nil {
		return "", 0, err
	}
	html := string(payload)
	cnt := ParseCnt(html)
	if cnt == "" {
		cnt = "27942"
	}
	last := ParseLastPage(html)
	if last <= 0 {
		return "", 0, fmt.Errorf("failed to detect Aladin last page")
	}
	return cnt, last, nil
}

func fetchHTMLWithClient(client *http.Client, method, baseURL, body, contentType string, r *rand.Rand) ([]byte, error) {
	if client == nil {
		client = aladinHTTPClient
	}
	attempts := envx.Int("ALADIN_HTTP_ATTEMPTS", 3)
	if attempts <= 0 {
		attempts = 1
	}
	backoffMin := durationSecondsEnv("ALADIN_HTTP_BACKOFF_MIN", 500*time.Millisecond)
	backoffMax := durationSecondsEnv("ALADIN_HTTP_BACKOFF_MAX", 4*time.Second)

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		payload, retryable, err := fetchHTMLOnce(client, method, baseURL, body, contentType)
		if err == nil {
			return payload, nil
		}
		lastErr = err
		if !retryable || attempt == attempts {
			return nil, err
		}
		delay := retryDelay(attempt, backoffMin, backoffMax, r)
		fmt.Printf("[warn] aladin retry attempt=%d/%d reason=%s delay=%s\n", attempt+1, attempts, aladinRetryReason(err), delay)
		time.Sleep(delay)
	}
	return nil, lastErr
}

func fetchHTMLOnce(client *http.Client, method, baseURL, body, contentType string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), durationSecondsEnv("ALADIN_HTTP_TIMEOUT", 30*time.Second))
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, baseURL, strings.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header = Headers(baseURL)
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, retryableHTTPError(err), err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, retryableHTTPError(err), err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, retryableHTTPStatus(resp.StatusCode), aladinHTTPStatusError(resp.StatusCode, payload)
	}
	return payload, false, nil
}

func aladinHTTPStatusError(status int, payload []byte) error {
	body := compactHTTPBody(payload)
	if body == "" {
		body = http.StatusText(status)
	}
	if body == "" {
		body = "non-200 response"
	}
	return fmt.Errorf("aladin http %d: %s", status, body)
}

func compactHTTPBody(payload []byte) string {
	body := html.UnescapeString(string(payload))
	body = reTags.ReplaceAllString(body, " ")
	body = strings.Join(strings.Fields(body), " ")
	body = strings.TrimSpace(body)
	const maxLen = 240
	if len(body) > maxLen {
		body = strings.TrimSpace(body[:maxLen]) + "..."
	}
	return body
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

func durationSecondsEnv(name string, fallback time.Duration) time.Duration {
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

func aladinRetryReason(err error) string {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "aladin http 429"):
		return "rate_limited"
	case strings.Contains(msg, "aladin http 500"),
		strings.Contains(msg, "aladin http 502"),
		strings.Contains(msg, "aladin http 503"),
		strings.Contains(msg, "aladin http 504"):
		return "unavailable"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "connection reset"), strings.Contains(msg, "connection refused"), strings.Contains(msg, "broken pipe"), strings.Contains(msg, "eof"):
		return "network"
	default:
		return "temporary"
	}
}

func CrawlPublishersDynamic(baseURL string, maxWorkers int, sleepMin, sleepMax float64, r *rand.Rand) ([]string, int, error) {
	cnt, lastPage, err := DetectCntAndLastPage(baseURL)
	if err != nil {
		return nil, 0, err
	}
	if maxWorkers <= 0 {
		maxWorkers = 1
	}

	type result struct {
		page int
		pubs []string
		err  error
	}

	jobs := make(chan int)
	results := make(chan result, lastPage)
	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		workerRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i+1)))
		go func(rr *rand.Rand) {
			defer wg.Done()
			for page := range jobs {
				p, pubs, err := FetchPage(page, cnt, baseURL, sleepMin, sleepMax, rr)
				results <- result{page: p, pubs: pubs, err: err}
			}
		}(workerRand)
	}

	go func() {
		for page := 1; page <= lastPage; page++ {
			jobs <- page
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	byPage := make(map[int][]string, lastPage)
	for res := range results {
		if res.err != nil {
			return nil, 0, res.err
		}
		byPage[res.page] = res.pubs
	}

	all := make([]string, 0)
	seen := map[string]struct{}{}
	for page := 1; page <= lastPage; page++ {
		pubs := byPage[page]
		sort.Strings(pubs)
		for _, pub := range pubs {
			pub = strings.TrimSpace(pub)
			if pub == "" {
				continue
			}
			if _, ok := seen[pub]; ok {
				continue
			}
			seen[pub] = struct{}{}
			all = append(all, pub)
		}
	}
	return all, lastPage, nil
}

func sleepRandom(minV, maxV float64, r *rand.Rand) {
	if maxV <= 0 {
		return
	}
	if minV < 0 {
		minV = 0
	}
	if maxV < minV {
		maxV = minV
	}
	delta := maxV - minV
	seconds := minV
	if delta > 0 {
		if r == nil {
			seconds += rand.Float64() * delta
		} else {
			seconds += r.Float64() * delta
		}
	}
	time.Sleep(time.Duration(seconds * float64(time.Second)))
}
