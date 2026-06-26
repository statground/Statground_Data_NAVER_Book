package aladin

import (
	"fmt"
	"html"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	reCntInputA   = regexp.MustCompile(`(?is)<input[^>]*name=["']cnt["'][^>]*value=["']([^"']+)["']`)
	reCntInputB   = regexp.MustCompile(`(?is)<input[^>]*value=["']([^"']+)["'][^>]*name=["']cnt["']`)
	rePageSet     = regexp.MustCompile(`Page_Set\('(\d+)'\)`)
	reEndAnchor   = regexp.MustCompile(`(?is)<a[^>]*href=["']([^"']*Page_Set\('\d+'\)[^"']*)["'][^>]*>\s*끝\s*</a>`)
	rePublisherTD = regexp.MustCompile(`(?is)<td[^>]*class=["'][^"']*c2b_center[^"']*["'][^>]*>(.*?)</td>`)
	reTags        = regexp.MustCompile(`(?is)<[^>]+>`)
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
	req, err := http.NewRequest(http.MethodPost, baseURL, strings.NewReader(form.Encode()))
	if err != nil {
		return 0, nil, err
	}
	req.Header = Headers(baseURL)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, nil, aladinHTTPStatusError(resp.StatusCode, payload)
	}
	return page, ExtractPublishers(string(payload)), nil
}

func DetectCntAndLastPage(baseURL string) (string, int, error) {
	req, err := http.NewRequest(http.MethodGet, baseURL, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header = Headers(baseURL)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, aladinHTTPStatusError(resp.StatusCode, payload)
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
