package naver

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
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
	q := url.Values{}
	q.Set("query", keyword)
	q.Set("display", fmt.Sprintf("%d", display))
	q.Set("start", fmt.Sprintf("%d", start))
	q.Set("sort", sort)

	req, err := http.NewRequest(http.MethodGet, BookSearchURL+"?"+q.Encode(), nil)
	if err != nil {
		return 0, nil, err
	}
	api := pickKey(keys, r)
	req.Header.Set("X-Naver-Client-Id", api.ClientID)
	req.Header.Set("X-Naver-Client-Secret", api.ClientSecret)

	client := &http.Client{Timeout: 20 * time.Second}
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
		return 0, nil, fmt.Errorf("naver api http %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var out searchResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return 0, nil, err
	}
	if out.Items == nil {
		out.Items = []BookItem{}
	}
	return out.Total, out.Items, nil
}
