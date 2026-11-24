package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"statground_naver_book_go/internal/bookmodel"
)

const googleBooksVolumesURL = "https://www.googleapis.com/books/v1/volumes"
const maxResponseBytes = 512 * 1024

type receipt struct {
	Schema           string    `json:"schema"`
	Provider         string    `json:"provider"`
	ISBN13           string    `json:"isbn13"`
	ObservedAt       time.Time `json:"observed_at"`
	HTTPStatus       int       `json:"http_status"`
	ReturnedVolumes  int       `json:"returned_volumes"`
	ExactISBNMatches int       `json:"exact_isbn_matches"`
	ResponseSHA256   string    `json:"response_sha256"`
}

func lookup(ctx context.Context, client *http.Client, endpoint, isbn, apiKey string) (receipt, error) {
	identity := bookmodel.NormalizeISBN(isbn)
	if !identity.Valid || identity.CanonicalISBN == "" {
		return receipt{}, fmt.Errorf("invalid ISBN-10 or ISBN-13")
	}
	if strings.TrimSpace(apiKey) == "" {
		return receipt{}, fmt.Errorf("GOOGLE_BOOKS_API_KEY is required")
	}
	canonical := isbn13(identity)
	params := url.Values{
		"q":          {"isbn:" + canonical},
		"key":        {apiKey},
		"maxResults": {"10"},
		"printType":  {"books"},
		"fields":     {"totalItems,items(volumeInfo(industryIdentifiers))"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return receipt{}, fmt.Errorf("Google Books request could not be constructed")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		// net/http errors can include the complete URL and API key.
		return receipt{}, fmt.Errorf("Google Books transport failed before response")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return receipt{}, fmt.Errorf("Google Books returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		return receipt{}, fmt.Errorf("Google Books response could not be read within limit")
	}
	var volumes struct {
		TotalItems *int `json:"totalItems"`
		Items      []struct {
			VolumeInfo struct {
				IndustryIdentifiers []struct {
					Type       string `json:"type"`
					Identifier string `json:"identifier"`
				} `json:"industryIdentifiers"`
			} `json:"volumeInfo"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &volumes); err != nil || volumes.TotalItems == nil || *volumes.TotalItems < 0 || len(volumes.Items) > 10 {
		return receipt{}, fmt.Errorf("invalid Google Books volumes response")
	}
	sig := sha256.Sum256(body)
	checked := receipt{
		Schema: "statground.book.google-inbound-receipt.v1", Provider: "google-books", ISBN13: canonical,
		ObservedAt: time.Now().UTC(), HTTPStatus: resp.StatusCode, ReturnedVolumes: len(volumes.Items),
		ResponseSHA256: hex.EncodeToString(sig[:]),
	}
	for _, volume := range volumes.Items {
		for _, identifier := range volume.VolumeInfo.IndustryIdentifiers {
			if identifier.Type == "ISBN_10" || identifier.Type == "ISBN_13" {
				match := bookmodel.NormalizeISBN(identifier.Identifier)
				if match.Valid && isbn13(match) == canonical {
					checked.ExactISBNMatches++
					break
				}
			}
		}
	}
	return checked, nil
}

// isbn13 maps an equivalent ISBN-10 to the same edition key as its ISBN-13.
func isbn13(identity bookmodel.ISBNIdentity) string {
	if identity.ISBN13 != "" {
		return identity.ISBN13
	}
	prefix := "978" + identity.ISBN10[:9]
	var sum int
	for i := range prefix {
		weight := 1
		if i%2 != 0 {
			weight = 3
		}
		sum += weight * int(prefix[i]-'0')
	}
	return prefix + string(rune('0'+(10-sum%10)%10))
}

func main() {
	isbn := flag.String("isbn", "", "ISBN-10 or ISBN-13 for one public bibliographic lookup")
	flag.Parse()
	client := &http.Client{
		Timeout:       10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	record, err := lookup(context.Background(), client, googleBooksVolumesURL, *isbn, os.Getenv("GOOGLE_BOOKS_API_KEY"))
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(record)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
