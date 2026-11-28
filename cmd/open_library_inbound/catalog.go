package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"time"

	"statground_naver_book_go/internal/bookmodel"
)

const (
	catalogIndex = "https://www.statground.net/sitemap_book_index.xml"
	catalogLimit = 96
	sitemapNS    = "http://www.sitemaps.org/schemas/sitemap/0.9"
)

var catalogPart = regexp.MustCompile(`^/sitemap_book(?:_[0-9]+)?\.xml$`)
var catalogDetail = regexp.MustCompile(`^/ko/workbench/book/details/([0-9]{13})/$`)

func catalogPath(value string) string {
	location, err := url.Parse(value)
	if err != nil || location.Scheme != "https" || location.Host != "www.statground.net" ||
		location.User != nil || location.RawQuery != "" || location.Fragment != "" {
		return ""
	}
	return location.Path
}

func catalogLocations(client *http.Client, address, root, entry string, budget int64, visit func(string) bool) error {
	response, err := client.Get(address)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("public book sitemap returned HTTP %d", response.StatusCode)
	}
	parser := xml.NewDecoder(io.LimitReader(response.Body, budget))
	var opened bool
	for {
		token, err := parser.Token()
		if err != nil {
			return err
		}
		if element, ok := token.(xml.StartElement); ok && !opened {
			if element.Name.Local != root || element.Name.Space != sitemapNS {
				return fmt.Errorf("public book sitemap has an unexpected root")
			}
			opened = true
		} else if ok && element.Name == (xml.Name{Space: sitemapNS, Local: entry}) {
			var location struct {
				URL string `xml:"loc"`
			}
			if err := parser.DecodeElement(&location, &element); err != nil {
				return err
			}
			if visit(location.URL) {
				return nil
			}
		} else if end, ok := token.(xml.EndElement); ok && end.Name == (xml.Name{Space: sitemapNS, Local: root}) {
			return nil
		}
	}
}

func currentCatalogISBNs(snapshot time.Time) (string, map[string]bool, error) {
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	var parts []string
	if err := catalogLocations(client, catalogIndex, "sitemapindex", "sitemap", 256<<10, func(location string) bool {
		if catalogPart.MatchString(catalogPath(location)) {
			parts = append(parts, location)
		}
		return false
	}); err != nil {
		return "", nil, err
	}
	if len(parts) == 0 {
		return "", nil, fmt.Errorf("public book sitemap index has no eligible paths")
	}
	slices.Sort(parts)
	parts = slices.Compact(parts)
	address := parts[(snapshot.Year()*12+int(snapshot.Month())-1)%len(parts)]
	isbns := make(map[string]bool)
	if err := catalogLocations(client, address, "urlset", "url", 1<<20, func(location string) bool {
		match := catalogDetail.FindStringSubmatch(catalogPath(location))
		if len(match) == 2 && bookmodel.NormalizeISBN(match[1]).Valid {
			isbns[match[1]] = false
		}
		return len(isbns) >= catalogLimit
	}); err != nil {
		return "", nil, err
	}
	if len(isbns) == 0 {
		return "", nil, fmt.Errorf("public book sitemap contains no valid ISBNs")
	}
	return address, isbns, nil
}
