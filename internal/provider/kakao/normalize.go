package kakao

import (
	"fmt"
	"strings"
	"time"

	"statground_naver_book_go/internal/bookmodel"
)

func normalizeDocument(raw responseDocument) (bookmodel.BookDocument, error) {
	isbn := bookmodel.NormalizeISBN(raw.ISBN)
	publishedAt, err := parsePublishedAt(raw.Datetime)
	if err != nil {
		return bookmodel.BookDocument{}, err
	}
	return bookmodel.BookDocument{
		Provider:      ProviderName,
		Title:         bookmodel.NormalizeText(raw.Title),
		Contents:      bookmodel.NormalizeText(raw.Contents),
		SourceURL:     strings.TrimSpace(raw.URL),
		ThumbnailURL:  strings.TrimSpace(raw.Thumbnail),
		ISBNRaw:       raw.ISBN,
		ISBN10:        isbn.ISBN10,
		ISBN13:        isbn.ISBN13,
		CanonicalISBN: isbn.CanonicalISBN,
		ISBNValid:     isbn.Valid,
		PublishedAt:   publishedAt,
		Authors:       normalizeStringList(raw.Authors),
		Publisher:     bookmodel.NormalizeText(raw.Publisher),
		Translators:   normalizeStringList(raw.Translators),
		ListPrice:     nonNegativePrice(raw.Price),
		SalePrice:     nonNegativePrice(raw.SalePrice),
		StatusRaw:     strings.TrimSpace(raw.Status),
	}, nil
}

func parsePublishedAt(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil, fmt.Errorf("kakao book datetime contract error")
	}
	return &parsed, nil
}

func normalizeStringList(values []string) []string {
	if values == nil {
		return []string{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = bookmodel.NormalizeText(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

// Kakao's public contract uses numeric price fields. A negative upstream value
// is treated as unavailable instead of being converted into a huge uint.
// Zero remains an observed value and is therefore preserved.
func nonNegativePrice(value *int64) *uint64 {
	if value == nil || *value < 0 {
		return nil
	}
	normalized := uint64(*value)
	return &normalized
}
