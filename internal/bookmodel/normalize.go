package bookmodel

import (
	"strings"

	"statground_naver_book_go/internal/util"
)

// NormalizeDocument applies provider-independent text, list, and ISBN
// normalization while preserving source-specific values such as StatusRaw and
// already-normalized price availability.
func NormalizeDocument(document BookDocument) BookDocument {
	document.Provider = strings.ToLower(strings.TrimSpace(document.Provider))
	document.Title = NormalizeText(document.Title)
	document.Contents = NormalizeText(document.Contents)
	document.SourceURL = strings.TrimSpace(document.SourceURL)
	document.ThumbnailURL = strings.TrimSpace(document.ThumbnailURL)
	document.Publisher = NormalizeText(document.Publisher)
	document.StatusRaw = strings.TrimSpace(document.StatusRaw)
	document.Authors = NormalizeNames(document.Authors)
	document.Translators = NormalizeNames(document.Translators)

	identity := NormalizeISBN(document.ISBNRaw)
	document.ISBNRaw = identity.Raw
	document.ISBN10 = identity.ISBN10
	document.ISBN13 = identity.ISBN13
	document.CanonicalISBN = identity.CanonicalISBN
	document.ISBNValid = identity.Valid
	return document
}

// NormalizeText removes provider markup, decodes entities, and collapses
// repeated whitespace.
func NormalizeText(value string) string {
	return util.StripHTML(value)
}

// NormalizeNames trims, removes markup, and de-duplicates names while
// preserving the provider's original order.
func NormalizeNames(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}

	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = NormalizeText(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

// SplitNames converts the legacy single-string author format into a normalized
// list. Commas are intentionally preserved because they may be part of a name.
func SplitNames(raw string) []string {
	return NormalizeNames(strings.FieldsFunc(raw, func(r rune) bool {
		switch r {
		case '^', '|', ';', '\n', '\r':
			return true
		default:
			return false
		}
	}))
}
