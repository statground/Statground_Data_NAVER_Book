package bookmodel

import "time"

// BookDocument is the provider-neutral representation of one upstream book
// document. Provider-specific raw rows remain separate from this model.
type BookDocument struct {
	Provider     string
	Title        string
	Contents     string
	SourceURL    string
	ThumbnailURL string

	ISBNRaw       string
	ISBN10        string
	ISBN13        string
	CanonicalISBN string
	ISBNValid     bool

	PublishedAt *time.Time
	Authors     []string
	Publisher   string
	Translators []string

	// Negative upstream price values represent an unavailable price and are
	// normalized to nil by the provider adapter. Zero remains observable.
	ListPrice *uint64
	SalePrice *uint64
	StatusRaw string
}
