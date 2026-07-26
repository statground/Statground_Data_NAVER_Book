package bookmodel

import (
	"reflect"
	"testing"
)

func TestNormalizeDocument(t *testing.T) {
	zeroPrice := uint64(0)
	document := NormalizeDocument(BookDocument{
		Provider:    " NAVER ",
		Title:       " <b>R</b>   Book ",
		Contents:    "Data &amp; statistics",
		SourceURL:   " https://example.com/book ",
		ISBNRaw:     " 0306406152 978-0-306-40615-7 ",
		Authors:     []string{" Alice ", "<b>Bob</b>", "alice", ""},
		Publisher:   " Example &amp; Co. ",
		Translators: []string{" Translator ", "translator"},
		SalePrice:   &zeroPrice,
		StatusRaw:   " unavailable ",
	})

	if document.Provider != "naver" {
		t.Fatalf("Provider = %q, want naver", document.Provider)
	}
	if document.Title != "R Book" || document.Contents != "Data & statistics" {
		t.Fatalf("unexpected normalized text: title=%q contents=%q", document.Title, document.Contents)
	}
	if document.SourceURL != "https://example.com/book" {
		t.Fatalf("SourceURL = %q", document.SourceURL)
	}
	if document.ISBN10 != "0306406152" || document.ISBN13 != "9780306406157" || document.CanonicalISBN != "9780306406157" || !document.ISBNValid {
		t.Fatalf("unexpected ISBN normalization: %#v", document)
	}
	if !reflect.DeepEqual(document.Authors, []string{"Alice", "Bob"}) {
		t.Fatalf("Authors = %#v", document.Authors)
	}
	if !reflect.DeepEqual(document.Translators, []string{"Translator"}) {
		t.Fatalf("Translators = %#v", document.Translators)
	}
	if document.SalePrice == nil || *document.SalePrice != 0 {
		t.Fatalf("zero provider price was not preserved: %#v", document.SalePrice)
	}
	if document.StatusRaw != "unavailable" {
		t.Fatalf("StatusRaw = %q", document.StatusRaw)
	}
}

func TestSplitNamesPreservesCommaAndSplitsLegacySeparators(t *testing.T) {
	got := SplitNames("Doe, Jane^John Smith | 홍길동; John Smith")
	want := []string{"Doe, Jane", "John Smith", "홍길동"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SplitNames = %#v, want %#v", got, want)
	}
}
