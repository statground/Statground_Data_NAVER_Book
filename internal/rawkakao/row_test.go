package rawkakao

import (
	"reflect"
	"testing"
	"time"

	"statground_naver_book_go/internal/bookmodel"
)

func TestBuildRowPreservesProviderEvidenceAndAliases(t *testing.T) {
	ids := []string{
		"01900000-0000-7000-8000-000000000001",
		"01900000-0000-7000-8000-000000000002",
	}
	idIndex := 0
	idGenerator := func() string {
		id := ids[idIndex]
		idIndex++
		return id
	}
	collectedAt := time.Date(2026, 7, 26, 12, 34, 56, 0, time.FixedZone("KST", 9*60*60))
	row, err := BuildRow(bookmodel.BookDocument{
		Provider:      "kakao",
		Title:         "Book",
		ISBNRaw:       "0306406152 9780306406157",
		ISBN10:        "0306406152",
		ISBN13:        "9780306406157",
		CanonicalISBN: "9780306406157",
		ISBNValid:     true,
		Authors:       []string{"Author"},
	}, Evidence{
		RunUUID:       "01900000-0000-7000-8000-000000000010",
		RequestUUID:   "01900000-0000-7000-8000-000000000011",
		Mode:          "fixed_keyword",
		Query:         "language learning",
		Sort:          "latest",
		Page:          2,
		Size:          50,
		TotalCount:    100,
		PageableCount: 75,
		IsEnd:         true,
		CollectedAt:   collectedAt,
	}, idGenerator)
	if err != nil {
		t.Fatal(err)
	}
	if row["uuid"] != ids[0] || row["event_uuid"] != ids[1] {
		t.Fatalf("IDs were not application-generated: %#v", row)
	}
	if row["search_mode"] != "fixed_keyword" || row["search_query"] != "language learning" ||
		row["collected_at"] != "2026-07-26 12:34:56.000" {
		t.Fatalf("missing collection evidence: %#v", row)
	}
	if !reflect.DeepEqual(row["isbn_aliases"], []string{"0306406152"}) {
		t.Fatalf("isbn_aliases = %#v", row["isbn_aliases"])
	}
	if row["canonical_isbn"] != "9780306406157" || row["isbn_valid"] != uint8(1) {
		t.Fatalf("unexpected ISBN row: %#v", row)
	}
	if row["content_hash"] == "" || row["payload"] == "" {
		t.Fatalf("content hash or payload missing: %#v", row)
	}
}

func TestContentHashIsStableAndChangesWithContent(t *testing.T) {
	document := bookmodel.BookDocument{Title: "Book", ISBNRaw: "9780306406157"}
	first, err := ContentHash(document)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ContentHash(document)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("content hash is not deterministic: %q != %q", first, second)
	}
	document.Title = "Changed"
	changed, err := ContentHash(document)
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatal("content hash did not change")
	}
}
