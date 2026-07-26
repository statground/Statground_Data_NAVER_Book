package naverprovider

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"testing"

	legacynaver "statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/provider"
)

func TestAdapterMapsProviderRequestAndNAVERResponse(t *testing.T) {
	keys := []legacynaver.APIKey{{ClientID: "id", ClientSecret: "secret"}}
	adapter := New(keys, rand.New(rand.NewSource(1)))
	adapter.fetch = func(keyword, sort string, start, display int, gotKeys []legacynaver.APIKey, _ *rand.Rand) (int, []legacynaver.BookItem, error) {
		if keyword != "R language" || sort != "sim" || start != 51 || display != 50 {
			t.Fatalf("unexpected legacy request: query=%q sort=%q start=%d display=%d", keyword, sort, start, display)
		}
		if !reflect.DeepEqual(gotKeys, keys) {
			t.Fatalf("keys = %#v, want %#v", gotKeys, keys)
		}
		return 120, []legacynaver.BookItem{{
			Title:       "<b>R</b> Book",
			Link:        "https://example.com/book",
			Image:       "https://example.com/book.jpg",
			Author:      "Alice^Bob",
			Discount:    "12000",
			Publisher:   "Example",
			ISBN:        "0306406152 9780306406157",
			Description: "Data &amp; statistics",
			Pubdate:     "20260726",
		}}, nil
	}

	response, err := adapter.Search(context.Background(), provider.SearchRequest{
		Query: "R language",
		Sort:  "accuracy",
		Page:  2,
		Size:  50,
	})
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if adapter.Name() != "naver" {
		t.Fatalf("Name = %q, want naver", adapter.Name())
	}
	if response.TotalCount != 120 || response.PageableCount != 120 || response.IsEnd {
		t.Fatalf("unexpected response metadata: %#v", response)
	}
	if len(response.Documents) != 1 {
		t.Fatalf("documents = %d, want 1", len(response.Documents))
	}
	document := response.Documents[0]
	if document.Title != "R Book" || document.CanonicalISBN != "9780306406157" || !document.ISBNValid {
		t.Fatalf("unexpected normalized document: %#v", document)
	}
	if !reflect.DeepEqual(document.Authors, []string{"Alice", "Bob"}) {
		t.Fatalf("Authors = %#v", document.Authors)
	}
	if document.SalePrice == nil || *document.SalePrice != 12000 {
		t.Fatalf("SalePrice = %#v", document.SalePrice)
	}
	if document.PublishedAt == nil || document.PublishedAt.Format("20060102") != "20260726" {
		t.Fatalf("PublishedAt = %#v", document.PublishedAt)
	}
}

func TestAdapterMapsLatestAndPageableLimit(t *testing.T) {
	adapter := New([]legacynaver.APIKey{{ClientID: "id", ClientSecret: "secret"}}, nil)
	adapter.fetch = func(_ string, sort string, _, _ int, _ []legacynaver.APIKey, _ *rand.Rand) (int, []legacynaver.BookItem, error) {
		if sort != "date" {
			t.Fatalf("sort = %q, want date", sort)
		}
		return 2500, nil, nil
	}

	response, err := adapter.Search(context.Background(), provider.SearchRequest{Query: "R", Sort: "latest"})
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if response.PageableCount != 1000 || !response.IsEnd {
		t.Fatalf("unexpected response metadata: %#v", response)
	}
}

func TestAdapterRejectsUnsupportedTargetAndNAVERStart(t *testing.T) {
	adapter := New(nil, nil)
	calls := 0
	adapter.fetch = func(string, string, int, int, []legacynaver.APIKey, *rand.Rand) (int, []legacynaver.BookItem, error) {
		calls++
		return 0, nil, nil
	}

	if _, err := adapter.Search(context.Background(), provider.SearchRequest{Query: "R", Target: "person"}); err == nil {
		t.Fatal("expected unsupported target error")
	}
	if _, err := adapter.Search(context.Background(), provider.SearchRequest{Query: "R", Page: 50, Size: 50}); err == nil {
		t.Fatal("expected NAVER start boundary error")
	}
	if calls != 0 {
		t.Fatalf("fetch calls = %d, want 0", calls)
	}
}

func TestAdapterHonorsCanceledContextBeforeFetch(t *testing.T) {
	adapter := New(nil, nil)
	adapter.fetch = func(string, string, int, int, []legacynaver.APIKey, *rand.Rand) (int, []legacynaver.BookItem, error) {
		return 0, nil, errors.New("fetch must not run")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := adapter.Search(ctx, provider.SearchRequest{Query: "R"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Search error = %v, want context.Canceled", err)
	}
}
