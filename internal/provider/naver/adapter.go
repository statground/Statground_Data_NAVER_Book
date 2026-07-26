package naverprovider

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/bookmodel"
	legacynaver "statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/provider"
)

type fetchFunc func(keyword, sort string, start, display int, keys []legacynaver.APIKey, r *rand.Rand) (int, []legacynaver.BookItem, error)

// Adapter exposes the current NAVER client through the provider-neutral search
// contract. Existing collector code continues to call the legacy client
// directly until the later runtime cutover phase.
type Adapter struct {
	keys  []legacynaver.APIKey
	rand  *rand.Rand
	fetch fetchFunc
}

func New(keys []legacynaver.APIKey, random *rand.Rand) *Adapter {
	return &Adapter{
		keys: append([]legacynaver.APIKey(nil), keys...),
		rand: random,
	}
}

func (a *Adapter) Name() string {
	return "naver"
}

func (a *Adapter) Search(ctx context.Context, request provider.SearchRequest) (provider.SearchResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return provider.SearchResponse{}, ctx.Err()
	default:
	}

	request, err := provider.NormalizeSearchRequest(request)
	if err != nil {
		return provider.SearchResponse{}, err
	}
	if request.Target != "" {
		return provider.SearchResponse{}, fmt.Errorf("NAVER compatibility adapter does not support target %q", request.Target)
	}

	sort, err := naverSort(request.Sort)
	if err != nil {
		return provider.SearchResponse{}, err
	}
	start := (request.Page-1)*request.Size + 1
	if start > 1000 {
		return provider.SearchResponse{}, fmt.Errorf("NAVER compatibility adapter start exceeds 1000")
	}

	fetch := a.fetch
	if fetch == nil {
		fetch = legacynaver.FetchItems
	}
	total, items, err := fetch(request.Query, sort, start, request.Size, a.keys, a.rand)
	if err != nil {
		return provider.SearchResponse{}, err
	}

	documents := make([]bookmodel.BookDocument, 0, len(items))
	for _, item := range items {
		documents = append(documents, naverDocument(item))
	}

	pageableCount := total
	if pageableCount < 0 {
		pageableCount = 0
	}
	if pageableCount > 1000 {
		pageableCount = 1000
	}
	lastResult := start + len(items) - 1
	isEnd := len(items) == 0 || lastResult >= pageableCount

	return provider.SearchResponse{
		TotalCount:    total,
		PageableCount: pageableCount,
		IsEnd:         isEnd,
		Documents:     documents,
	}, nil
}

func naverSort(sort string) (string, error) {
	switch sort {
	case "accuracy":
		return "sim", nil
	case "latest":
		return "date", nil
	default:
		return "", fmt.Errorf("unsupported NAVER compatibility sort %q", sort)
	}
}

func naverDocument(item legacynaver.BookItem) bookmodel.BookDocument {
	var publishedAt *time.Time
	if value, err := time.Parse("20060102", strings.TrimSpace(item.Pubdate)); err == nil {
		publishedAt = &value
	}

	var salePrice *uint64
	if value := strings.TrimSpace(item.Discount); value != "" {
		if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
			salePrice = &parsed
		}
	}

	return bookmodel.NormalizeDocument(bookmodel.BookDocument{
		Provider:     "naver",
		Title:        item.Title,
		Contents:     item.Description,
		SourceURL:    item.Link,
		ThumbnailURL: item.Image,
		ISBNRaw:      item.ISBN,
		PublishedAt:  publishedAt,
		Authors:      bookmodel.SplitNames(item.Author),
		Publisher:    item.Publisher,
		SalePrice:    salePrice,
	})
}

var _ provider.SearchProvider = (*Adapter)(nil)
