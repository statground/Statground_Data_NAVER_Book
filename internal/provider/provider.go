package provider

import (
	"context"
	"fmt"
	"strings"

	"statground_naver_book_go/internal/bookmodel"
)

type SearchRequest struct {
	Query  string
	Sort   string
	Target string
	Page   int
	Size   int
}

type SearchResponse struct {
	TotalCount    int
	PageableCount int
	IsEnd         bool
	Documents     []bookmodel.BookDocument
}

type SearchProvider interface {
	Name() string
	Search(context.Context, SearchRequest) (SearchResponse, error)
}

// NormalizeSearchRequest applies provider-neutral defaults and the common
// Kakao-compatible bounds used during the migration.
func NormalizeSearchRequest(request SearchRequest) (SearchRequest, error) {
	request.Query = strings.TrimSpace(request.Query)
	request.Sort = strings.ToLower(strings.TrimSpace(request.Sort))
	request.Target = strings.ToLower(strings.TrimSpace(request.Target))

	if request.Query == "" {
		return SearchRequest{}, fmt.Errorf("book search query is required")
	}
	if request.Sort == "" {
		request.Sort = "accuracy"
	}
	switch request.Sort {
	case "accuracy", "latest":
	default:
		return SearchRequest{}, fmt.Errorf("unsupported book search sort %q", request.Sort)
	}

	switch request.Target {
	case "", "title", "isbn", "publisher", "person":
	default:
		return SearchRequest{}, fmt.Errorf("unsupported book search target %q", request.Target)
	}

	if request.Page == 0 {
		request.Page = 1
	}
	if request.Size == 0 {
		request.Size = 10
	}
	if err := ValidateSearchRequest(request); err != nil {
		return SearchRequest{}, err
	}
	return request, nil
}

// ValidateSearchRequest validates a fully normalized request without applying
// defaults. Providers can normalize their own defaults and then share this
// allowlist and boundary check.
func ValidateSearchRequest(request SearchRequest) error {
	if strings.TrimSpace(request.Query) == "" {
		return fmt.Errorf("book search query is required")
	}
	switch strings.ToLower(strings.TrimSpace(request.Sort)) {
	case "accuracy", "latest":
	default:
		return fmt.Errorf("unsupported book search sort %q", request.Sort)
	}
	switch strings.ToLower(strings.TrimSpace(request.Target)) {
	case "", "title", "isbn", "publisher", "person":
	default:
		return fmt.Errorf("unsupported book search target %q", request.Target)
	}
	if request.Page < 1 || request.Page > 50 {
		return fmt.Errorf("book search page must be between 1 and 50")
	}
	if request.Size < 1 || request.Size > 50 {
		return fmt.Errorf("book search size must be between 1 and 50")
	}
	return nil
}
