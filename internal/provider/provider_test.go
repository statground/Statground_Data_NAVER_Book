package provider

import "testing"

func TestNormalizeSearchRequestDefaultsAndTrims(t *testing.T) {
	got, err := NormalizeSearchRequest(SearchRequest{Query: " R language "})
	if err != nil {
		t.Fatalf("NormalizeSearchRequest returned error: %v", err)
	}
	if got.Query != "R language" || got.Sort != "accuracy" || got.Page != 1 || got.Size != 10 || got.Target != "" {
		t.Fatalf("unexpected normalized request: %#v", got)
	}
}

func TestNormalizeSearchRequestAllowlistAndBounds(t *testing.T) {
	for _, request := range []SearchRequest{
		{Query: ""},
		{Query: "R", Sort: "date"},
		{Query: "R", Target: "author"},
		{Query: "R", Page: -1},
		{Query: "R", Page: 51},
		{Query: "R", Size: -1},
		{Query: "R", Size: 51},
	} {
		if _, err := NormalizeSearchRequest(request); err == nil {
			t.Fatalf("expected validation error for %#v", request)
		}
	}

	for _, target := range []string{"", "title", "isbn", "publisher", "person"} {
		if _, err := NormalizeSearchRequest(SearchRequest{
			Query:  "R",
			Sort:   "latest",
			Target: target,
			Page:   50,
			Size:   1,
		}); err != nil {
			t.Fatalf("valid target %q rejected: %v", target, err)
		}
	}
}
