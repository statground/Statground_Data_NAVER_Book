package terms

import (
	"math/rand"
	"testing"
)

func TestPickUniqueTermsUsesExistingBookMorphKeywords(t *testing.T) {
	rows := []map[string]any{
		{
			"title":       "어린이를 위한 세계사 여행",
			"description": "고대 문명과 역사 이야기를 다룬 책",
		},
	}

	got := PickUniqueTerms("keyword", 10, rows, rand.New(rand.NewSource(1)))
	if len(got) == 0 {
		t.Fatal("expected keyword terms from sampled book rows")
	}
	if contains(got, "데이터") || contains(got, "통계") || contains(got, "Data") || contains(got, "Statistics") {
		t.Fatalf("keyword terms should not fall back to static data terms: %v", got)
	}
	if !containsAny(got, "세계사", "세계사 여행", "고대 문명", "역사 이야기") {
		t.Fatalf("expected morph-like terms from book title/description, got %v", got)
	}
}

func TestPickUniqueTermsSkipsKeywordWithoutExistingRows(t *testing.T) {
	got := PickUniqueTerms("keyword", 10, nil, rand.New(rand.NewSource(1)))
	if len(got) != 0 {
		t.Fatalf("expected no static fallback terms when sample rows are unavailable, got %v", got)
	}
}

func TestExtractMorphKeywordsStripsKoreanParticles(t *testing.T) {
	got := ExtractMorphKeywordsFromText("인공지능으로 배우는 한국어 문법")
	if contains(got, "인공지능으로") {
		t.Fatalf("expected particle-stripped token, got %v", got)
	}
	if !contains(got, "인공지능") {
		t.Fatalf("expected stripped token 인공지능, got %v", got)
	}
	if !contains(got, "한국어 문법") {
		t.Fatalf("expected adjacent morph phrase 한국어 문법, got %v", got)
	}
}

func TestExtractMorphKeywordsKeepsPhrasesBeforeBroadSingles(t *testing.T) {
	got := ExtractMorphKeywordsFromText("데이터 분석 실무와 통계 이야기")
	if contains(got, "데이터") || contains(got, "분석") || contains(got, "통계") {
		t.Fatalf("expected broad single words to be skipped, got %v", got)
	}
	if !contains(got, "데이터 분석") {
		t.Fatalf("expected specific adjacent phrase 데이터 분석, got %v", got)
	}
	if !contains(got, "데이터 분석 실무") {
		t.Fatalf("expected specific three-token phrase 데이터 분석 실무, got %v", got)
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func containsAny(items []string, wants ...string) bool {
	for _, want := range wants {
		if contains(items, want) {
			return true
		}
	}
	return false
}
