package terms

import (
	"math/rand"
	"regexp"
	"strings"

	"statground_naver_book_go/internal/util"
)

var (
	fallbackKeywords = []string{"통계", "데이터", "Statistics", "Data"}
	reKoreanToken    = regexp.MustCompile(`[가-힣]{2,}`)
	reEnglishToken   = regexp.MustCompile(`[A-Za-z][A-Za-z0-9_\-']{3,}`)
	reSplitAuthor    = regexp.MustCompile(`\^+`)
)

func Fallback() []string {
	out := make([]string, len(fallbackKeywords))
	copy(out, fallbackKeywords)
	return out
}

func SanitizeKeyword(keyword string, r *rand.Rand) string {
	keyword = strings.TrimSpace(keyword)
	if keyword != "" {
		return keyword
	}
	return RandomFallback(r)
}

func RandomFallback(r *rand.Rand) string {
	if r == nil {
		return fallbackKeywords[rand.Intn(len(fallbackKeywords))]
	}
	return fallbackKeywords[r.Intn(len(fallbackKeywords))]
}

func ExtractKeywordsFromTitle(title string) []string {
	title = util.StripHTML(title)
	if strings.TrimSpace(title) == "" {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, token := range reKoreanToken.FindAllString(title, -1) {
		token = strings.TrimSpace(token)
		if len([]rune(token)) < 2 {
			continue
		}
		if _, ok := seen[token]; !ok {
			seen[token] = struct{}{}
			out = append(out, token)
		}
	}
	for _, token := range reEnglishToken.FindAllString(title, -1) {
		token = strings.TrimSpace(token)
		if len(token) < 4 {
			continue
		}
		if _, ok := seen[token]; !ok {
			seen[token] = struct{}{}
			out = append(out, token)
		}
	}
	return out
}

func ExtractKeywordsFromTitles(titles []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, title := range titles {
		for _, token := range ExtractKeywordsFromTitle(title) {
			if _, ok := seen[token]; ok {
				continue
			}
			seen[token] = struct{}{}
			out = append(out, token)
		}
	}
	return out
}

func ExtractAuthors(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, value := range values {
		value = util.StripHTML(value)
		if strings.TrimSpace(value) == "" {
			continue
		}
		for _, part := range reSplitAuthor.Split(value, -1) {
			part = strings.TrimSpace(part)
			if len([]rune(part)) < 2 {
				continue
			}
			if _, ok := seen[part]; ok {
				continue
			}
			seen[part] = struct{}{}
			out = append(out, part)
		}
	}
	return out
}

func ExtractPublishers(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, value := range values {
		value = util.StripHTML(value)
		value = strings.TrimSpace(value)
		if len([]rune(value)) < 2 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func RandomChoice(items []string, r *rand.Rand) string {
	if len(items) == 0 {
		return RandomFallback(r)
	}
	if r == nil {
		return items[rand.Intn(len(items))]
	}
	return items[r.Intn(len(items))]
}

func GenerateKeyword(sampleRows []map[string]any, r *rand.Rand) string {
	if len(sampleRows) == 0 {
		return RandomFallback(r)
	}
	sources := []string{"title", "author", "publisher"}
	source := RandomChoice(sources, r)
	switch source {
	case "author":
		authors := make([]string, 0, len(sampleRows))
		for _, row := range sampleRows {
			authors = append(authors, util.ToString(row["author"]))
		}
		return SanitizeKeyword(RandomChoice(ExtractAuthors(authors), r), r)
	case "publisher":
		publishers := make([]string, 0, len(sampleRows))
		for _, row := range sampleRows {
			publishers = append(publishers, util.ToString(row["publisher"]))
		}
		return SanitizeKeyword(RandomChoice(ExtractPublishers(publishers), r), r)
	default:
		titles := make([]string, 0, len(sampleRows))
		for _, row := range sampleRows {
			titles = append(titles, util.ToString(row["title"]))
		}
		return SanitizeKeyword(RandomChoice(ExtractKeywordsFromTitles(titles), r), r)
	}
}

func PickUniqueTerms(mode string, batchSize int, sampleRows []map[string]any, r *rand.Rand) []string {
	if batchSize <= 0 {
		return nil
	}
	if len(sampleRows) == 0 {
		fallbacks := Fallback()
		if batchSize < len(fallbacks) {
			return fallbacks[:batchSize]
		}
		return fallbacks
	}

	mode = strings.ToLower(strings.TrimSpace(mode))
	var pool []string
	switch mode {
	case "author":
		items := make([]string, 0, len(sampleRows))
		for _, row := range sampleRows {
			items = append(items, util.ToString(row["author"]))
		}
		pool = ExtractAuthors(items)
	case "publisher":
		items := make([]string, 0, len(sampleRows))
		for _, row := range sampleRows {
			items = append(items, util.ToString(row["publisher"]))
		}
		pool = ExtractPublishers(items)
	default:
		items := make([]string, 0, len(sampleRows))
		for _, row := range sampleRows {
			items = append(items, util.ToString(row["title"]))
		}
		pool = ExtractKeywordsFromTitles(items)
	}
	if len(pool) == 0 {
		pool = Fallback()
	}
	shuffled := make([]string, len(pool))
	copy(shuffled, pool)
	if r != nil {
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	}
	if batchSize < len(shuffled) {
		shuffled = shuffled[:batchSize]
	}
	for i := range shuffled {
		shuffled[i] = SanitizeKeyword(shuffled[i], r)
	}
	return shuffled
}
