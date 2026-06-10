package terms

import (
	"math/rand"
	"regexp"
	"strings"
	"unicode"

	"statground_naver_book_go/internal/util"
)

var (
	fallbackKeywords = []string{"통계", "데이터", "Statistics", "Data"}
	reMorphToken     = regexp.MustCompile(`[가-힣]+|[A-Za-z][A-Za-z0-9_\-']{2,}`)
	reKoreanOnly     = regexp.MustCompile(`^[가-힣]+$`)
	reSplitAuthor    = regexp.MustCompile(`\^+`)
	reHangul         = regexp.MustCompile(`[가-힣]`)
)

var koreanKeywordStopwords = map[string]struct{}{
	"개정판": {}, "공식": {}, "교재": {}, "기본서": {}, "기초": {}, "대한민국": {},
	"문제": {}, "문제집": {}, "베스트": {}, "북스": {}, "비법": {}, "사전": {},
	"세트": {}, "시리즈": {}, "실전": {}, "완벽": {}, "워크북": {}, "입문": {},
	"최신": {}, "최신판": {}, "특별판": {}, "편집부": {}, "한국": {}, "핸드북": {},
	"활용": {},
}

var broadSingleKeywordStopwords = map[string]struct{}{
	"과학": {}, "교육": {}, "기술": {}, "데이터": {}, "문명": {}, "분석": {},
	"세계": {}, "역사": {}, "연구": {}, "이론": {}, "이야기": {}, "지식": {},
	"통계": {}, "학습": {},
}

var englishKeywordStopwords = map[string]struct{}{
	"book": {}, "books": {}, "edition": {}, "guide": {}, "handbook": {},
	"introduction": {}, "manual": {}, "primer": {}, "series": {}, "using": {},
	"with": {}, "workbook": {},
}

var genericEnglishPublisherSeeds = map[string]struct{}{
	"book": {}, "books": {}, "company": {}, "group": {}, "inc": {}, "llc": {},
	"ltd": {}, "media": {}, "press": {}, "pub": {}, "publisher": {},
	"publishing": {},
}

var koreanKeywordSuffixes = []string{
	"으로부터", "으로써", "으로서", "에서", "에게", "부터", "까지", "처럼",
	"보다", "으로", "라고", "라는", "하고", "하며", "하게", "하기", "하는",
	"한다", "했다", "이며", "이고", "은", "는", "이", "가", "을", "를",
	"의", "에", "도", "만", "와", "과", "로",
}

type morphToken struct {
	Text   string
	Script string
}

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
	return ExtractMorphKeywordsFromText(title)
}

func ExtractMorphKeywordsFromText(text string) []string {
	tokens := morphTokensFromText(text)
	if len(tokens) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for i := 0; i+2 < len(tokens); i++ {
		left := tokens[i]
		mid := tokens[i+1]
		right := tokens[i+2]
		if left.Script != mid.Script || mid.Script != right.Script {
			continue
		}
		addKeyword(&out, seen, left.Text+" "+mid.Text+" "+right.Text)
	}
	for i := 0; i+1 < len(tokens); i++ {
		left := tokens[i]
		right := tokens[i+1]
		if left.Script != right.Script {
			continue
		}
		addKeyword(&out, seen, left.Text+" "+right.Text)
	}
	for _, token := range tokens {
		addKeyword(&out, seen, token.Text)
	}
	return out
}

func ExtractKeywordsFromBookRows(rows []map[string]any) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, row := range rows {
		text := strings.TrimSpace(strings.Join([]string{
			util.ToString(row["title"]),
			util.ToString(row["description"]),
		}, " "))
		for _, token := range ExtractMorphKeywordsFromText(text) {
			addKeyword(&out, seen, token)
		}
	}
	return out
}

func morphTokensFromText(text string) []morphToken {
	text = util.StripHTML(text)
	if strings.TrimSpace(text) == "" {
		return nil
	}
	out := make([]morphToken, 0)
	for _, raw := range reMorphToken.FindAllString(text, -1) {
		if reKoreanOnly.MatchString(raw) {
			token := cleanKoreanKeyword(raw)
			if token == "" {
				continue
			}
			out = append(out, morphToken{Text: token, Script: "ko"})
			continue
		}
		token := cleanEnglishKeyword(raw)
		if token == "" {
			continue
		}
		out = append(out, morphToken{Text: token, Script: "en"})
	}
	return out
}

func cleanKoreanKeyword(token string) string {
	token = strings.TrimSpace(token)
	for _, suffix := range koreanKeywordSuffixes {
		if strings.HasSuffix(token, suffix) && len([]rune(token)) > len([]rune(suffix))+1 {
			token = strings.TrimSuffix(token, suffix)
			break
		}
	}
	if len([]rune(token)) < 2 {
		return ""
	}
	if _, ok := koreanKeywordStopwords[token]; ok {
		return ""
	}
	return token
}

func cleanEnglishKeyword(token string) string {
	token = strings.Trim(strings.ToLower(strings.TrimSpace(token)), "-_'")
	if len(token) < 4 {
		return ""
	}
	if _, ok := englishKeywordStopwords[token]; ok {
		return ""
	}
	return token
}

func addKeyword(out *[]string, seen map[string]struct{}, token string) {
	token = strings.Join(strings.Fields(strings.TrimSpace(token)), " ")
	if token == "" {
		return
	}
	if !strings.Contains(token, " ") {
		if _, ok := broadSingleKeywordStopwords[token]; ok {
			return
		}
	}
	if _, ok := seen[token]; ok {
		return
	}
	seen[token] = struct{}{}
	*out = append(*out, token)
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

func NormalizePublisher(value string) string {
	value = util.StripHTML(value)
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	return value
}

func IsPublisherSearchCandidate(value string) bool {
	value = NormalizePublisher(value)
	if len([]rune(value)) < 2 {
		return false
	}
	compact := compactPublisherToken(value)
	if compact == "" {
		return false
	}
	if reHangul.MatchString(value) {
		return true
	}
	if len([]rune(compact)) <= 2 {
		return false
	}
	if _, ok := genericEnglishPublisherSeeds[compact]; ok {
		return false
	}
	return true
}

func compactPublisherToken(value string) string {
	var b strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

func ExtractPublishers(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, value := range values {
		value = NormalizePublisher(value)
		if !IsPublisherSearchCandidate(value) {
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
		return SanitizeKeyword(RandomChoice(ExtractKeywordsFromBookRows(sampleRows), r), r)
	}
}

func PickUniqueTerms(mode string, batchSize int, sampleRows []map[string]any, r *rand.Rand) []string {
	if batchSize <= 0 {
		return nil
	}
	if len(sampleRows) == 0 {
		return nil
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
		pool = ExtractKeywordsFromBookRows(sampleRows)
	}
	if len(pool) == 0 {
		return nil
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
