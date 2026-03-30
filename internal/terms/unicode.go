package terms

import (
    "math/rand"
    "strings"
)

type weightedRuneRange struct {
    Start  rune
    End    rune
    Weight int
}

var defaultUnicodeRanges = []weightedRuneRange{
    // Korean books/titles dominate NAVER Book, so Hangul gets the highest weight.
    {Start: 0xAC00, End: 0xD7A3, Weight: 60}, // Hangul Syllables
    {Start: 0x3131, End: 0x318E, Weight: 5},  // Hangul Compatibility Jamo
    {Start: 0x4E00, End: 0x9FFF, Weight: 12}, // CJK Unified Ideographs
    {Start: 0x3041, End: 0x3096, Weight: 4},  // Hiragana
    {Start: 0x30A1, End: 0x30FA, Weight: 4},  // Katakana
    {Start: 0x0041, End: 0x005A, Weight: 8},  // Latin uppercase
    {Start: 0x0061, End: 0x007A, Weight: 8},  // Latin lowercase
    {Start: 0x0030, End: 0x0039, Weight: 2},  // ASCII digits
}

func randIntn(r *rand.Rand, n int) int {
    if n <= 0 {
        return 0
    }
    if r == nil {
        return rand.Intn(n)
    }
    return r.Intn(n)
}

func pickWeightedRange(r *rand.Rand, ranges []weightedRuneRange) weightedRuneRange {
    total := 0
    for _, rr := range ranges {
        if rr.Weight > 0 && rr.End >= rr.Start {
            total += rr.Weight
        }
    }
    if total <= 0 {
        return weightedRuneRange{Start: '가', End: '힣', Weight: 1}
    }
    n := randIntn(r, total)
    running := 0
    for _, rr := range ranges {
        if rr.Weight <= 0 || rr.End < rr.Start {
            continue
        }
        running += rr.Weight
        if n < running {
            return rr
        }
    }
    return ranges[len(ranges)-1]
}

func randomRuneFromRange(rr weightedRuneRange, r *rand.Rand) rune {
    width := int(rr.End-rr.Start) + 1
    if width <= 1 {
        return rr.Start
    }
    return rr.Start + rune(randIntn(r, width))
}

func RandomUnicodeTerm(termLength int, r *rand.Rand) string {
    if termLength <= 0 {
        termLength = 1
    }
    var b strings.Builder
    for i := 0; i < termLength; i++ {
        rr := pickWeightedRange(r, defaultUnicodeRanges)
        b.WriteRune(randomRuneFromRange(rr, r))
    }
    return b.String()
}

func RandomUnicodeTerms(batchSize, termLength int, r *rand.Rand) []string {
    if batchSize <= 0 {
        return nil
    }
    if termLength <= 0 {
        termLength = 1
    }
    seen := make(map[string]struct{}, batchSize)
    out := make([]string, 0, batchSize)
    maxAttempts := batchSize*50 + 100
    for attempts := 0; len(out) < batchSize && attempts < maxAttempts; attempts++ {
        term := strings.TrimSpace(RandomUnicodeTerm(termLength, r))
        if term == "" {
            continue
        }
        if _, ok := seen[term]; ok {
            continue
        }
        seen[term] = struct{}{}
        out = append(out, term)
    }
    return out
}
