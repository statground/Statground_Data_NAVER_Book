package bookmodel

import (
	"strings"
	"unicode"
)

// ISBNIdentity contains the validated identifiers extracted from an upstream
// provider's raw ISBN field.
type ISBNIdentity struct {
	Raw           string
	ISBN10        string
	ISBN13        string
	CanonicalISBN string
	Valid         bool
}

// NormalizeISBN extracts and validates ISBN-10 and ISBN-13 candidates from a
// provider value. A valid ISBN-13 takes precedence as the canonical value.
func NormalizeISBN(raw string) ISBNIdentity {
	result := ISBNIdentity{Raw: strings.TrimSpace(raw)}
	seen := make(map[string]struct{})

	for _, token := range isbnTokens(raw) {
		candidate := strings.ToUpper(strings.ReplaceAll(token, "-", ""))
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}

		switch {
		case result.ISBN13 == "" && validISBN13(candidate):
			result.ISBN13 = candidate
		case result.ISBN10 == "" && validISBN10(candidate):
			result.ISBN10 = candidate
		}
	}

	if result.ISBN13 != "" {
		result.CanonicalISBN = result.ISBN13
	} else {
		result.CanonicalISBN = result.ISBN10
	}
	result.Valid = result.CanonicalISBN != ""
	return result
}

func isbnTokens(raw string) []string {
	return strings.FieldsFunc(raw, func(r rune) bool {
		return !(unicode.IsDigit(r) || r == 'X' || r == 'x' || r == '-')
	})
}

func validISBN10(value string) bool {
	if len(value) != 10 {
		return false
	}

	sum := 0
	for i, r := range value {
		digit := 0
		switch {
		case r >= '0' && r <= '9':
			digit = int(r - '0')
		case i == 9 && r == 'X':
			digit = 10
		default:
			return false
		}
		sum += (10 - i) * digit
	}
	return sum%11 == 0
}

func validISBN13(value string) bool {
	if len(value) != 13 || (!strings.HasPrefix(value, "978") && !strings.HasPrefix(value, "979")) {
		return false
	}

	sum := 0
	for i, r := range value {
		if r < '0' || r > '9' {
			return false
		}
		digit := int(r - '0')
		if i == 12 {
			return digit == (10-sum%10)%10
		}
		if i%2 == 0 {
			sum += digit
		} else {
			sum += 3 * digit
		}
	}
	return false
}
