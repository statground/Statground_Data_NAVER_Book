package bookmodel

import (
	"strings"
	"testing"
)

func TestNormalizeISBNGuideCases(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		want10    string
		want13    string
		canonical string
		valid     bool
	}{
		{
			name:      "ISBN-10 only",
			raw:       "0306406152",
			want10:    "0306406152",
			canonical: "0306406152",
			valid:     true,
		},
		{
			name:      "ISBN-13 only",
			raw:       "9780306406157",
			want13:    "9780306406157",
			canonical: "9780306406157",
			valid:     true,
		},
		{
			name:      "ISBN-10 and ISBN-13 separated by space",
			raw:       "0306406152 9780306406157",
			want10:    "0306406152",
			want13:    "9780306406157",
			canonical: "9780306406157",
			valid:     true,
		},
		{
			name:      "hyphenated values",
			raw:       "0-306-40615-2 / 978-0-306-40615-7",
			want10:    "0306406152",
			want13:    "9780306406157",
			canonical: "9780306406157",
			valid:     true,
		},
		{
			name:      "isbn prefix",
			raw:       "isbn=9780306406157",
			want13:    "9780306406157",
			canonical: "9780306406157",
			valid:     true,
		},
		{
			name:      "ISBN-10 X checksum",
			raw:       "ISBN-10: 0-8044-2957-X",
			want10:    "080442957X",
			canonical: "080442957X",
			valid:     true,
		},
		{
			name:  "invalid checksums",
			raw:   "0306406153 9780306406158",
			valid: false,
		},
		{
			name:  "empty",
			raw:   " \t\n ",
			valid: false,
		},
		{
			name:      "duplicate token",
			raw:       "9780306406157 978-0-306-40615-7 9780306406157",
			want13:    "9780306406157",
			canonical: "9780306406157",
			valid:     true,
		},
		{
			name:  "unrelated numeric token",
			raw:   "order=202607260001 sequence=123456789",
			valid: false,
		},
		{
			name:      "lowercase X",
			raw:       "080442957x",
			want10:    "080442957X",
			canonical: "080442957X",
			valid:     true,
		},
		{
			name:      "invalid candidate does not hide later valid value",
			raw:       "9780306406158; isbn=9780306406157",
			want13:    "9780306406157",
			canonical: "9780306406157",
			valid:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeISBN(tt.raw)
			if got.Raw != strings.TrimSpace(tt.raw) {
				t.Fatalf("Raw = %q, want %q", got.Raw, strings.TrimSpace(tt.raw))
			}
			if got.ISBN10 != tt.want10 {
				t.Fatalf("ISBN10 = %q, want %q", got.ISBN10, tt.want10)
			}
			if got.ISBN13 != tt.want13 {
				t.Fatalf("ISBN13 = %q, want %q", got.ISBN13, tt.want13)
			}
			if got.CanonicalISBN != tt.canonical {
				t.Fatalf("CanonicalISBN = %q, want %q", got.CanonicalISBN, tt.canonical)
			}
			if got.Valid != tt.valid {
				t.Fatalf("Valid = %t, want %t", got.Valid, tt.valid)
			}
		})
	}
}
