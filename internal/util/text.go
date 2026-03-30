package util

import (
	"fmt"
	"html"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var stripTagRe = regexp.MustCompile(`(?is)<[^>]+>`)

func StripHTML(s string) string {
	s = html.UnescapeString(s)
	s = stripTagRe.ReplaceAllString(s, " ")
	s = strings.Join(strings.Fields(s), " ")
	return strings.TrimSpace(s)
}

func EscapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func SQLString(s string) string {
	return "'" + EscapeSQLString(s) + "'"
}

func QuoteStringList(items []string) string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		out = append(out, SQLString(item))
	}
	return strings.Join(out, ",")
}

func KeysSorted(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func ToInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case uint:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		if x > ^uint64(0)>>1 {
			return 0
		}
		return int64(x)
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		x = strings.TrimSpace(x)
		if x == "" {
			return 0
		}
		if strings.ContainsAny(x, ".eE") {
			f, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return 0
			}
			return int64(f)
		}
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

func ToString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}
