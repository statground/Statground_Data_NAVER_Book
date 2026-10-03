package util

import (
	"encoding/json"
	"math"
	"strconv"
)

// NonnegativeInteger rejects malformed/fractional metadata instead of
// interpreting it as an empty source or a successful refresh epoch.
func NonnegativeInteger(value any) (int64, bool) {
	switch n := value.(type) {
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n >= float64(math.MaxInt64) || math.Trunc(n) != n {
			return 0, false
		}
		return int64(n), true
	case int:
		return int64(n), n >= 0
	case int64:
		return n, n >= 0
	case uint64:
		return int64(n), n <= math.MaxInt64
	case string:
		parsed, err := strconv.ParseInt(n, 10, 64)
		return parsed, err == nil && parsed >= 0
	case json.Number:
		parsed, err := n.Int64()
		return parsed, err == nil && parsed >= 0
	default:
		return 0, false
	}
}
