package kakaostore

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type visibilityFake struct {
	queries []string
	pending bool
	rows    []map[string]any
	err     error
}

func (f *visibilityFake) QueryJSONEachRowContext(_ context.Context, query string) ([]map[string]any, error) {
	f.queries = append(f.queries, query)
	if f.err != nil {
		return nil, f.err
	}
	if strings.Contains(query, "replayed_at IS NULL") {
		if f.pending {
			return []map[string]any{{"pending": 1}}, nil
		}
		return nil, nil
	}
	return f.rows, nil
}

func TestSourceConfirmationRequiresDrainedOutboxAndExactRawRunWatermark(t *testing.T) {
	for _, condition := range []string{"confirmed", "pending", "missing", "wrong_watermark", "query_error", "malformed_count"} {
		f := &visibilityFake{rows: []map[string]any{{"confirmed_rows": 5, "confirmed_source_millis": int64(1000), "observed_millis": int64(2000)}}}
		switch condition {
		case "pending":
			f.pending = true
		case "missing":
			f.rows[0]["confirmed_rows"] = 4
		case "wrong_watermark":
			f.rows[0]["confirmed_source_millis"] = int64(999)
		case "query_error":
			f.err = errors.New("private provider credential")
		case "malformed_count":
			f.rows[0]["confirmed_rows"] = "not a number"
		}
		latest, observed, err := ConfirmCollectionSource(context.Background(), f, "Data_Book_KAKAO_Raw.kakao_book_raw", "Data_Book_KAKAO_Log.kakao_direct_insert_outbox", "01a00000-0000-7000-8000-000000000001", 5, 1000)
		if condition == "confirmed" && (err != nil || latest != 1000 || observed != 2000) || condition != "confirmed" && err == nil {
			t.Fatalf("condition=%s latest=%d observed=%d err=%v", condition, latest, observed, err)
		}
		if err != nil && strings.Contains(err.Error(), "credential") {
			t.Fatal("source error exposed transport detail")
		}
		if (condition == "pending" || condition == "query_error") && len(f.queries) != 1 {
			t.Fatalf("unconfirmed outbox proceeded to raw read: %v", f.queries)
		}
		for _, query := range f.queries {
			if !strings.HasPrefix(query, "SELECT ") || !strings.Contains(query, "max_threads=1,max_execution_time=") {
				t.Fatalf("unbounded or mutating source verification: %s", query)
			}
		}
	}
}
