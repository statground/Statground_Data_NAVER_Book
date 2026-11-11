package collector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/writerlease"
)

type ackTransport func(*http.Request) (*http.Response, error)

func (f ackTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type ackAdmission struct{}

func (ackAdmission) Assert(context.Context) error { return nil }
func (ackAdmission) Close() error                 { return nil }
func (ackAdmission) Begin(ctx context.Context, _ writerlease.Intent) (context.Context, writerlease.Operation, error) {
	return ctx, ackOperation{}, nil
}

type ackOperation struct{}

func (ackOperation) Confirm(context.Context) error { return nil }
func (ackOperation) Abandon()                      {}

// The real legacy NAVER producers share one exact client across Collectors.
// Their source-ready boundary is command success, not Kakao's separate receipt.
func TestActualCollectorsFinishSharedRawLogCacheMarkersBeforeCommandSuccess(t *testing.T) {
	t.Setenv("EXISTING_ISBN_LOOKUP_ENABLED", "false")
	t.Setenv("NAVER_OUTBOX_ACK_BATCH_SIZE", "25")
	for _, markerUnknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "ack", true: "unknown"}[markerUnknown], func(t *testing.T) {
			outbox, targets, markers, confirmed := 0, 0, 0, false
			client := &ch.Client{Host: "http://fixture.invalid", Database: "Data_Book_NAVER_Raw", WriterAdmission: ackAdmission{}, HTTPClient: &http.Client{Timeout: time.Second, Transport: ackTransport(func(r *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return nil, err
				}
				text := string(body)
				switch {
				case strings.HasPrefix(strings.TrimSpace(text), "ALTER TABLE"):
					markers++
					if strings.Count(text, "toUUID(") != 3 || !strings.Contains(text, "mutations_sync = 2") {
						return nil, errors.New("wrong bounded marker")
					}
					if markerUnknown {
						return nil, errors.New("marker acknowledgement lost")
					}
					confirmed = true
				case strings.Contains(strings.SplitN(text, "\n", 2)[0], "naver_direct_insert_outbox"):
					outbox++
				case strings.HasPrefix(text, "INSERT INTO"):
					targets++
					if outbox != targets {
						return nil, errors.New("target preceded outbox")
					}
				default:
					return nil, errors.New("unexpected query")
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
			})}}
			err := ch.RunWriterCommand(func() error {
				first, err := New(client, "naver_book_raw", nil, 1)
				if err != nil {
					return err
				}
				second, err := New(client, "naver_book_raw", nil, 2)
				if err != nil {
					return err
				}
				meta := SearchMeta{Mode: "fixture", Query: "fixture", Sort: "sim", Start: 1, Display: 1, Total: 1}
				if err := first.upsertItems([]naver.BookItem{{Title: "fixture", ISBN: "9780000000001"}}, "fixture", "fixture", false, meta); err != nil {
					return err
				}
				if err := second.publishSearchLogWithTimeout(meta, "success", 1, "", "fixture", 250*time.Millisecond); err != nil {
					return err
				}
				if err := first.Ingest.InsertPublisherCacheRows([]map[string]any{{"publisher": "fixture"}}); err != nil {
					return err
				}
				if markers != 0 || confirmed {
					t.Fatal("small group did not remain pending until success barrier")
				}
				return nil
			})
			if (err != nil) != markerUnknown || outbox != 3 || targets != 3 || markers != 1 || confirmed == markerUnknown {
				t.Fatalf("err=%v outbox=%d targets=%d markers=%d confirmed=%t", err, outbox, targets, markers, confirmed)
			}
			if err == nil && !confirmed {
				t.Fatal("command success escaped before positive marker ACK")
			}
		})
	}
}
