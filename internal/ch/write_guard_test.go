package ch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"statground_naver_book_go/internal/writerlease"
)

type recordingAdmission struct {
	err       error
	begun     int
	confirmed int
	abandoned int
	intent    writerlease.Intent
	cancel    context.CancelFunc
}

func (a *recordingAdmission) Assert(context.Context) error { return a.err }
func (a *recordingAdmission) Close() error                 { return nil }
func (a *recordingAdmission) Begin(ctx context.Context, intent writerlease.Intent) (context.Context, writerlease.Operation, error) {
	a.begun++
	a.intent = intent
	if a.err != nil {
		return nil, nil, a.err
	}
	child, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	return child, &recordingOperation{admission: a}, nil
}

type recordingOperation struct {
	admission *recordingAdmission
	done      bool
}

func (o *recordingOperation) Confirm(context.Context) error {
	if o.done {
		return errors.New("already terminal")
	}
	o.done = true
	o.admission.confirmed++
	o.admission.cancel()
	return nil
}
func (o *recordingOperation) Abandon() {
	if !o.done {
		o.done = true
		o.admission.abandoned++
		o.admission.cancel()
	}
}

type interruptedBody struct{}

func (interruptedBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (interruptedBody) Close() error             { return nil }

func TestMutationRequiresPositiveSynchronousAcknowledgement(t *testing.T) {
	for _, kind := range []string{"empty", "exception_body", "exception_header", "truncated", "transport", "lease_lost", "nonempty"} {
		t.Run(kind, func(t *testing.T) {
			admission := &recordingAdmission{}
			requests := 0
			client := &Client{Host: "http://database.test", WriterAdmission: admission, HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				q := req.URL.Query()
				for name, want := range map[string]string{"async_insert": "0", "insert_distributed_sync": "1", "mutations_sync": "2", "wait_end_of_query": "1", "materialized_views_ignore_errors": "0"} {
					if q.Get(name) != want {
						t.Fatalf("%s=%q", name, q.Get(name))
					}
				}
				if q.Get("query_id") != admission.intent.QueryID || q.Get("query_id") == "" {
					t.Fatal("query identity missing")
				}
				payload, _ := io.ReadAll(req.Body)
				sum := sha256.Sum256(payload)
				if admission.intent.RequestSHA256 != hex.EncodeToString(sum[:]) || admission.intent.Target != "Data_Book_NLK_Raw.nlk_resource_raw" {
					t.Fatal("durable identity does not describe request")
				}
				if kind == "transport" {
					return nil, errors.New("connection lost")
				}
				response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: req}
				switch kind {
				case "exception_body":
					response.Body = io.NopCloser(strings.NewReader("Code: 734. late query failure"))
				case "exception_header":
					response.Header.Set("X-ClickHouse-Exception-Code", "734")
				case "truncated":
					response.Body = interruptedBody{}
				case "nonempty":
					response.Body = io.NopCloser(strings.NewReader(" "))
				case "lease_lost":
					admission.cancel()
				}
				return response, nil
			})}}
			err := client.ExecContext(context.Background(), "INSERT INTO Data_Book_NLK_Raw.nlk_resource_raw SELECT 1")
			if kind == "empty" {
				if err != nil || admission.confirmed != 1 || admission.abandoned != 0 {
					t.Fatalf("err=%v confirmed=%d abandoned=%d", err, admission.confirmed, admission.abandoned)
				}
			} else if err == nil || admission.confirmed != 0 || admission.abandoned != 1 {
				t.Fatalf("uncertain request finished: err=%v confirmed=%d abandoned=%d", err, admission.confirmed, admission.abandoned)
			}
			if requests != 1 || admission.begun != 1 {
				t.Fatalf("requests=%d begin=%d", requests, admission.begun)
			}
		})
	}
}

func TestAdmissionFailureDoesNotIssueClickHouseRequest(t *testing.T) {
	a := &recordingAdmission{err: errors.New("pending operation")}
	requests := 0
	c := &Client{Host: "http://database.test", WriterAdmission: a, HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { requests++; return nil, errors.New("must not request") })}}
	if c.Exec("INSERT INTO Data_Book_Service.book_provider_latest SELECT 1") == nil || requests != 0 {
		t.Fatalf("requests=%d", requests)
	}
}

func TestSQLMutationClassificationIsFailClosed(t *testing.T) {
	cases := []struct {
		sql, target string
		reject      bool
	}{
		{`INSERT INTO db.target SELECT 1`, "db.target", false},
		{"INSERT INTO `db`.`target` (value) FORMAT JSONEachRow\n{\"value\":\"SELECT; DROP TABLE db.target FORMAT JSONEachRow\"}", "db.target", false},
		{`INSERT INTO db.target SELECT 'FORMAT JSONEachRow' SETTINGS async_insert = 1`, "", true},
		{`INSERT INTO db.target SELECT 1 SETTINGS async_insert=0+1`, "", true},
		{`INSERT INTO db.target SELECT 1 SETTINGS async_insert='0'`, "", true},
		{`INSERT INTO db.target SELECT 1 SETTINGS async_insert=0x0`, "", true},
		{`INSERT INTO db.target SELECT 1 SETTINGS materialized_views_ignore_errors=1`, "", true},
		{`INSERT INTO db.target SELECT 1 SETTINGS mutations_sync=1`, "", true},
		{`INSERT INTO db.target SELECT 1 SETTINGS async_insert=0, mutations_sync=2`, "db.target", false},
		{`ALTER TABLE db.outbox UPDATE replayed_at=now() WHERE 1 SETTINGS mutations_sync=2`, "db.outbox", false},
		{`ALTER TABLE db.target DELETE WHERE 1`, "", true},
		{`SELECT 1 FORMAT JSONEachRow; INSERT INTO db.target SELECT 1`, "", true},
		{`WITH 'INSERT' AS value SELECT value FORMAT JSONEachRow`, "", false},
		{"WITH 1 AS `SELECT` INSERT INTO db.target SELECT 1", "db.target", false},
		{`CREATE TABLE db.target (v UInt8)`, "", true},
		{`SYSTEM REFRESH VIEW Data_Book_Service.mv_book_catalog_latest_refresh`, "", false},
		{`SYSTEM START VIEW mirtype_book.mv_naver_language_book_catalog_refresh`, "", false},
		{`SYSTEM WAIT VIEW db.other`, "", false},
		{`SYSTEM REFRESH VIEW db.other`, "", true},
		{`SYSTEM START VIEW db.other`, "", true},
		{`SYSTEM WAIT VIEW db.other EXTRA`, "", true},
		{`SYSTEM STOP VIEWS`, "", true},
		{`CHECK GRANT SELECT, INSERT ON db.target`, "", false},
	}
	for _, test := range cases {
		t.Run(test.sql, func(t *testing.T) {
			target, err := mutationTarget(test.sql, "db")
			if (err != nil) != test.reject || target != test.target {
				t.Fatalf("target=%q error=%v", target, err)
			}
		})
	}
}

func TestReadOnlyRequestDoesNotRequireWriterControlPlane(t *testing.T) {
	t.Setenv("BOOK_WRITER_LEASE_HELPER", "")
	t.Setenv("BOOK_WRITER_LEASE_CONFIG", "")
	c := &Client{Host: "http://database.test", HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{\"value\":1}\n")), Request: req}, nil
	})}}
	rows, err := c.QueryJSONEachRow("SELECT 1 AS value")
	if err != nil || len(rows) != 1 {
		t.Fatalf("read-only rows=%v err=%v", rows, err)
	}
}
