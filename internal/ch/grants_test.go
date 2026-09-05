package ch

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestCheckTableGrantReadsScalarAndRejectsInvalidResults(t *testing.T) {
	for _, test := range []struct {
		name, body      string
		allowed, failed bool
	}{
		{"allowed", "1\n", true, false},
		{"denied_http_200", "0\n", false, false},
		{"empty", "", false, true},
		{"multiple", "1\n0\n", false, true},
		{"invalid", "2\n", false, true},
		{"unexpected_json", "{\"result\":1}\n", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &Client{Host: "database.invalid", HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(r.Body)
				if string(body) != "CHECK GRANT SELECT ON `data`.`books`" || r.URL.Query().Get("default_format") != "TabSeparated" {
					t.Fatal("grant query must use a scalar HTTP format without a SQL FORMAT clause")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(test.body)), Header: make(http.Header), Request: r}, nil
			})}}
			allowed, err := client.CheckTableGrantContext(context.Background(), "SELECT", "data.books")
			if allowed != test.allowed || (err != nil) != test.failed {
				t.Fatalf("allowed=%t err=%v", allowed, err)
			}
		})
	}
}

func TestCheckTableGrantRejectsUnsafeInputBeforeTransport(t *testing.T) {
	client := &Client{HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid input reached transport")
		return nil, nil
	})}}
	for _, input := range [][2]string{{"SELECT; DROP", "data.books"}, {"INSERT", "data.books; DROP"}, {"CREATE", "data.books"}} {
		if allowed, err := client.CheckTableGrantContext(context.Background(), input[0], input[1]); allowed || err == nil {
			t.Fatal("invalid privilege or target accepted")
		}
	}
}
