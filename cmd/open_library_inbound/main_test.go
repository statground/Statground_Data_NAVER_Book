package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestEditionDumpMatchesExactISBNAndRetainsProviderIdentity(t *testing.T) {
	requested, err := candidates("1491910399,9781491910399\n9781492097402")
	if err != nil {
		t.Fatal(err)
	}
	dump := strings.Join([]string{
		`/type/edition	/books/OL123M	3	2026-09-12T10:20:00.000000	{"key": "/books/OL123M", "revision": 3, "isbn_10": ["1491910399"], "isbn_13": ["9781491910399"], "title": "R & data <analysis>"}`,
		`/type/edition	/books/OL456M	1	2026-09-12T10:20:00.000000	{"key":"/books/OL456M","revision":1,"isbn_13":["9781491910399"],"title":"Another record"}`,
		`/type/edition	/books/OL789M	1	2026-09-12T10:20:00.000000	{"key":"/books/OL789M","revision":1,"isbn_13":["9781491910398"],"title":"Invalid ISBN"}`,
	}, "\n")
	var out bytes.Buffer
	report := receipt{Provider: "open-library", SnapshotDate: "2026-10-31"}
	if err := scanDump(strings.NewReader(dump), &out, requested, &report, 0); err != nil {
		t.Fatal(err)
	}
	if report.RowsScanned != 3 || report.Editions != 2 || !requested["9781491910399"] || requested["9781492097402"] {
		t.Fatalf("incorrect coverage: %+v %v", report, requested)
	}
	decoder := json.NewDecoder(&out)
	for index, key := range []string{"/books/OL123M", "/books/OL456M"} {
		var got matchedEdition
		if err := decoder.Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.EditionKey != key || got.SourceSnapshot != "2026-10-31" ||
			len(got.MatchedISBN13) != 1 || len(got.SourceSHA256) != 64 || !json.Valid(got.SourceRecord) {
			t.Fatalf("incorrect edition: %+v", got)
		}
		exactRow := strings.Split(dump, "\n")[index]
		digest := sha256.Sum256(got.SourceRow)
		if string(got.SourceRow) != exactRow || hex.EncodeToString(digest[:]) != got.SourceSHA256 {
			t.Fatalf("edition %s raw row or checksum differs", key)
		}
	}
}

func TestProbeValidatesRowsWithoutEmittingRecords(t *testing.T) {
	requested, err := candidates("9781491910399")
	if err != nil {
		t.Fatal(err)
	}
	row := `/type/edition	/books/OL123M	3	2026-09-12T10:20:00.000000	{"key":"/books/OL123M","revision":3,"isbn_13":["9781491910399"]}`
	var out bytes.Buffer
	report := receipt{}
	if err := scanDump(strings.NewReader(row+"\nmalformed"), &out, requested, &report, 1); err != nil {
		t.Fatal(err)
	}
	if report.RowsScanned != 1 || report.Editions != 0 || out.Len() != 0 {
		t.Fatalf("probe wrote records: %+v", report)
	}
}

func TestInconsistentEditionIdentityWithholdsValidReceipt(t *testing.T) {
	requested, err := candidates("9781491910399")
	if err != nil {
		t.Fatal(err)
	}
	row := `/type/edition	/books/OL123M	3	2026-09-12T10:20:00.000000	{"key":"/books/OL456M","revision":3,"isbn_13":["9781491910399"]}`
	var out bytes.Buffer
	if err := scanDump(strings.NewReader(row), &out, requested, &receipt{}, 0); err == nil || out.Len() != 0 {
		t.Fatal("unexpected approval of mismatched edition key")
	}
}
