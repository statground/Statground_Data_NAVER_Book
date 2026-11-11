package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/bookpipeline"
	"statground_naver_book_go/internal/bookrefreshreceipt"
	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/kakaostore"
)

func TestRealProducerCompletionRegistersOnlyConfirmedSourceAndPreservesFailure(t *testing.T) {
	for _, brokerFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(brokerFailure), func(t *testing.T) {
			dir := privateTestDir(t)
			receipt := filepath.Join(dir, "collection.json")
			t.Setenv("KAKAO_COLLECTION_RECEIPT_FILE", receipt)
			t.Setenv("BOOK_PIPELINE_MODE", "broker")
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/internal/book-pipeline/v1/source-batches" {
					calls++
					if brokerFailure {
						w.WriteHeader(503)
						return
					}
					var req bookpipeline.Registration
					json.NewDecoder(r.Body).Decode(&req)
					json.NewEncoder(w).Encode(bookpipeline.RegistrationResult{Schema: "book.pipeline.source-registration-result.v1", Status: "registered", Durable: true, Environment: "test", BatchID: req.BatchID, SourceVersion: req.SourceVersion, SourceSHA256: req.SourceSHA256, EventID: strings.Repeat("c", 64), ReceiptReference: "/server/retained.json"})
					return
				}
				// Explicit local ClickHouse transport model; actual producer function
				// and its original source-confirmation queries execute unchanged.
				body, _ := io.ReadAll(r.Body)
				if strings.Contains(string(body), "replayed_at IS NULL") {
					return
				}
				fmt.Fprintln(w, `{"confirmed_rows":2,"confirmed_source_millis":1500,"observed_millis":2000}`)
			}))
			defer server.Close()
			u, _ := url.Parse(server.URL)
			port, _ := strconv.Atoi(u.Port())
			client := ch.New(u.Hostname(), port, "fixture", "test-only", "Data_Book_KAKAO_Raw")
			store := &kakaostore.ClickHouseStore{Client: client, Config: kakaostore.Config{RawTable: "Data_Book_KAKAO_Raw.kakao_book_raw", OutboxTable: "Data_Book_KAKAO_Log.kakao_direct_insert_outbox"}}
			p := bookpipeline.Profile{Schema: "book.pipeline.registrar-profile.v1", Endpoint: server.URL + "/internal/book-pipeline/v1/source-batches", Username: "fixture", Password: "test-only", Environment: "test", AuthorityRevision: 1, DurableDirectory: dir, AllowLoopbackHTTP: true}
			profileRaw, _ := json.Marshal(p)
			profilePath := filepath.Join(dir, "profile.json")
			os.WriteFile(profilePath, profileRaw, 0600)
			t.Setenv("BOOK_PIPELINE_REGISTRAR_PROFILE", profilePath)
			t.Setenv("BOOK_PIPELINE_REGISTRAR_PROFILE_SHA256", bookpipeline.SHA(profileRaw))
			summary := collectionSummary{}
			summary.total.Inserted = 2
			summary.total.LatestInsertedAt = time.UnixMilli(1500)
			err := writeCollectionReceipt(context.Background(), store, "01a00000-0000-7000-8000-000000000001", 1000, summary)
			if (err != nil) != brokerFailure || calls != 1 {
				t.Fatalf("err=%v calls=%d", err, calls)
			}
			r, readErr := bookrefreshreceipt.Read(receipt)
			if readErr != nil || r.ValidateCompleted() != nil {
				t.Fatal("acknowledged source receipt lost")
			}
			raw, _ := os.ReadFile(receipt)
			pending := filepath.Join(dir, bookpipeline.SHA(raw)+".pending.json")
			if _, err := bookpipeline.PrivateBytes(pending, 8192); err != nil {
				t.Fatal("pending registration WAL lost")
			}
		})
	}
}

func privateTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}
