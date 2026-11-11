package bookpipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"statground_naver_book_go/internal/bookrefreshreceipt"
)

func receiptFile(t *testing.T, dir string) (string, []byte) {
	t.Helper()
	path := filepath.Join(dir, "source.json")
	r := bookrefreshreceipt.Receipt{Version: 1, RunUUID: "01a00000-0000-7000-8000-000000000001", State: "completed", StartedMillis: 1000, Inserted: 2, LatestInsertedMillis: 1500, CompletedMillis: 2000}
	if err := bookrefreshreceipt.Write(path, r); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	return path, raw
}

func profile(dir, endpoint string) Profile {
	return Profile{Schema: "book.pipeline.registrar-profile.v1", Endpoint: endpoint, Username: "fixture", Password: "test-only", Environment: "test", AuthorityRevision: 1, DurableDirectory: dir, AllowLoopbackHTTP: true}
}

func answer(request Registration) RegistrationResult {
	return RegistrationResult{Schema: "book.pipeline.source-registration-result.v1", Status: "registered", Duplicate: false, Durable: true, Environment: request.Environment, BatchID: request.BatchID, SourceVersion: request.SourceVersion, SourceSHA256: request.SourceSHA256, EventID: "01a00000-0000-7000-8000-000000000002", ReceiptReference: "/retained/test/" + request.SourceSHA256 + ".json"}
}

func TestRegisterRetainsBeforeAuthenticatedHTTPAndReusesDurableAck(t *testing.T) {
	dir := privateTestDir(t)
	path, raw := receiptFile(t, dir)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		u, p, ok := r.BasicAuth()
		if !ok || u != "fixture" || p != "test-only" || r.Method != "POST" {
			t.Error("authentication or method")
		}
		var req Registration
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			t.Fatal("decode")
		}
		got, err := base64.StdEncoding.DecodeString(req.ReceiptB64)
		if err != nil || string(got) != string(raw) || req.SourceVersion != SHA(raw) || req.SourceBytes != len(raw) {
			t.Error("source identity")
		}
		if _, err := PrivateBytes(req.SourceReference, 4096); err != nil {
			t.Error("receipt not durable before POST")
		}
		if _, err := PrivateBytes(filepath.Join(dir, SHA(raw)+".pending.json"), 8192); err != nil {
			t.Error("pending WAL not durable before POST")
		}
		json.NewEncoder(w).Encode(answer(req))
	}))
	defer server.Close()
	p := profile(dir, server.URL+"/internal/book-pipeline/v1/source-batches")
	for i := 0; i < 2; i++ {
		out, err := Register(context.Background(), p, path)
		if err != nil || !out.Durable {
			t.Fatalf("register: %v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("ack replay called network %d times", calls)
	}
	got, _ := os.ReadFile(path)
	if string(got) != string(raw) {
		t.Fatal("input mutated")
	}
}

func TestFailureRedirectAndCancellationPreservePendingWithoutRetry(t *testing.T) {
	for _, kind := range []string{"failure", "redirect", "wrong_hash", "nil_event", "duplicate_key", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			dir := privateTestDir(t)
			path, raw := receiptFile(t, dir)
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var req Registration
				json.NewDecoder(r.Body).Decode(&req)
				switch kind {
				case "failure":
					w.WriteHeader(503)
				case "redirect":
					w.Header().Set("Location", "/other")
					w.WriteHeader(307)
				case "wrong_hash":
					v := answer(req)
					v.SourceSHA256 = strings.Repeat("0", 64)
					json.NewEncoder(w).Encode(v)
				case "nil_event":
					v := answer(req)
					v.EventID = "00000000-0000-0000-0000-000000000000"
					json.NewEncoder(w).Encode(v)
				case "duplicate_key":
					w.Write([]byte(`{"schema":"book.pipeline.source-registration-result.v1","schema":"other"}`))
				}
			}))
			defer server.Close()
			ctx := context.Background()
			if kind == "cancel" {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			_, err := Register(ctx, profile(dir, server.URL+"/internal/book-pipeline/v1/source-batches"), path)
			if err == nil {
				t.Fatal("accepted negative")
			}
			expected := 1
			if kind == "cancel" {
				expected = 0
			}
			if calls != expected {
				t.Fatalf("calls=%d", calls)
			}
			for _, suffix := range []string{".receipt.json", ".pending.json"} {
				if _, err := PrivateBytes(filepath.Join(dir, SHA(raw)+suffix), 8192); err != nil {
					t.Fatal("lost pending evidence")
				}
			}
			if _, err := os.Lstat(filepath.Join(dir, SHA(raw)+".registered.json")); !os.IsNotExist(err) {
				t.Fatal("invented registered ACK")
			}
		})
	}
}

func TestProspectiveSpoolFileCapIncludesLockAndRefuses4097th(t *testing.T) {
	dir := privateTestDir(t)
	for i := 0; i < 4095; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("held-%04d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if immutable(filepath.Join(dir, "last.lock"), nil) != nil {
		t.Fatal("4096th refused")
	}
	if immutable(filepath.Join(dir, "over"), []byte("not written")) == nil {
		t.Fatal("4097th accepted")
	}
	rows, _ := os.ReadDir(dir)
	if len(rows) != 4096 {
		t.Fatal("cap changed")
	}
	if nonzeroUUID("00000000-0000-0000-0000-000000000000") {
		t.Fatal("nil UUID")
	}
}

func TestPinnedProfilePrivateFilesClosedJSONAndDefaultMode(t *testing.T) {
	dir := privateTestDir(t)
	p := profile(dir, "http://127.0.0.1:1/internal/book-pipeline/v1/source-batches")
	raw, _ := json.Marshal(p)
	path := filepath.Join(dir, "profile.json")
	os.WriteFile(path, raw, 0600)
	t.Setenv("BOOK_PIPELINE_REGISTRAR_PROFILE", path)
	t.Setenv("BOOK_PIPELINE_REGISTRAR_PROFILE_SHA256", SHA(raw))
	if _, err := LoadProfile(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BOOK_PIPELINE_REGISTRAR_PROFILE_SHA256", strings.Repeat("0", 64))
	if _, err := LoadProfile(); err == nil {
		t.Fatal("unpinned profile")
	}
	link := filepath.Join(dir, "link")
	os.Symlink(path, link)
	if _, err := PrivateBytes(link, 4096); err == nil {
		t.Fatal("symlink")
	}
	for _, body := range []string{`{"version":1,"version":1}`, `{"unknown":1}`, `{} {}`, strings.Repeat(" ", 4097)} {
		if _, err := Receipt([]byte(body)); err == nil {
			t.Fatal("open JSON accepted")
		}
	}
	for _, body := range []string{
		`{"version":1,"run_uuid":"01a00000-0000-7000-8000-000000000001","state":"completed","started_millis":1000,"latest_inserted_millis":0,"completed_millis":2000}`,
		`{"version":1,"run_uuid":"01a00000-0000-7000-8000-000000000001","state":"completed","started_millis":1000,"inserted":null,"latest_inserted_millis":0,"completed_millis":2000}`,
	} {
		if _, err := Receipt([]byte(body)); err == nil {
			t.Fatal("missing/null source count masquerades as zero")
		}
	}
	t.Setenv("BOOK_PIPELINE_MODE", "")
	if m, e := Mode(); m != "direct" || e != nil {
		t.Fatal("default changed")
	}
	t.Setenv("BOOK_PIPELINE_MODE", "unrecognized")
	if _, e := Mode(); e == nil {
		t.Fatal("unknown mode")
	}
}

func TestJobBindsPrivateReceiptVersionAndNoSourceMutation(t *testing.T) {
	dir := privateTestDir(t)
	receipt, raw := receiptFile(t, dir)
	j := RefreshJob{Schema: "book.pipeline.refresh-request.v1", JobID: strings.Repeat("a", 64), Environment: "test", RunUUID: "01a00000-0000-7000-8000-000000000001", BatchID: "01a00000-0000-7000-8000-000000000001", SourceID: "kakao.collection", SourceVersion: SHA(raw), SourceSHA256: SHA(raw), SourceBytes: len(raw), SourceReference: receipt, TransformVersion: "native-book-refresh.v1", AuthorityRevision: 1, OwnerFence: 2}
	path := filepath.Join(dir, "job.json")
	body, _ := json.Marshal(j)
	os.WriteFile(path, body, 0600)
	if _, _, err := ReadJob(path); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(receipt, append(raw, ' '), 0600)
	if _, _, err := ReadJob(path); err == nil {
		t.Fatal("source drift")
	}
	j.SourceVersion = strings.Repeat("b", 64)
	if j.Validate() == nil {
		t.Fatal("version/hash mismatch")
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
