package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"statground_naver_book_go/internal/bookpipeline"
	"statground_naver_book_go/internal/bookrefreshreceipt"
)

// The SQL root is supplied explicitly; tests never discover another repository
// or connect to a configured production registrar/broker/database.
func TestActualSQLRegistrarAndDataCommandRegistrationABI(t *testing.T) {
	sql := os.Getenv("BOOK_PIPELINE_SQL_SOURCE")
	if sql == "" {
		t.Skip("explicit reviewed SQL source root not supplied")
	}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	retained := filepath.Join(dir, "retained")
	spool := filepath.Join(dir, "spool")
	os.Mkdir(retained, 0700)
	os.Mkdir(spool, 0700)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	script := `import sys,json,threading
sys.path.insert(0,sys.argv[1])
from book_pipeline.store import Store
from book_pipeline.receipts import ReceiptStore
from book_pipeline.engine import Registrar
from book_pipeline.http import registration_server
s=Store.sqlite(sys.argv[2],initialize=True)
r=Registrar(s,ReceiptStore(sys.argv[3],"test"))
http=registration_server(r,"127.0.0.1",0,"fixture","test-only")
thread=threading.Thread(target=http.serve_forever,daemon=True);thread.start()
print(json.dumps({"port":http.server_address[1]}),flush=True)
sys.stdin.readline()
http.shutdown();http.server_close();thread.join()
with s.transaction() as c:
 print(json.dumps({"batches":s.one(c,"SELECT count(*) AS n FROM source_batch")["n"],"events":s.one(c,"SELECT count(*) AS n FROM event_outbox")["n"]}),flush=True)
`
	child := exec.CommandContext(ctx, "python3", "-I", "-B", "-u", "-c", script, sql, filepath.Join(dir, "fixture.sqlite"), retained)
	stdin, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if child.Start() != nil {
		t.Fatal("local SQL fixture unavailable")
	}
	waited := false
	defer func() {
		stdin.Close()
		if !waited {
			child.Process.Kill()
			child.Wait()
		}
	}()
	reader := bufio.NewReader(stdout)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal("local SQL fixture startup failed")
	}
	var ready struct {
		Port int `json:"port"`
	}
	if json.Unmarshal([]byte(line), &ready) != nil || ready.Port <= 0 {
		t.Fatal("port")
	}
	receipt := filepath.Join(dir, "collection.json")
	runUUID := "01a00000-0000-7000-8000-000000000001"
	if bookrefreshreceipt.Write(receipt, bookrefreshreceipt.Receipt{Version: 1, RunUUID: runUUID, State: "completed", StartedMillis: 1000, Inserted: 2, LatestInsertedMillis: 1500, CompletedMillis: 2000}) != nil {
		t.Fatal("receipt")
	}
	p := bookpipeline.Profile{Schema: "book.pipeline.registrar-profile.v1", Endpoint: fmt.Sprintf("http://127.0.0.1:%d/internal/book-pipeline/v1/source-batches", ready.Port), Username: "fixture", Password: "test-only", Environment: "test", AuthorityRevision: 1, DurableDirectory: spool, AllowLoopbackHTTP: true}
	raw, _ := json.Marshal(p)
	profile := filepath.Join(dir, "profile.json")
	os.WriteFile(profile, raw, 0600)
	t.Setenv("BOOK_PIPELINE_MODE", "broker")
	t.Setenv("BOOK_PIPELINE_REGISTRAR_PROFILE", profile)
	t.Setenv("BOOK_PIPELINE_REGISTRAR_PROFILE_SHA256", bookpipeline.SHA(raw))
	if err := run(ctx, []string{"--receipt", receipt}); err != nil {
		t.Fatalf("actual registration ABI: %v", err)
	}
	raw, _ = os.ReadFile(receipt)
	ack := filepath.Join(spool, bookpipeline.SHA(raw)+".registered.json")
	ackraw, _ := os.ReadFile(ack)
	var first bookpipeline.RegistrationResult
	if bookpipeline.Decode(ackraw, 8192, &first) != nil || len(first.EventID) != 64 || first.Duplicate {
		t.Fatal("actual ACK")
	}
	// A source-owned local ACK loss after committed registration must reconcile
	// against the original DB identity, not create another batch/event.
	if os.Remove(ack) != nil {
		t.Fatal("fixture ACK loss")
	}
	if err := run(ctx, []string{"--receipt", receipt}); err != nil {
		t.Fatal(err)
	}
	ackraw, _ = os.ReadFile(ack)
	var second bookpipeline.RegistrationResult
	if bookpipeline.Decode(ackraw, 8192, &second) != nil || !second.Duplicate || second.EventID != first.EventID {
		t.Fatal("duplicate ABI")
	}
	stdin.Write([]byte("inspect\n"))
	stdin.Close()
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatal("inspection")
	}
	var counts map[string]int
	if json.Unmarshal([]byte(line), &counts) != nil || counts["batches"] != 1 || counts["events"] != 1 {
		t.Fatalf("duplicate authority effects: %s", line)
	}
	if child.Wait() != nil {
		t.Fatal("fixture closure")
	}
	waited = true
}
