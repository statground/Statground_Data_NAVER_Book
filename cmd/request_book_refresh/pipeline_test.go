package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/bookpipeline"
)

func testJob() bookpipeline.RefreshJob {
	r := sourceReceipt()
	return bookpipeline.RefreshJob{Schema: "book.pipeline.refresh-request.v1", JobID: strings.Repeat("a", 64), Environment: "test", RunUUID: r.RunUUID, BatchID: r.RunUUID, SourceID: "kakao.collection", SourceVersion: strings.Repeat("b", 64), SourceSHA256: strings.Repeat("b", 64), SourceBytes: 250, SourceReference: "/retained/source.json", TransformVersion: "native-book-refresh.v1", AuthorityRevision: 1, OwnerFence: 7}
}

type emptyTargetNative struct{ *nativeFake }

func (f emptyTargetNative) QueryJSONEachRowContext(ctx context.Context, q string) ([]map[string]any, error) {
	if strings.Contains(q, "AS generation_epoch FROM ") {
		return []map[string]any{{"row_count": 0, "generation_epoch": 0}}, nil
	}
	return f.nativeFake.QueryJSONEachRowContext(ctx, q)
}

func TestActualGoResultAcceptedBySQLValidatorABI(t *testing.T) {
	sql := os.Getenv("BOOK_PIPELINE_SQL_SOURCE")
	if sql == "" {
		t.Skip("explicit reviewed SQL source root not supplied")
	}
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			dir := privateTestDir(t)
			result := filepath.Join(dir, "result.json")
			jobPath := filepath.Join(dir, "job.json")
			receiptPath := filepath.Join(dir, "source.json")
			r := sourceReceipt()
			raw, _ := json.Marshal(r)
			os.WriteFile(receiptPath, raw, 0600)
			job := testJob()
			job.SourceReference = receiptPath
			job.SourceSHA256 = bookpipeline.SHA(raw)
			job.SourceVersion = job.SourceSHA256
			job.SourceBytes = len(raw)
			jobRaw, _ := json.Marshal(job)
			os.WriteFile(jobPath, jobRaw, 0600)
			var native refreshClient = fakeNative()
			if empty {
				native = emptyTargetNative{fakeNative()}
			}
			if err := executeJob(context.Background(), native, r, job, func(context.Context, refreshStage) error { return nil }, 0, result); err != nil {
				t.Fatal(err)
			}
			script := `import sys,json
sys.path.insert(0,sys.argv[1])
from book_pipeline.model import validate_result,receipt
with open(sys.argv[2],'rb') as f:result=json.load(f)
with open(sys.argv[3],'rb') as f:job=json.load(f)
with open(sys.argv[4],'rb') as f:source=receipt(f.read())
validate_result(result,job,source_watermark_millis=source['latest_inserted_millis'])
assert result['publication_verified'] is False
print('validated_actual_Go_result')
`
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, "python3", "-I", "-B", "-c", script, sql, result, jobPath, receiptPath).CombinedOutput()
			if err != nil || strings.TrimSpace(string(output)) != "validated_actual_Go_result" {
				t.Fatalf("SQL actual Go-result contract: %v %s", err, output)
			}
		})
	}
}

func TestWorkerActualRefreshCallableWritesFourRealReadbacksOnly(t *testing.T) {
	fake := fakeNative()
	path := filepath.Join(privateTestDir(t), "result.json")
	if err := executeJob(context.Background(), fake, sourceReceipt(), testJob(), func(context.Context, refreshStage) error { return nil }, 0, path); err != nil {
		t.Fatal(err)
	}
	raw, err := bookpipeline.PrivateBytes(path, 8192)
	if err != nil {
		t.Fatal(err)
	}
	var result bookpipeline.RefreshResult
	if bookpipeline.Decode(raw, 8192, &result) != nil || result.Validate() != nil {
		t.Fatal("invalid typed result")
	}
	if result.JobID != testJob().JobID || result.OwnerFence != 7 || !result.RefreshVerified || result.PublicationVerified || len(result.Stages) != 4 {
		t.Fatal("binding or status")
	}
	for i, s := range result.Stages {
		if s.View != stages[i].View || s.Target != stages[i].Target || !s.Verified || s.TargetRows != 5 || s.TargetGenerationMillis != 2000000100000 {
			t.Fatal("fabricated or missing readback")
		}
	}
	before := len(fake.commands)
	if executeJob(context.Background(), fake, sourceReceipt(), testJob(), func(context.Context, refreshStage) error { return nil }, 0, path) == nil || len(fake.commands) != before {
		t.Fatal("duplicate result path executed")
	}
	result.PublicationVerified = true
	bad := filepath.Join(filepath.Dir(path), "forged.json")
	if bookpipeline.WriteResult(bad, result) == nil {
		t.Fatal("publication claim accepted")
	}
}

func TestWorkerFailurePendingOutboxAndCancellationNeverWriteSuccess(t *testing.T) {
	for _, kind := range []string{"request_failure", "pending_source", "cancel", "wrong_run"} {
		t.Run(kind, func(t *testing.T) {
			fake := fakeNative()
			ctx := context.Background()
			job := testJob()
			switch kind {
			case "request_failure":
				fake.failCommand = "SYSTEM REFRESH VIEW " + stages[2].View
			case "pending_source":
				fake.pendingOutbox = true
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "wrong_run":
				job.RunUUID = "01a00000-0000-7000-8000-000000000003"
				job.BatchID = job.RunUUID
			}
			path := filepath.Join(privateTestDir(t), "result.json")
			if executeJob(ctx, fake, sourceReceipt(), job, func(context.Context, refreshStage) error { return nil }, 0, path) == nil {
				t.Fatal("accepted failure")
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("partial execution falsely completed")
			}
			if kind != "request_failure" && len(fake.commands) != 0 {
				t.Fatal("effects before admission")
			}
		})
	}
}

func TestBrokerCLIRequiresBoundJobAndDirectModeStaysDefault(t *testing.T) {
	t.Setenv("BOOK_PIPELINE_MODE", "broker")
	if runArgs(context.Background(), nil) == nil {
		t.Fatal("legacy parallel refresh allowed")
	}
	if runArgs(context.Background(), []string{"--job-json", "/absent"}) == nil {
		t.Fatal("missing result")
	}
	t.Setenv("BOOK_PIPELINE_MODE", "")
	if mode, err := bookpipeline.Mode(); mode != "direct" || err != nil {
		t.Fatal("direct changed")
	}
	if runArgs(context.Background(), []string{"--job-json", "/absent", "--result", "/absent"}) == nil {
		t.Fatal("job auto-enabled broker mode")
	}
	j := testJob()
	body, _ := json.Marshal(j)
	dir := privateTestDir(t)
	jobPath := filepath.Join(dir, "job.json")
	os.WriteFile(jobPath, body, 0600)
	t.Setenv("BOOK_PIPELINE_MODE", "broker")
	if runArgs(context.Background(), []string{"--job-json", jobPath, "--result", filepath.Join(dir, "result")}) == nil {
		t.Fatal("missing immutable receipt admitted")
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
