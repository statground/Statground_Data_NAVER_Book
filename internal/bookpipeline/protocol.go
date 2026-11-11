// Package bookpipeline binds acknowledged Book source receipts to the control
// registrar. It never publishes Kafka events or RabbitMQ messages itself.
package bookpipeline

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"

	"statground_naver_book_go/internal/bookrefreshreceipt"
)

const MaxReceiptBytes = 4096

var digest = regexp.MustCompile(`^[0-9a-f]{64}$`)
var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func nonzeroUUID(value string) bool {
	return uuid.MatchString(value) && value != "00000000-0000-0000-0000-000000000000"
}

func refused() error        { return errors.New("book pipeline contract refused") }
func SHA(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

// Mode is opt-in. Unknown modes fail closed instead of silently selecting a
// second execution owner.
func Mode() (string, error) {
	mode := strings.TrimSpace(os.Getenv("BOOK_PIPELINE_MODE"))
	if mode == "" || mode == "direct" {
		return "direct", nil
	}
	if mode == "broker" {
		return mode, nil
	}
	return "", refused()
}

// Decode rejects duplicate keys, unknown fields, trailing values and oversized
// inputs. Both the worker and registrar use a closed versioned wire contract.
func Decode(raw []byte, maximum int, out any) error {
	if len(raw) == 0 || len(raw) > maximum {
		return refused()
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	var scan func() error
	scan = func() error {
		t, err := d.Token()
		if err != nil {
			return err
		}
		if t == nil {
			return refused()
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{':
				seen := map[string]bool{}
				for d.More() {
					k, err := d.Token()
					if err != nil {
						return err
					}
					key, ok := k.(string)
					if !ok || seen[key] {
						return refused()
					}
					seen[key] = true
					if err := scan(); err != nil {
						return err
					}
				}
			case '[':
				for d.More() {
					if err := scan(); err != nil {
						return err
					}
				}
			default:
				return refused()
			}
			_, err = d.Token()
			return err
		}
		return nil
	}
	if scan() != nil {
		return refused()
	}
	if _, err := d.Token(); err != io.EOF {
		return refused()
	}
	// Every wire struct is closed and requires explicit values, including false
	// and zero. An absent/null field must not fabricate source acknowledgement.
	typ := reflect.TypeOf(out)
	if typ == nil || typ.Kind() != reflect.Pointer || typ.Elem().Kind() != reflect.Struct {
		return refused()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return refused()
	}
	typ = typ.Elem()
	if len(fields) != typ.NumField() {
		return refused()
	}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Tag.Get("json")
		if _, ok := fields[name]; !ok {
			return refused()
		}
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return refused()
	}
	return nil
}

// PrivateBytes retains the named FD identity while reading. Profiles and jobs
// must be source-owned regular 0600 files; links and path substitution refuse.
func PrivateBytes(path string, maximum int) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, refused()
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return nil, refused()
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, refused()
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return nil, refused()
	}
	s, ok := before.Sys().(*syscall.Stat_t)
	if !ok || !before.Mode().IsRegular() || before.Mode().Perm() != 0600 || s.Uid != uint32(os.Geteuid()) || s.Nlink != 1 || before.Size() <= 0 || before.Size() > int64(maximum) {
		return nil, refused()
	}
	raw, err := io.ReadAll(io.LimitReader(f, int64(maximum)+1))
	if err != nil || len(raw) != int(before.Size()) {
		return nil, refused()
	}
	after, e1 := f.Stat()
	named, e2 := os.Lstat(path)
	if e1 != nil || e2 != nil || !sameStat(before, after) || !sameStat(before, named) {
		return nil, refused()
	}
	return raw, nil
}

func sameStat(a, b os.FileInfo) bool {
	x, xok := a.Sys().(*syscall.Stat_t)
	y, yok := b.Sys().(*syscall.Stat_t)
	return xok && yok && x.Dev == y.Dev && x.Ino == y.Ino && x.Mode == y.Mode && x.Uid == y.Uid && x.Gid == y.Gid && x.Nlink == y.Nlink && x.Size == y.Size && x.Mtim == y.Mtim && x.Ctim == y.Ctim
}

func Receipt(raw []byte) (bookrefreshreceipt.Receipt, error) {
	var r bookrefreshreceipt.Receipt
	if Decode(raw, MaxReceiptBytes, &r) != nil || !nonzeroUUID(r.RunUUID) || r.ValidateCompleted() != nil {
		return r, refused()
	}
	return r, nil
}

type RefreshJob struct {
	Schema            string `json:"schema"`
	JobID             string `json:"job_id"`
	Environment       string `json:"environment"`
	RunUUID           string `json:"run_uuid"`
	BatchID           string `json:"batch_id"`
	SourceID          string `json:"source_id"`
	SourceVersion     string `json:"source_version"`
	SourceSHA256      string `json:"source_sha256"`
	SourceBytes       int    `json:"source_bytes"`
	SourceReference   string `json:"source_reference"`
	TransformVersion  string `json:"transform_version"`
	AuthorityRevision uint64 `json:"authority_revision"`
	OwnerFence        uint64 `json:"owner_fence"`
}

func (j RefreshJob) Validate() error {
	if j.Schema != "book.pipeline.refresh-request.v1" || !digest.MatchString(j.JobID) || (j.Environment != "prod" && j.Environment != "test") || !nonzeroUUID(j.RunUUID) || j.BatchID != j.RunUUID || j.SourceID != "kakao.collection" || !digest.MatchString(j.SourceSHA256) || j.SourceVersion != j.SourceSHA256 || j.SourceBytes < 1 || j.SourceBytes > MaxReceiptBytes || !filepath.IsAbs(j.SourceReference) || j.TransformVersion != "native-book-refresh.v1" || j.AuthorityRevision == 0 || j.OwnerFence == 0 {
		return refused()
	}
	return nil
}

func ReadJob(path string) (RefreshJob, bookrefreshreceipt.Receipt, error) {
	var j RefreshJob
	var r bookrefreshreceipt.Receipt
	raw, err := PrivateBytes(path, 8192)
	if err != nil || Decode(raw, 8192, &j) != nil || j.Validate() != nil {
		return j, r, refused()
	}
	raw, err = PrivateBytes(j.SourceReference, MaxReceiptBytes)
	if err != nil || len(raw) != j.SourceBytes || SHA(raw) != j.SourceSHA256 {
		return j, r, refused()
	}
	r, err = Receipt(raw)
	if err != nil || r.RunUUID != j.RunUUID {
		return j, r, refused()
	}
	return j, r, nil
}

type StageResult struct {
	View                   string `json:"view"`
	Target                 string `json:"target"`
	SourceWatermarkMillis  int64  `json:"source_watermark_millis"`
	RefreshStartedMillis   int64  `json:"refresh_started_millis"`
	RefreshSuccessMillis   int64  `json:"refresh_success_millis"`
	TargetRows             int64  `json:"target_rows"`
	TargetGenerationMillis int64  `json:"target_generation_millis"`
	Verified               bool   `json:"verified"`
}

type RefreshResult struct {
	Schema              string        `json:"schema"`
	JobID               string        `json:"job_id"`
	Environment         string        `json:"environment"`
	RunUUID             string        `json:"run_uuid"`
	BatchID             string        `json:"batch_id"`
	SourceVersion       string        `json:"source_version"`
	SourceSHA256        string        `json:"source_sha256"`
	TransformVersion    string        `json:"transform_version"`
	OwnerFence          uint64        `json:"owner_fence"`
	Status              string        `json:"status"`
	RefreshVerified     bool          `json:"refresh_verified"`
	PublicationVerified bool          `json:"publication_verified"`
	Stages              []StageResult `json:"stages"`
}

func Result(j RefreshJob, stages []StageResult) RefreshResult {
	return RefreshResult{Schema: "book.pipeline.refresh-result.v1", JobID: j.JobID, Environment: j.Environment, RunUUID: j.RunUUID, BatchID: j.BatchID, SourceVersion: j.SourceVersion, SourceSHA256: j.SourceSHA256, TransformVersion: j.TransformVersion, OwnerFence: j.OwnerFence, Status: "verified", RefreshVerified: true, PublicationVerified: false, Stages: stages}
}

// WriteResult is append-only. Existing result files never authorize a second
// execution; the control worker reconciles them before any new dispatch.
func WriteResult(path string, result RefreshResult) error {
	if ResultAvailable(path) != nil || result.Validate() != nil {
		return refused()
	}
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > 8192 {
		return refused()
	}
	return immutable(path, append(raw, '\n'))
}

func ResultAvailable(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || privateDirectory(filepath.Dir(path)) != nil {
		return refused()
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return refused()
	}
	return nil
}

func (r RefreshResult) Validate() error {
	if r.Schema != "book.pipeline.refresh-result.v1" || !digest.MatchString(r.JobID) || (r.Environment != "prod" && r.Environment != "test") || !nonzeroUUID(r.RunUUID) || r.BatchID != r.RunUUID || !digest.MatchString(r.SourceSHA256) || r.SourceVersion != r.SourceSHA256 || r.TransformVersion != "native-book-refresh.v1" || r.OwnerFence == 0 || r.Status != "verified" || !r.RefreshVerified || r.PublicationVerified || len(r.Stages) != 4 {
		return refused()
	}
	views := []string{"Data_Book_Service.mv_book_catalog_latest_refresh", "webr_book.mv_naver_r_book_catalog_refresh", "mirtype_book.mv_naver_language_book_catalog_refresh", "Data_Book_Service.mv_book_bibliography_discovery_refresh"}
	targets := []string{"Data_Book_Service.v_book_catalog_latest_current", "webr_book.v_naver_r_book_catalog", "mirtype_book.v_naver_language_book_catalog", "Data_Book_Service.v_book_bibliography_discovery"}
	for i, s := range r.Stages {
		if s.View != views[i] || s.Target != targets[i] || !s.Verified || s.SourceWatermarkMillis <= 0 || s.RefreshStartedMillis < (s.SourceWatermarkMillis/1000+1)*1000 || s.RefreshSuccessMillis < s.RefreshStartedMillis || s.TargetRows < 0 || s.TargetGenerationMillis < 0 || s.TargetRows > 0 && s.TargetGenerationMillis == 0 || i > 0 && s.SourceWatermarkMillis != r.Stages[i-1].RefreshSuccessMillis {
			return refused()
		}
	}
	return nil
}
