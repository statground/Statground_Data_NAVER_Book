package bookpipeline

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type Profile struct {
	Schema            string `json:"schema"`
	Endpoint          string `json:"endpoint"`
	Username          string `json:"username"`
	Password          string `json:"password"`
	Environment       string `json:"environment"`
	AuthorityRevision uint64 `json:"authority_revision"`
	DurableDirectory  string `json:"durable_directory"`
	AllowLoopbackHTTP bool   `json:"allow_loopback_http"`
}

func LoadProfile() (Profile, error) {
	var p Profile
	raw, err := PrivateBytes(os.Getenv("BOOK_PIPELINE_REGISTRAR_PROFILE"), 4096)
	expected := os.Getenv("BOOK_PIPELINE_REGISTRAR_PROFILE_SHA256")
	if err != nil || !digest.MatchString(expected) || SHA(raw) != expected || Decode(raw, 4096, &p) != nil || p.Validate() != nil {
		return p, refused()
	}
	return p, nil
}

func (p Profile) Validate() error {
	u, err := url.Parse(p.Endpoint)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/internal/book-pipeline/v1/source-batches" || u.Hostname() == "" {
		return refused()
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || !p.AllowLoopbackHTTP || ip == nil || !ip.IsLoopback() {
			return refused()
		}
	}
	if p.Schema != "book.pipeline.registrar-profile.v1" || p.Username == "" || strings.Contains(p.Username, ":") || p.Password == "" || len(p.Username) > 256 || len(p.Password) > 1024 || (p.Environment != "prod" && p.Environment != "test") || p.AuthorityRevision == 0 {
		return refused()
	}
	return privateDirectory(p.DurableDirectory)
}

func privateDirectory(path string) error {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || !filepath.IsAbs(path) || canonical != path {
		return refused()
	}
	s, err := os.Lstat(path)
	if err != nil || !s.IsDir() || s.Mode().Perm() != 0700 {
		return refused()
	}
	owner, ok := s.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Geteuid()) {
		return refused()
	}
	return nil
}

func immutable(path string, raw []byte) error {
	parent := filepath.Dir(path)
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || privateDirectory(parent) != nil {
		return refused()
	}
	dirfd, err := syscall.Open(parent, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return refused()
	}
	dir := os.NewFile(uintptr(dirfd), parent)
	defer dir.Close()
	if syscall.Flock(dirfd, syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return refused()
	}
	defer syscall.Flock(dirfd, syscall.LOCK_UN)
	before, err := dir.Stat()
	if err != nil {
		return refused()
	}
	sameDirectory := func() bool {
		named, err := os.Lstat(parent)
		if err != nil {
			return false
		}
		x, xok := before.Sys().(*syscall.Stat_t)
		y, yok := named.Sys().(*syscall.Stat_t)
		return xok && yok && x.Dev == y.Dev && x.Ino == y.Ino && x.Mode == y.Mode && x.Uid == y.Uid && x.Gid == y.Gid && x.Nlink == y.Nlink
	}
	if !sameDirectory() {
		return refused()
	}
	// A full spool refuses new work without deleting pending evidence.
	entries, err := dir.ReadDir(4097)
	if err != nil && err != io.EOF || len(entries) >= 4096 {
		return refused()
	}
	var total int64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return refused()
		}
		total += info.Size()
		if total > 64*1024*1024 {
			return refused()
		}
	}
	if total+int64(len(raw)) > 64*1024*1024 {
		return refused()
	}
	fd, err := syscall.Openat(dirfd, filepath.Base(path), syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return refused()
	}
	f := os.NewFile(uintptr(fd), path)
	n, err := f.Write(raw)
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil || dir.Sync() != nil || !sameDirectory() {
		return refused()
	}
	return nil
}

func retain(path string, raw []byte) error {
	if _, err := os.Lstat(path); err == nil {
		old, err := PrivateBytes(path, 8192)
		if err != nil || !bytes.Equal(old, raw) {
			return refused()
		}
		return nil
	} else if !os.IsNotExist(err) {
		return refused()
	}
	return immutable(path, raw)
}

type Registration struct {
	Schema            string `json:"schema"`
	Environment       string `json:"environment"`
	SourceID          string `json:"source_id"`
	SourceVersion     string `json:"source_version"`
	BatchID           string `json:"batch_id"`
	RunUUID           string `json:"run_uuid"`
	TransformVersion  string `json:"transform_version"`
	AuthorityRevision uint64 `json:"authority_revision"`
	CorrelationID     string `json:"correlation_id"`
	SourceSHA256      string `json:"source_sha256"`
	SourceBytes       int    `json:"source_bytes"`
	SourceReference   string `json:"source_reference"`
	ReceiptB64        string `json:"receipt_b64"`
}

type RegistrationResult struct {
	Schema           string `json:"schema"`
	Status           string `json:"status"`
	Duplicate        bool   `json:"duplicate"`
	Durable          bool   `json:"durable"`
	Environment      string `json:"environment"`
	BatchID          string `json:"batch_id"`
	SourceVersion    string `json:"source_version"`
	SourceSHA256     string `json:"source_sha256"`
	EventID          string `json:"event_id"`
	ReceiptReference string `json:"receipt_reference"`
}

func (r RegistrationResult) matches(request Registration) bool {
	return r.Schema == "book.pipeline.source-registration-result.v1" && r.Status == "registered" && r.Durable && r.Environment == request.Environment && r.BatchID == request.BatchID && r.SourceVersion == request.SourceVersion && r.SourceSHA256 == request.SourceSHA256 && nonzeroUUID(r.EventID) && filepath.IsAbs(r.ReceiptReference) && filepath.Clean(r.ReceiptReference) == r.ReceiptReference
}

// Register persists the immutable receipt and exact pending request before its
// single HTTP attempt. Failure never removes them or produces a completion ACK.
// The server owns receipt retention plus the TiDB batch/outbox transaction.
func Register(ctx context.Context, p Profile, receiptPath string) (RegistrationResult, error) {
	var result RegistrationResult
	if p.Validate() != nil {
		return result, refused()
	}
	raw, err := PrivateBytes(receiptPath, MaxReceiptBytes)
	if err != nil {
		return result, refused()
	}
	r, err := Receipt(raw)
	if err != nil {
		return result, err
	}
	hash := SHA(raw)
	base := filepath.Join(p.DurableDirectory, hash)
	if _, err := os.Lstat(base + ".lock"); os.IsNotExist(err) {
		if immutable(base+".lock", nil) != nil {
			return result, refused()
		}
	} else if err != nil {
		return result, refused()
	}
	lock, err := os.OpenFile(base+".lock", os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return result, refused()
	}
	defer lock.Close()
	s, err := lock.Stat()
	if err != nil || !s.Mode().IsRegular() || s.Mode().Perm() != 0600 {
		return result, refused()
	}
	ls, ok := s.Sys().(*syscall.Stat_t)
	if !ok || ls.Uid != uint32(os.Geteuid()) || ls.Nlink != 1 {
		return result, refused()
	}
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return result, refused()
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	retained := base + ".receipt.json"
	if retain(retained, raw) != nil {
		return result, refused()
	}
	request := Registration{Schema: "book.pipeline.source-registration.v1", Environment: p.Environment, SourceID: "kakao.collection", SourceVersion: hash, BatchID: r.RunUUID, RunUUID: r.RunUUID, TransformVersion: "native-book-refresh.v1", AuthorityRevision: p.AuthorityRevision, CorrelationID: r.RunUUID, SourceSHA256: hash, SourceBytes: len(raw), SourceReference: retained, ReceiptB64: base64.StdEncoding.EncodeToString(raw)}
	body, err := json.Marshal(request)
	if err != nil || len(body) > 8192 {
		return result, refused()
	}
	profileBytes, _ := json.Marshal(p)
	wal, _ := json.Marshal(struct {
		Schema        string       `json:"schema"`
		ProfileSHA256 string       `json:"profile_sha256"`
		Request       Registration `json:"request"`
	}{"book.pipeline.registration-WAL.v1", SHA(profileBytes), request})
	if len(wal) > 8192 || retain(base+".pending.json", append(wal, '\n')) != nil {
		return result, refused()
	}
	ackPath := base + ".registered.json"
	if ack, err := PrivateBytes(ackPath, 8192); err == nil {
		if Decode(ack, 8192, &result) != nil || !result.matches(request) {
			return result, refused()
		}
		return result, nil
	} else if _, err := os.Lstat(ackPath); !os.IsNotExist(err) {
		return result, refused()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(body))
	if err != nil {
		return result, refused()
	}
	req.SetBasicAuth(p.Username, p.Password)
	req.Header.Set("Content-Type", "application/json")
	transport := &http.Transport{Proxy: nil, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 30 * time.Second}
	defer transport.CloseIdleConnections()
	client := http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return result, refused()
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return result, refused()
	}
	ack, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil || Decode(ack, 8192, &result) != nil || !result.matches(request) {
		return result, refused()
	}
	if retain(ackPath, ack) != nil {
		return result, refused()
	}
	return result, nil
}
