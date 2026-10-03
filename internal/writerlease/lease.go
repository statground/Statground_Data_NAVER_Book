// Package writerlease admits Book writes through the SQL-owned CAS helper.
package writerlease

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Resource = "statground-book-shared-writer-v1"

var participants = []string{"nlk-raw", "nlk-projection", "full-catalog", "provider-collection"}

type safeError string

func (e safeError) Error() string { return "Book writer lease failed category=" + string(e) }

type Reply struct {
	Resource      string   `json:"resource"`
	ControlEpoch  string   `json:"control_epoch"`
	Holder        string   `json:"holder"`
	Fence         string   `json:"fence"`
	ExpiresEpoch  int64    `json:"expires_epoch"`
	ServerEpoch   int64    `json:"server_epoch"`
	FencedWriters []string `json:"fenced_writers"`
	Released      bool     `json:"released"`
	InFlight      bool     `json:"in_flight"`
	Operation     string   `json:"operation"`
	Target        string   `json:"target"`
	QueryID       string   `json:"query_id"`
	RequestSHA256 string   `json:"request_sha256"`
}

type Intent struct{ Operation, Target, QueryID, RequestSHA256 string }

// Operation remains pending in the backend unless Confirm receives a positive
// synchronous acknowledgement. Abandon never clears a durable intent.
type Operation interface {
	Confirm(context.Context) error
	Abandon()
}

type Admission interface {
	Assert(context.Context) error
	Begin(context.Context, Intent) (context.Context, Operation, error)
	Close() error
}

type Guard struct {
	mu          sync.Mutex
	operationMu sync.Mutex
	helper      string
	config      string
	configBytes []byte
	helperSHA   [32]byte
	epoch       string
	holder      string
	fence       string
	expiresAt   time.Time
	pending     *Intent
	lost        error
	started     bool
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	invoke      func(context.Context, string, ...string) ([]byte, error)
	renewEvery  time.Duration
}

var processGuard = New()

func Default() *Guard { return processGuard }

func New() *Guard {
	ctx, cancel := context.WithCancel(context.Background())
	return &Guard{ctx: ctx, cancel: cancel, done: make(chan struct{}), invoke: invoke, renewEvery: 10 * time.Second}
}

type limitedBuffer struct{ bytes.Buffer }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 65536 {
		return 0, safeError("reply_limit")
	}
	return b.Buffer.Write(p)
}

func invoke(ctx context.Context, path string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, path, args...)
	var output limitedBuffer
	cmd.Stdout = &output
	if err := cmd.Run(); err != nil {
		return nil, safeError("helper_unavailable")
	}
	return output.Bytes(), nil
}

func trustedFile(path string, executable bool) error {
	if !filepath.IsAbs(path) {
		return safeError("configuration")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return safeError("configuration")
	}
	owner := info.Sys().(*syscall.Stat_t).Uid
	if owner != 0 && owner != uint32(os.Getuid()) || info.Mode().Perm()&0022 != 0 {
		return safeError("configuration")
	}
	if executable && info.Mode().Perm()&0100 == 0 || !executable && info.Mode().Perm()&0077 != 0 {
		return safeError("configuration")
	}
	return nil
}

func (g *Guard) initializeLocked(ctx context.Context) error {
	if g.closed || g.lost != nil {
		return safeError("ownership_lost")
	}
	if g.started {
		if !time.Now().Before(g.expiresAt) {
			g.failLocked(safeError("ownership_expired"))
			return g.lost
		}
		return nil
	}
	g.helper, g.config = os.Getenv("BOOK_WRITER_LEASE_HELPER"), os.Getenv("BOOK_WRITER_LEASE_CONFIG")
	if trustedFile(g.helper, true) != nil || trustedFile(g.config, false) != nil {
		return safeError("configuration")
	}
	data, err := os.ReadFile(g.config)
	if err != nil || len(data) > 65536 {
		return safeError("configuration")
	}
	var cfg struct {
		Schema       string `json:"schema"`
		Resource     string `json:"resource"`
		ControlEpoch string `json:"control_epoch"`
	}
	if json.Unmarshal(data, &cfg) != nil || cfg.Schema != "sg.book.writer-lease.config.v1" || cfg.Resource != Resource || cfg.ControlEpoch == "" {
		return safeError("configuration")
	}
	g.configBytes, g.epoch = data, cfg.ControlEpoch
	helperBytes, err := os.ReadFile(g.helper)
	if err != nil || len(helperBytes) > 2<<20 {
		return safeError("configuration")
	}
	g.helperSHA = sha256.Sum256(helperBytes)
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return safeError("entropy")
	}
	g.holder = hex.EncodeToString(id[:])
	if _, err = g.callLocked(ctx, "acquire", nil); err != nil {
		g.failLocked(err)
		return err
	}
	g.started = true
	go g.heartbeat()
	return nil
}

func (g *Guard) callLocked(ctx context.Context, phase string, intent *Intent) (Reply, error) {
	data, err := os.ReadFile(g.config)
	if err != nil || !bytes.Equal(data, g.configBytes) || trustedFile(g.config, false) != nil || trustedFile(g.helper, true) != nil {
		return Reply{}, safeError("configuration_changed")
	}
	helperBytes, err := os.ReadFile(g.helper)
	if err != nil || sha256.Sum256(helperBytes) != g.helperSHA {
		return Reply{}, safeError("helper_changed")
	}
	configSHA := sha256.Sum256(g.configBytes)
	args := []string{phase, "--config", g.config, "--config-sha256", hex.EncodeToString(configSHA[:]), "--resource", Resource, "--holder", g.holder}
	if g.fence != "" {
		args = append(args, "--fence", g.fence)
	}
	if intent != nil {
		args = append(args, "--operation", intent.Operation, "--target", intent.Target, "--query-id", intent.QueryID, "--request-sha256", intent.RequestSHA256)
		if phase == "finish" {
			args = append(args, "--confirmation", "synchronous-ack")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	started := time.Now()
	output, err := g.invoke(ctx, g.helper, args...)
	if err != nil {
		return Reply{}, safeError("helper_unavailable")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(output, &fields) != nil || len(fields) != 13 {
		return Reply{}, safeError("reply_invalid")
	}
	for _, key := range []string{"resource", "control_epoch", "holder", "fence", "expires_epoch", "server_epoch", "fenced_writers", "released", "in_flight", "operation", "target", "query_id", "request_sha256"} {
		if value, ok := fields[key]; !ok || bytes.Equal(value, []byte("null")) {
			return Reply{}, safeError("reply_invalid")
		}
	}
	var reply Reply
	d := json.NewDecoder(bytes.NewReader(output))
	d.DisallowUnknownFields()
	if d.Decode(&reply) != nil || d.Decode(new(any)) != io.EOF {
		return Reply{}, safeError("reply_invalid")
	}
	fence, err := strconv.ParseUint(reply.Fence, 10, 64)
	if err != nil || fence == 0 || reply.Resource != Resource || reply.ControlEpoch != g.epoch || reply.Holder != g.holder || g.fence != "" && reply.Fence != g.fence {
		return Reply{}, safeError("identity_changed")
	}
	seen := map[string]bool{}
	for _, writer := range reply.FencedWriters {
		seen[writer] = true
	}
	if len(seen) != len(participants) || len(reply.FencedWriters) != len(participants) {
		return Reply{}, safeError("coverage_invalid")
	}
	for _, writer := range participants {
		if !seen[writer] {
			return Reply{}, safeError("coverage_invalid")
		}
	}
	if phase == "release" {
		if !reply.Released || reply.InFlight {
			return Reply{}, safeError("release_unconfirmed")
		}
	} else if phase != "finish" && (reply.Released || reply.ServerEpoch <= 0 || reply.ExpiresEpoch <= reply.ServerEpoch+60) {
		return Reply{}, safeError("ttl_invalid")
	}
	if intent != nil {
		if reply.Released || phase == "begin" && !reply.InFlight || phase == "finish" && reply.InFlight {
			return Reply{}, safeError("intent_unconfirmed")
		}
		if reply.Operation != intent.Operation || reply.Target != intent.Target || reply.QueryID != intent.QueryID || reply.RequestSHA256 != intent.RequestSHA256 {
			return Reply{}, safeError("intent_identity_changed")
		}
	} else if g.pending != nil {
		if !reply.InFlight || reply.Operation != g.pending.Operation || reply.Target != g.pending.Target || reply.QueryID != g.pending.QueryID || reply.RequestSHA256 != g.pending.RequestSHA256 {
			return Reply{}, safeError("intent_identity_changed")
		}
	} else if reply.InFlight || reply.Operation != "" || reply.Target != "" || reply.QueryID != "" || reply.RequestSHA256 != "" {
		return Reply{}, safeError("unexpected_intent")
	}
	if phase != "finish" && phase != "release" {
		// Count round-trip time against the server-reported TTL, never add it
		// to the ownership window. The backend remains authoritative.
		g.expiresAt = started.Add(time.Duration(reply.ExpiresEpoch-reply.ServerEpoch) * time.Second)
	}
	if g.fence == "" {
		g.fence = reply.Fence
	}
	return reply, nil
}

func (g *Guard) failLocked(err error) {
	if g.lost == nil {
		g.lost = err
	}
	g.cancel()
}

func (g *Guard) heartbeat() {
	defer close(g.done)
	ticker := time.NewTicker(g.renewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.mu.Lock()
			var err error
			if !time.Now().Before(g.expiresAt) {
				err = safeError("ownership_expired")
			} else {
				_, err = g.callLocked(context.Background(), "renew", nil)
			}
			if err != nil {
				g.failLocked(err)
			}
			g.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func (g *Guard) Assert(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.initializeLocked(ctx); err != nil {
		return err
	}
	_, err := g.callLocked(ctx, "assert", nil)
	if err != nil {
		g.failLocked(err)
	}
	return err
}

func (g *Guard) Context(ctx context.Context) (context.Context, context.CancelFunc) {
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.ctx, cancel)
	return child, func() { stop(); cancel() }
}

type operation struct {
	g      *Guard
	intent Intent
	cancel context.CancelFunc
	stop   func() bool
	once   sync.Once
	result error
}

func (g *Guard) Begin(ctx context.Context, intent Intent) (context.Context, Operation, error) {
	g.operationMu.Lock()
	g.mu.Lock()
	if err := g.initializeLocked(ctx); err != nil {
		g.mu.Unlock()
		g.operationMu.Unlock()
		return nil, nil, err
	}
	if _, err := g.callLocked(ctx, "begin", &intent); err != nil {
		g.failLocked(err)
		g.mu.Unlock()
		g.operationMu.Unlock()
		return nil, nil, err
	}
	g.pending = &intent
	g.mu.Unlock()
	child, cancel := context.WithCancel(ctx)
	return child, &operation{g: g, intent: intent, cancel: cancel, stop: context.AfterFunc(g.ctx, cancel)}, nil
}

func (op *operation) Confirm(ctx context.Context) error {
	op.once.Do(func() {
		op.g.mu.Lock()
		_, op.result = op.g.callLocked(ctx, "finish", &op.intent)
		if op.result == nil {
			op.g.pending = nil
		} else {
			op.g.failLocked(op.result)
		}
		op.g.mu.Unlock()
		op.stop()
		op.cancel()
		op.g.operationMu.Unlock()
	})
	return op.result
}

func (op *operation) Abandon() {
	op.once.Do(func() {
		op.result = safeError("delivery_pending")
		op.g.mu.Lock()
		op.g.failLocked(safeError("delivery_pending"))
		op.g.mu.Unlock()
		op.stop()
		op.cancel()
		op.g.operationMu.Unlock()
	})
}

func (g *Guard) Close() error {
	g.mu.Lock()
	if g.closed || !g.started {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.cancel()
	g.mu.Unlock()
	// Cancel admitted requests before waiting for their terminal response.
	// New admissions see closed=true even while Close is waiting for drain.
	g.operationMu.Lock()
	defer g.operationMu.Unlock()
	<-g.done
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pending != nil || g.lost != nil {
		return safeError("delivery_pending")
	}
	_, err := g.callLocked(context.Background(), "release", nil)
	return err
}

func IsLeaseError(err error) bool {
	var value safeError
	return errors.As(err, &value)
}
