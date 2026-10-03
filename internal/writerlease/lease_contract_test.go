package writerlease

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHeartbeatLossCancelsAdmittedRequest(t *testing.T) {
	g, b := newProtocolGuard(t)
	g.renewEvery = time.Millisecond
	ctx, op, err := g.Begin(context.Background(), Intent{"00000000-0000-4000-8000-000000000002", "Data_Book_NLK_Raw.nlk_resource_raw", "00000000-0000-4000-8000-000000000002", strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.change = func(phase string, r *Reply) error {
		if phase == "renew" {
			return errors.New("renew lost")
		}
		return nil
	}
	b.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("heartbeat loss did not cancel")
	}
	op.Abandon()
	if g.Assert(context.Background()) == nil {
		t.Fatal("assert resumed after loss")
	}
	if g.pending == nil {
		t.Fatal("heartbeat loss cleared durable intent")
	}
}

func TestProtectedConfigEnvAndHelperPinned(t *testing.T) {
	for _, change := range []string{"env", "config", "helper"} {
		t.Run(change, func(t *testing.T) {
			g, _ := newProtocolGuard(t)
			originalInvoke := g.invoke
			originalPath := os.Getenv("BOOK_WRITER_LEASE_CONFIG")
			g.invoke = func(ctx context.Context, path string, args ...string) ([]byte, error) {
				values := map[string]string{}
				for i := 1; i+1 < len(args); i += 2 {
					values[args[i]] = args[i+1]
				}
				if values["--config"] != originalPath {
					t.Fatal("child path not pinned")
				}
				raw, err := os.ReadFile(values["--config"])
				if err != nil {
					return nil, err
				}
				sum := sha256.Sum256(raw)
				if values["--config-sha256"] != hex.EncodeToString(sum[:]) {
					t.Fatal("child bytes not pinned")
				}
				return originalInvoke(ctx, path, args...)
			}
			if err := g.Assert(context.Background()); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "env":
				t.Setenv("BOOK_WRITER_LEASE_CONFIG", filepath.Join(t.TempDir(), "other.json"))
			case "config":
				if err := os.WriteFile(g.config, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "helper":
				if err := os.WriteFile(g.helper, []byte("changed"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			err := g.Assert(context.Background())
			if change == "env" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("trusted file modification accepted")
			}
		})
	}
}

func TestMissingReplyFieldsFailClosed(t *testing.T) {
	g, _ := newProtocolGuard(t)
	original := g.invoke
	g.invoke = func(ctx context.Context, path string, args ...string) ([]byte, error) {
		raw, err := original(ctx, path, args...)
		if err != nil {
			return nil, err
		}
		var values map[string]any
		_ = json.Unmarshal(raw, &values)
		delete(values, "in_flight")
		return json.Marshal(values)
	}
	if g.Assert(context.Background()) == nil {
		t.Fatal("missing active state accepted")
	}
}

func TestChildVerifiesSameOpenedConfigBytes(t *testing.T) {
	g, _ := newProtocolGuard(t)
	original := g.invoke
	g.invoke = func(ctx context.Context, path string, args ...string) ([]byte, error) {
		values := map[string]string{}
		for i := 1; i+1 < len(args); i += 2 {
			values[args[i]] = args[i+1]
		}
		if err := os.WriteFile(g.config, []byte(`{"schema":"sg.book.writer-lease.config.v1","resource":"statground-book-shared-writer-v1","control_epoch":"`+testEpoch+`","host":"another-backend"}`), 0600); err != nil {
			return nil, err
		}
		raw, err := os.ReadFile(values["--config"])
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(raw)
		if values["--config-sha256"] != hex.EncodeToString(sum[:]) {
			return nil, errors.New("child config changed")
		}
		return original(ctx, path, args...)
	}
	if _, _, err := g.Begin(context.Background(), Intent{"00000000-0000-4000-8000-000000000002", "Data_Book_NLK_Raw.nlk_resource_raw", "00000000-0000-4000-8000-000000000002", strings.Repeat("a", 64)}); err == nil {
		t.Fatal("backend substituted after parent pin check")
	}
	if g.pending != nil {
		t.Fatal("write admitted under substituted backend")
	}
}
