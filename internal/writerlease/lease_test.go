package writerlease

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const testEpoch = "019a0000-0000-7000-8000-000000000001"

type protocolFixture struct {
	mu      sync.Mutex
	phases  []string
	pending *Intent
	change  func(string, *Reply) error
}

func newProtocolGuard(t *testing.T) (*Guard, *protocolFixture) {
	t.Helper()
	dir := t.TempDir()
	helper, config := filepath.Join(dir, "helper"), filepath.Join(dir, "config.json")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"schema":"sg.book.writer-lease.config.v1","resource":"statground-book-shared-writer-v1","control_epoch":"` + testEpoch + `"}`)
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BOOK_WRITER_LEASE_HELPER", helper)
	t.Setenv("BOOK_WRITER_LEASE_CONFIG", config)
	fixture := &protocolFixture{}
	guard := New()
	guard.invoke = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		phase := args[0]
		fixture.phases = append(fixture.phases, phase)
		values := map[string]string{}
		for i := 1; i+1 < len(args); i += 2 {
			values[args[i]] = args[i+1]
		}
		reply := Reply{Resource: Resource, ControlEpoch: testEpoch, Holder: values["--holder"], Fence: "1", ExpiresEpoch: 1180, ServerEpoch: 1000, FencedWriters: append([]string(nil), participants...)}
		if phase == "begin" {
			fixture.pending = &Intent{Operation: values["--operation"], Target: values["--target"], QueryID: values["--query-id"], RequestSHA256: values["--request-sha256"]}
		}
		if fixture.pending != nil {
			reply.InFlight = phase != "finish"
			reply.Operation, reply.Target, reply.QueryID, reply.RequestSHA256 = fixture.pending.Operation, fixture.pending.Target, fixture.pending.QueryID, fixture.pending.RequestSHA256
		}
		if phase == "finish" {
			fixture.pending = nil
		}
		if phase == "release" {
			reply.Released = true
		}
		if fixture.change != nil {
			if err := fixture.change(phase, &reply); err != nil {
				return nil, err
			}
		}
		return json.Marshal(reply)
	}
	t.Cleanup(func() { guard.Close() })
	return guard, fixture
}

func testIntent() Intent {
	return Intent{Operation: "019a0000-0000-7000-8000-000000000002", Target: "Data_Book_Service.book_provider_latest", QueryID: "019a0000-0000-7000-8000-000000000003", RequestSHA256: strings.Repeat("c", 64)}
}

func TestOwnershipIdentityDriftStopsFurtherAdmission(t *testing.T) {
	for _, field := range []string{"resource", "epoch", "holder", "fence", "participants", "unexpected-pending"} {
		t.Run(field, func(t *testing.T) {
			guard, fixture := newProtocolGuard(t)
			if err := guard.Assert(context.Background()); err != nil {
				t.Fatal(err)
			}
			fixture.mu.Lock()
			fixture.change = func(phase string, reply *Reply) error {
				if phase != "assert" {
					return nil
				}
				switch field {
				case "resource":
					reply.Resource = "other-target"
				case "epoch":
					reply.ControlEpoch = "019a0000-0000-7000-8000-000000000099"
				case "holder":
					reply.Holder = strings.Repeat("b", 32)
				case "fence":
					reply.Fence = "2"
				case "participants":
					reply.FencedWriters = reply.FencedWriters[:1]
				case "unexpected-pending":
					reply.InFlight = true
					reply.Operation = testIntent().Operation
				}
				return nil
			}
			fixture.mu.Unlock()
			if err := guard.Assert(context.Background()); err == nil {
				t.Fatal("changed owner accepted")
			}
			if _, _, err := guard.Begin(context.Background(), testIntent()); err == nil {
				t.Fatal("new operation admitted after lost owner")
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			for _, phase := range fixture.phases {
				if phase == "begin" {
					t.Fatal("begin sent after ownership loss")
				}
			}
		})
	}
}

func TestUnknownBeginCommitDoesNotRetryOrRelease(t *testing.T) {
	guard, fixture := newProtocolGuard(t)
	fixture.change = func(phase string, _ *Reply) error {
		if phase == "begin" {
			return errors.New("commit reply lost")
		}
		return nil
	}
	if _, _, err := guard.Begin(context.Background(), testIntent()); err == nil {
		t.Fatal("unknown commit admitted")
	}
	if _, _, err := guard.Begin(context.Background(), testIntent()); err == nil {
		t.Fatal("unknown commit retried")
	}
	if err := guard.Close(); err == nil {
		t.Fatal("unknown commit released")
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	count := 0
	for _, phase := range fixture.phases {
		if phase == "begin" {
			count++
		}
		if phase == "release" {
			t.Fatal("release sent after unknown begin")
		}
	}
	if count != 1 {
		t.Fatalf("begin attempts=%d", count)
	}
}

func TestCloseCancelsActiveOperationBeforeWaitingForDrain(t *testing.T) {
	guard, fixture := newProtocolGuard(t)
	ctx, op, err := guard.Begin(context.Background(), testIntent())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- guard.Close() }()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		op.Abandon()
		t.Fatal("Close did not cancel admitted operation")
	}
	op.Abandon()
	if _, _, err := guard.Begin(context.Background(), testIntent()); err == nil {
		t.Fatal("admitted operation during Close")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("pending operation released")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after drain")
	}
	if err := op.Confirm(context.Background()); err == nil {
		t.Fatal("abandoned operation confirmed")
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	for _, phase := range fixture.phases {
		if phase == "release" {
			t.Fatal("pending operation released")
		}
	}
}

func TestInvalidFinishIdentityRemainsAStickyFailure(t *testing.T) {
	guard, fixture := newProtocolGuard(t)
	fixture.change = func(phase string, reply *Reply) error {
		if phase == "finish" {
			reply.QueryID = "019a0000-0000-7000-8000-000000000099"
		}
		return nil
	}
	_, op, err := guard.Begin(context.Background(), testIntent())
	if err != nil {
		t.Fatal(err)
	}
	if err := op.Confirm(context.Background()); err == nil {
		t.Fatal("different request confirmed")
	}
	if err := op.Confirm(context.Background()); err == nil {
		t.Fatal("repeat erased failed confirmation")
	}
	if err := guard.Close(); err == nil {
		t.Fatal("unconfirmed intent released")
	}
}

func TestChangedConfigBlocksHelperInvocation(t *testing.T) {
	guard, fixture := newProtocolGuard(t)
	if err := guard.Assert(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(guard.config, append(append([]byte(nil), guard.configBytes...), byte('\n')), 0600); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	before := len(fixture.phases)
	fixture.mu.Unlock()
	if err := guard.Assert(context.Background()); err == nil {
		t.Fatal("changed config accepted")
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.phases) != before {
		t.Fatal("helper invoked using changed config")
	}
}
