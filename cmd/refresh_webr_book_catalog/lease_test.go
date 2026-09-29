package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestBookMVLeaseFailsClosedWhenHelperOrConfigMissing(t *testing.T) {
	t.Setenv("BOOK_MV_LEASE_HELPER", "")
	t.Setenv("BOOK_MV_LEASE_CONFIG", "")
	if _, err := acquireBookMVLease(); err == nil || !strings.Contains(err.Error(), "BOOK_MV_LEASE_HELPER") {
		t.Fatalf("missing helper error=%v", err)
	}
	t.Setenv("BOOK_MV_LEASE_HELPER", "/usr/bin/true")
	if _, err := acquireBookMVLease(); err == nil || !strings.Contains(err.Error(), "BOOK_MV_LEASE_CONFIG") {
		t.Fatalf("missing config error=%v", err)
	}
}

func TestBookMVLeaseRejectsChangedFenceBeforeMutation(t *testing.T) {
	old := bookMVLeaseInvoke
	t.Cleanup(func() { bookMVLeaseInvoke = old })
	bookMVLeaseInvoke = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		fence := "41"
		if args[0] == "assert" {
			fence = "42"
		}
		return json.Marshal(bookMVLeaseReply{
			Resource: bookMVLeaseResource, Holder: args[4], Fence: fence,
			ExpiresEpoch:  time.Now().Add(3 * time.Minute).Unix(),
			FencedWriters: []string{"manual-refresh", "mv-maintenance"},
		})
	}
	lease := &bookMVLease{helper: "/test-helper", holder: strings.Repeat("a", 32)}
	if _, err := lease.call("acquire"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Check(); err == nil || !strings.Contains(err.Error(), "fence changed") {
		t.Fatalf("changed fence check error=%v", err)
	}
}
