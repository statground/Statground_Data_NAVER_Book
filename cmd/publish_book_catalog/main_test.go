package main

import (
	"context"
	"testing"
)

func TestInvalidPublishCLIStopsBeforeDatabase(t *testing.T) {
	for _, args := range [][]string{nil, {"--manifest", "missing.json", "--chunk-size", "0"}, {"--manifest", "missing.json", "--chunk-size", "100001"}, {"--manifest", "missing.json", "--snapshot-date", "invalid"}, {"--manifest", "missing.json", "--plan-only"}, {"--manifest", "missing.json", "unexpected"}} {
		if err := run(context.Background(), args); err == nil {
			t.Fatalf("invalid flags/inventory accepted: %v", args)
		}
	}
}
