package main

import (
	"context"
	"strings"
	"testing"
)

func TestSnapshotDateIsRequiredBeforeClickHouseConfiguration(t *testing.T) {
	t.Setenv("NLK_SERVICE_BACKFILL_SNAPSHOT_DATE", "")
	err := run(context.Background(), nil)
	if err == nil || err.Error() != "NLK service projection backfill failed category=invalid_snapshot_date" {
		t.Fatalf("run() error = %v", err)
	}
}

func TestConcurrencyOtherThanOneIsRejectedBeforeClickHouseConfiguration(t *testing.T) {
	err := run(context.Background(), []string{
		"--snapshot-date=2026-05-29",
		"--concurrency=2",
	})
	if err == nil || err.Error() != "NLK service projection backfill failed category=invalid_concurrency" {
		t.Fatalf("run() error = %v", err)
	}
}

func TestInvalidProjectionDoesNotEchoInput(t *testing.T) {
	const unsafe = "authority; DROP TABLE private.secret"
	err := run(context.Background(), []string{
		"--snapshot-date=2026-05-29",
		"--projections=" + unsafe,
	})
	if err == nil || err.Error() != "NLK service projection backfill failed category=invalid_projection" {
		t.Fatalf("run() error = %v", err)
	}
	if strings.Contains(err.Error(), unsafe) {
		t.Fatalf("unsafe error echoed projection input: %q", err)
	}
}
