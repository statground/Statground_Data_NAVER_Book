package collector

import (
	"errors"
	"testing"
	"time"
)

func TestShouldSkipIngestPreflightErrorWhenOptionalAndRetryable(t *testing.T) {
	t.Setenv("DB_PREFLIGHT_REQUIRED", "false")
	err := errors.New("clickhouse http 500: DB::Exception: TOO_MANY_SIMULTANEOUS_QUERIES")
	if !ShouldSkipIngestPreflightError(err) {
		t.Fatal("expected optional retryable DB preflight error to be skipped")
	}
	if got := ShortOperationalError(err); got != "clickhouse_too_many_queries" {
		t.Fatalf("ShortOperationalError = %q, want clickhouse_too_many_queries", got)
	}
}

func TestShouldNotSkipIngestPreflightErrorWhenRequired(t *testing.T) {
	t.Setenv("DB_PREFLIGHT_REQUIRED", "true")
	err := errors.New("clickhouse http 500: DB::Exception: TOO_MANY_SIMULTANEOUS_QUERIES")
	if ShouldSkipIngestPreflightError(err) {
		t.Fatal("required DB preflight errors must remain fatal")
	}
}

func TestShouldNotSkipIngestPreflightPrivilegeError(t *testing.T) {
	t.Setenv("DB_PREFLIGHT_REQUIRED", "false")
	err := errors.New("clickhouse http 500: DB::Exception: Not enough privileges. (ACCESS_DENIED)")
	if ShouldSkipIngestPreflightError(err) {
		t.Fatal("privilege/config DB preflight errors must remain fatal")
	}
}

func TestSearchLogRequiredDefaultsStrict(t *testing.T) {
	if !searchLogRequired() {
		t.Fatal("search log publish should be required by default")
	}
}

func TestSearchLogRequiredCanBeBestEffort(t *testing.T) {
	t.Setenv("SEARCH_LOG_REQUIRED", "false")
	if searchLogRequired() {
		t.Fatal("SEARCH_LOG_REQUIRED=false should make search log publish best-effort")
	}
}

func TestSearchLogBestEffortTimeoutCapsLongLogTimeout(t *testing.T) {
	t.Setenv("DB_LOG_WRITE_TIMEOUT_SECONDS", "90")
	t.Setenv("DB_LOG_BEST_EFFORT_TIMEOUT_SECONDS", "")
	if got := searchLogBestEffortTimeout(); got != 8*time.Second {
		t.Fatalf("searchLogBestEffortTimeout = %s, want 8s", got)
	}
}

func TestSearchLogBestEffortTimeoutCanBeOverridden(t *testing.T) {
	t.Setenv("DB_LOG_BEST_EFFORT_TIMEOUT_SECONDS", "2.5")
	if got := searchLogBestEffortTimeout(); got != 2500*time.Millisecond {
		t.Fatalf("searchLogBestEffortTimeout = %s, want 2.5s", got)
	}
}
