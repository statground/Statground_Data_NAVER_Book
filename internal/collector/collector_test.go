package collector

import (
	"errors"
	"testing"
	"time"
)

func TestShouldSkipIngestPreflightErrorWhenOptionalAndRetryable(t *testing.T) {
	t.Setenv("KAFKA_PREFLIGHT_REQUIRED", "false")
	err := errors.New(`kafka preflight failed to connect to bootstrap broker "redacted": failed to dial: failed to open connection to redacted: dial tcp redacted: i/o timeout`)
	if !ShouldSkipIngestPreflightError(err) {
		t.Fatal("expected optional retryable Kafka preflight error to be skipped")
	}
	if got := ShortOperationalError(err); got != "kafka_timeout" {
		t.Fatalf("ShortOperationalError = %q, want kafka_timeout", got)
	}
}

func TestShouldNotSkipIngestPreflightErrorWhenRequired(t *testing.T) {
	t.Setenv("KAFKA_PREFLIGHT_REQUIRED", "true")
	err := errors.New(`kafka preflight failed to connect to bootstrap broker "redacted": failed to dial: failed to open connection to redacted: dial tcp redacted: i/o timeout`)
	if ShouldSkipIngestPreflightError(err) {
		t.Fatal("required Kafka preflight errors must remain fatal")
	}
}

func TestShouldNotSkipIngestPreflightAuthError(t *testing.T) {
	t.Setenv("KAFKA_PREFLIGHT_REQUIRED", "false")
	err := errors.New("kafka preflight failed to read metadata for topic \"book.events\": SASL authentication failed")
	if ShouldSkipIngestPreflightError(err) {
		t.Fatal("auth/config Kafka preflight errors must remain fatal")
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
	t.Setenv("KAFKA_LOG_PUBLISH_TIMEOUT_SECONDS", "90")
	t.Setenv("KAFKA_LOG_BEST_EFFORT_TIMEOUT_SECONDS", "")
	if got := searchLogBestEffortTimeout(); got != 8*time.Second {
		t.Fatalf("searchLogBestEffortTimeout = %s, want 8s", got)
	}
}

func TestSearchLogBestEffortTimeoutCanBeOverridden(t *testing.T) {
	t.Setenv("KAFKA_LOG_BEST_EFFORT_TIMEOUT_SECONDS", "2.5")
	if got := searchLogBestEffortTimeout(); got != 2500*time.Millisecond {
		t.Fatalf("searchLogBestEffortTimeout = %s, want 2.5s", got)
	}
}
