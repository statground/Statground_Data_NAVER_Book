package collector

import (
	"errors"
	"testing"
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
