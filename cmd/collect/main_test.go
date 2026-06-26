package main

import (
	"errors"
	"math/rand"
	"strings"
	"testing"
)

func TestRunTermCollectionContinuesRetryableErrorsWhenOptional(t *testing.T) {
	t.Setenv("COLLECT_TERM_REQUIRED", "false")
	t.Setenv("COLLECT_SLEEP_MAX", "0")

	calls := make([]string, 0, 3)
	err := runTermCollection("author", []string{"first", "leader", "last"}, rand.New(rand.NewSource(1)), func(term string) error {
		calls = append(calls, term)
		if term == "leader" {
			return errors.New(`Kafka write errors (1/1), errors: kafka.(*Client).Produce: fetch request error: topic partition has no leader (topic="book.events" partition=2)`)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runTermCollection returned error: %v", err)
	}
	if got := strings.Join(calls, ","); got != "first,leader,last" {
		t.Fatalf("calls = %q, want all terms to be attempted", got)
	}
}

func TestRunTermCollectionFailsWhenAllOptionalTermsFail(t *testing.T) {
	t.Setenv("COLLECT_TERM_REQUIRED", "false")
	t.Setenv("COLLECT_SLEEP_MAX", "0")

	err := runTermCollection("author", []string{"a", "b"}, rand.New(rand.NewSource(1)), func(string) error {
		return errors.New(`Kafka write errors (1/1), errors: kafka.(*Client).Produce: fetch request error: topic partition has no leader (topic="book.events" partition=2)`)
	})
	if err == nil {
		t.Fatal("expected all-failed optional collection to return an error")
	}
	if !strings.Contains(err.Error(), "no successful terms") && !strings.Contains(err.Error(), "no successes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunTermCollectionStopsOnRequiredRetryableError(t *testing.T) {
	t.Setenv("COLLECT_TERM_REQUIRED", "true")
	t.Setenv("COLLECT_SLEEP_MAX", "0")

	calls := 0
	err := runTermCollection("author", []string{"a", "b"}, rand.New(rand.NewSource(1)), func(string) error {
		calls++
		return errors.New(`Kafka write errors (1/1), errors: kafka.(*Client).Produce: fetch request error: topic partition has no leader (topic="book.events" partition=2)`)
	})
	if err == nil {
		t.Fatal("expected required collection to return the first retryable error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}
