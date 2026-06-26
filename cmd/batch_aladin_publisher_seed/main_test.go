package main

import (
	"fmt"
	"testing"
)

func TestCleanPublisherSeedsSkipsBroadAndDuplicateSeeds(t *testing.T) {
	got, skipped := cleanPublisherSeeds([]string{
		" iN ",
		"다함",
		"다함",
		"출판iN",
		"RHK",
		"IT",
	})

	want := []string{"다함", "출판iN", "RHK"}
	if skipped != 3 {
		t.Fatalf("skipped = %d, want 3", skipped)
	}
	if len(got) != len(want) {
		t.Fatalf("cleaned publishers = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cleaned publishers = %v, want %v", got, want)
		}
	}
}

func TestShouldSkipAladinPublisherSeedForTemporaryErrorByDefault(t *testing.T) {
	t.Setenv("ALADIN_PUBLISHER_SEED_REQUIRED", "")
	err := fmt.Errorf("aladin http 503: Service Unavailable")
	if !shouldSkipAladinPublisherSeed(err) {
		t.Fatal("expected temporary Aladin 503 to be skippable by default")
	}
}

func TestShouldNotSkipAladinPublisherSeedWhenRequired(t *testing.T) {
	t.Setenv("ALADIN_PUBLISHER_SEED_REQUIRED", "true")
	err := fmt.Errorf("aladin http 503: Service Unavailable")
	if shouldSkipAladinPublisherSeed(err) {
		t.Fatal("expected required Aladin publisher seed to fail on 503")
	}
}

func TestTemporaryAladinErrorClassification(t *testing.T) {
	cases := []string{
		"aladin http 429: Too Many Requests",
		"aladin http 502: Bad Gateway",
		"aladin http 503: Service Unavailable",
		"Get https://www.aladin.co.kr: context deadline exceeded",
		"Post https://www.aladin.co.kr: EOF",
	}
	for _, tc := range cases {
		if !isTemporaryAladinError(fmt.Errorf("%s", tc)) {
			t.Fatalf("expected temporary Aladin error for %q", tc)
		}
	}
	if isTemporaryAladinError(fmt.Errorf("failed to detect Aladin last page")) {
		t.Fatal("parse/contract errors should not be treated as temporary")
	}
}

func TestAladinPublisherCacheRequiredDefaultsOptional(t *testing.T) {
	t.Setenv("ALADIN_PUBLISHER_CACHE_REQUIRED", "")
	if aladinPublisherCacheRequired() {
		t.Fatal("publisher cache should be optional by default")
	}
	t.Setenv("ALADIN_PUBLISHER_CACHE_REQUIRED", "true")
	if !aladinPublisherCacheRequired() {
		t.Fatal("publisher cache should be required when explicitly enabled")
	}
}

func TestAladinPublisherCollectionRequiredDefaultsOptional(t *testing.T) {
	t.Setenv("ALADIN_PUBLISHER_COLLECTION_REQUIRED", "")
	if aladinPublisherCollectionRequired() {
		t.Fatal("publisher collection should be optional by default")
	}
	t.Setenv("ALADIN_PUBLISHER_COLLECTION_REQUIRED", "required")
	if !aladinPublisherCollectionRequired() {
		t.Fatal("publisher collection should be required when explicitly enabled")
	}
}

func TestShortOperationalErrorClassifiesAccessDenied(t *testing.T) {
	err := fmt.Errorf("clickhouse http 500: DB::Exception: Not enough privileges. (ACCESS_DENIED)")
	if got := shortOperationalError(err); got != "access_denied" {
		t.Fatalf("shortOperationalError = %q, want access_denied", got)
	}
}

func TestShortOperationalErrorClassifiesKafkaLeaderUnavailable(t *testing.T) {
	err := fmt.Errorf(`kafka.(*Client).Produce: fetch request error: topic partition has no leader (topic="book.events" partition=2)`)
	if got := shortOperationalError(err); got != "kafka_leader_unavailable" {
		t.Fatalf("shortOperationalError = %q, want kafka_leader_unavailable", got)
	}
}
