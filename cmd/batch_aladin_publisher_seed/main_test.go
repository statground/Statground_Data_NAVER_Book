package main

import "testing"

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
