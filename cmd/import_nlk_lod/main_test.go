package main

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestDefaultDatasetsExcludeAggregateOfflineAndOnlineArchives(t *testing.T) {
	if defaultDatasets != "book,concept,person,library" {
		t.Fatalf("default datasets=%q", defaultDatasets)
	}
	for _, forbidden := range []string{"offline", "online"} {
		if strings.Contains(defaultDatasets, forbidden) {
			t.Fatalf("aggregate dataset %s must be opt-in", forbidden)
		}
	}
}

func TestSplitDatasets(t *testing.T) {
	got := splitDatasets("book, Concept;Person\nLibrary")
	want := []string{"book", " Concept", "Person", "Library"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitDatasets=%q want=%q", got, want)
	}
}

func TestInvalidSnapshotErrorDoesNotEchoInputPath(t *testing.T) {
	const sensitivePath = "/private/operator/archive"
	err := run(context.Background(), []string{
		"--input-dir", sensitivePath,
		"--datasets", "book",
		"--snapshot-date", "not-a-date",
		"--dry-run=true",
	})
	if err == nil {
		t.Fatal("expected invalid snapshot error")
	}
	if strings.Contains(err.Error(), sensitivePath) || err.Error() != "NLK LOD import failed category=invalid_snapshot_date" {
		t.Fatalf("unsafe error=%q", err)
	}
}

func TestInvalidEntryShardFlagsFailBeforeInputAccess(t *testing.T) {
	tests := [][]string{
		{"--entry-shard-count=0", "--entry-shard-index=0"},
		{"--entry-shard-count=17", "--entry-shard-index=0"},
		{"--entry-shard-count=2", "--entry-shard-index=-1"},
		{"--entry-shard-count=2", "--entry-shard-index=2"},
	}
	for _, args := range tests {
		err := run(context.Background(), append(args, "--dry-run=true"))
		if err == nil || err.Error() != "NLK LOD import failed category=invalid_flags" {
			t.Fatalf("args=%q error=%v", args, err)
		}
	}
}

func TestFullDriveWorkflowRequiresCoverageBeforePublication(t *testing.T) {
	body, err := os.ReadFile("../../.github/workflows/nlk_drive_import.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(body)
	for _, required := range []string{"group: statground-book-writer-refresh", "cancel-in-progress: false", "queue: max", "--expected-files 208", "--expected-bytes 88736746306", `NLK_PRESSURE_GATE_ENABLED: "true"`, "--resume=true", "--manifest-out", "book-catalog-publish-state.json"} {
		if !strings.Contains(workflow, required) {
			t.Errorf("missing complete import contract %s", required)
		}
	}
	steps := []string{"Verify all 208 source files", "Gate raw import capacity", "Stream all RDF files", "Gate service projection capacity", "Project every completed dataset", "Gate public catalog capacity", "Verify complete coverage and publish"}
	previous := -1
	for _, step := range steps {
		index := strings.Index(workflow, step)
		if index <= previous {
			t.Fatalf("pipeline order violates complete publication at %s", step)
		}
		previous = index
	}
	if strings.Contains(workflow, "--max-records") || strings.Contains(workflow, "--entry-shard") {
		t.Fatal("partial source scope must not reach full publication workflow")
	}
}
