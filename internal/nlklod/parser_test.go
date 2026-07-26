package nlklod

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStreamResourcesAndBuildRow(t *testing.T) {
	file, err := os.Open("testdata/sample.rdf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var resources []Resource
	if err := StreamResources(file, func(index uint64, resource Resource) error {
		if index != uint64(len(resources)) {
			t.Fatalf("resource index=%d want=%d", index, len(resources))
		}
		resources = append(resources, resource)
		return nil
	}); err != nil {
		t.Fatalf("StreamResources() error=%v", err)
	}
	if len(resources) != 3 {
		t.Fatalf("resource count=%d want=3", len(resources))
	}
	if resources[0].QName != "bibo:Book" || resources[0].About != "http://lod.nl.go.kr/resource/KJU200810182" {
		t.Fatalf("unexpected first resource: %+v", resources[0])
	}

	snapshot := time.Date(2026, 5, 29, 0, 0, 0, 0, time.FixedZone("Asia/Seoul", 9*60*60))
	row, err := BuildRow(resources[0], Evidence{
		RunUUID:       "019d0000-0000-7000-8000-000000000001",
		DatasetName:   "book",
		SnapshotDate:  snapshot,
		SourceArchive: "book_rdf_20260529.zip",
		SourceEntry:   "book_rdf_20260529/book_0.rdf",
		RecordIndex:   0,
		ImportedAt:    snapshot.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("BuildRow() error=%v", err)
	}
	if got := row["resource_type"]; got != "book" {
		t.Fatalf("resource_type=%v", got)
	}
	if got := row["issued_year"]; got != uint16(2008) {
		t.Fatalf("issued_year=%T(%v), want uint16(2008)", got, got)
	}
	if got := row["issued_raw"]; got != "2008" {
		t.Fatalf("issued_raw=%v", got)
	}

	raw := row["isbn_raw"].([]string)
	isbn10 := row["isbn10"].([]string)
	isbn13 := row["isbn13"].([]string)
	canonical := row["canonical_isbns"].([]string)
	valid := row["isbn_valid"].([]int)
	for name, length := range map[string]int{
		"isbn10": len(isbn10), "isbn13": len(isbn13),
		"canonical": len(canonical), "valid": len(valid),
	} {
		if length != len(raw) {
			t.Fatalf("%s length=%d raw length=%d", name, length, len(raw))
		}
	}
	if !reflect.DeepEqual(valid, []int{1, 1, 0}) {
		t.Fatalf("isbn_valid=%v", valid)
	}
	if canonical[0] != "9788901082851" || canonical[1] != "9788901038636" || canonical[2] != "" {
		t.Fatalf("canonical_isbns=%v", canonical)
	}

	payload := row["payload_json"].(string)
	if len(payload) > MaxPayloadJSONBytes || !json.Valid([]byte(payload)) {
		t.Fatalf("payload is not bounded valid JSON: bytes=%d", len(payload))
	}
	encodedRow, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encodedRow), `"isbn_valid":[1,1,0]`) {
		t.Fatalf("isbn_valid must encode as a JSON numeric array: %s", encodedRow)
	}
}

func TestBuildRowIgnoresNLKDatePublishedForIssuedYear(t *testing.T) {
	resource := Resource{
		QName: "nlon:Book",
		Properties: map[string][]PropertyValue{
			"nlon:datePublished": {{Value: "2026-06-04T10:35:42^^http://www.w3.org/2001/XMLSchema#dateTime"}},
			"dcterms:issued":     {{Value: "民國73[1984]"}},
		},
	}
	row, err := BuildRow(resource, Evidence{
		RunUUID:       "019d0000-0000-7000-8000-000000000001",
		DatasetName:   "book",
		SnapshotDate:  time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
		SourceArchive: "book_rdf_20260529.zip",
		SourceEntry:   "book_0.rdf",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := row["issued_year"]; got != uint16(1984) {
		t.Fatalf("issued_year=%v, must come from dcterms:issued", got)
	}
}

func TestPayloadJSONFallsBackToBoundedSummary(t *testing.T) {
	resource := Resource{
		QName: "nlon:OnlineMaterial",
		About: "http://lod.nl.go.kr/resource/CNTS-1",
		Properties: map[string][]PropertyValue{
			"dcterms:abstract": {{Value: strings.Repeat("가", 60_000)}},
			"dcterms:title":    {{Value: "긴 초록"}},
		},
	}
	payload, err := boundedPayloadJSON(resource, resource.About)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > MaxPayloadJSONBytes {
		t.Fatalf("payload bytes=%d", len(payload))
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("invalid payload JSON: %v", err)
	}
	if decoded["truncated"] != true {
		t.Fatalf("expected truncated summary, got %v", decoded)
	}
}

func TestResourceTypeUsesRDFResourceInsteadOfDatasetName(t *testing.T) {
	resource := Resource{
		QName:    "rdf:Description",
		RDFTypes: []string{"http://lod.nl.go.kr/ontology/Library"},
	}
	if got := normalizeResourceType(resource, "Organization"); got != "library" {
		t.Fatalf("resource type=%q want=library", got)
	}
}

func TestImmutableLineageProducesDeterministicUUIDv7HashAndVersion(t *testing.T) {
	resource := Resource{
		QName: "bibo:Book",
		About: "http://lod.nl.go.kr/resource/KMO1",
		Properties: map[string][]PropertyValue{
			"dcterms:title": {{Value: "같은 책"}},
			"bibo:isbn":     {{Value: "9788901082851"}},
		},
	}
	evidence := Evidence{
		RunUUID:       "019d0000-0000-7000-8000-000000000001",
		DatasetName:   "book",
		SnapshotDate:  time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC),
		SourceArchive: "book_rdf_20260529.zip",
		SourceEntry:   "book_0.rdf",
		RecordIndex:   7,
	}
	row1, err := BuildRow(resource, evidence)
	if err != nil {
		t.Fatal(err)
	}
	evidence.RunUUID = "019d0000-0000-7000-8000-000000000099"
	evidence.ImportedAt = time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	row2, err := BuildRow(resource, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if row1["uuid"] != row2["uuid"] {
		t.Fatalf("lineage UUID differs: %v/%v", row1["uuid"], row2["uuid"])
	}
	if row1["content_hash"] != row2["content_hash"] || row1["version"] != row2["version"] {
		t.Fatalf("deterministic fields differ: hash %v/%v version %v/%v", row1["content_hash"], row2["content_hash"], row1["version"], row2["version"])
	}
	uuid := row1["uuid"].(string)
	if len(uuid) != 36 || uuid[14] != '7' || !strings.ContainsRune("89ab", rune(uuid[19])) {
		t.Fatalf("deterministic observation UUID is not RFC 9562 UUIDv7: %q", uuid)
	}
}
