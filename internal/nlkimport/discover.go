package nlkimport

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type archivePlan struct {
	Dataset           string
	BaseName          string
	LocalPath         string
	CompressedBytes   uint64
	UncompressedBytes uint64
	Entries           []entryPlan
}

type entryPlan struct {
	Name              string
	CRC32             uint32
	UncompressedBytes uint64
}

type datasetSpec struct {
	Name string
	Stem string
}

var supportedDatasets = map[string]datasetSpec{
	"book":                   {Name: "book", Stem: "book"},
	"concept":                {Name: "concept", Stem: "Concept"},
	"person":                 {Name: "person", Stem: "Person"},
	"library":                {Name: "library", Stem: "Library"},
	"organization":           {Name: "organization", Stem: "Organization"},
	"offline":                {Name: "offline", Stem: "Offline"},
	"online":                 {Name: "online", Stem: "Online"},
	"audiovisual":            {Name: "audiovisual", Stem: "audiovisual"},
	"government_publication": {Name: "government_publication", Stem: "govermentpublication"},
	"governmentpublication":  {Name: "government_publication", Stem: "govermentpublication"},
	"govermentpublication":   {Name: "government_publication", Stem: "govermentpublication"},
	"serial":                 {Name: "serial", Stem: "serial"},
	"thesis":                 {Name: "thesis", Stem: "thesis"},
}

func NormalizeDatasets(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		key := strings.ToLower(strings.TrimSpace(value))
		if key == "" {
			continue
		}
		spec, ok := supportedDatasets[key]
		if !ok {
			return nil, safeError("unsupported_dataset")
		}
		if _, exists := seen[spec.Name]; exists {
			continue
		}
		seen[spec.Name] = struct{}{}
		out = append(out, spec.Name)
	}
	if len(out) == 0 {
		return nil, safeError("empty_datasets")
	}
	return out, nil
}

func discoverArchives(inputDir string, datasets []string, snapshot time.Time) ([]archivePlan, error) {
	if strings.TrimSpace(inputDir) == "" {
		return nil, safeError("input_directory_required")
	}
	if snapshot.IsZero() {
		return nil, safeError("snapshot_date_required")
	}
	normalized, err := NormalizeDatasets(datasets)
	if err != nil {
		return nil, err
	}
	dateSuffix := snapshot.Format("20060102")
	plans := make([]archivePlan, 0, len(normalized))
	for _, dataset := range normalized {
		spec := supportedDatasets[strings.ToLower(dataset)]
		baseName := fmt.Sprintf("%s_rdf_%s.zip", spec.Stem, dateSuffix)
		localPath := filepath.Join(inputDir, baseName)
		info, err := os.Stat(localPath)
		if err != nil || !info.Mode().IsRegular() {
			return nil, safeError("archive_unavailable")
		}
		reader, err := zip.OpenReader(localPath)
		if err != nil {
			return nil, safeError("archive_invalid")
		}
		plan := archivePlan{
			Dataset:         spec.Name,
			BaseName:        baseName,
			LocalPath:       localPath,
			CompressedBytes: uint64(info.Size()),
		}
		for _, file := range reader.File {
			if file.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(file.Name), ".rdf") {
				continue
			}
			plan.Entries = append(plan.Entries, entryPlan{
				Name:              file.Name,
				CRC32:             file.CRC32,
				UncompressedBytes: file.UncompressedSize64,
			})
			plan.UncompressedBytes += file.UncompressedSize64
		}
		_ = reader.Close()
		if len(plan.Entries) == 0 {
			return nil, safeError("archive_has_no_rdf")
		}
		sort.SliceStable(plan.Entries, func(i, j int) bool {
			return plan.Entries[i].Name < plan.Entries[j].Name
		})
		plans = append(plans, plan)
	}
	return plans, nil
}
