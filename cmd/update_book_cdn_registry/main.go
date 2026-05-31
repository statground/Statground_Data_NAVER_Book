package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type exportReport struct {
	Language     string                  `json:"language"`
	SinceHours   int                     `json:"since_hours"`
	Files        int                     `json:"files"`
	Changed      int                     `json:"changed"`
	GeneratedAt  string                  `json:"generated_at"`
	RegistryPath string                  `json:"registry_path"`
	Shards       map[string]*shardReport `json:"shards"`
}

type shardReport struct {
	Prefix  string `json:"prefix"`
	Repo    string `json:"repo"`
	Files   int    `json:"files"`
	New     int    `json:"new"`
	Changed int    `json:"changed"`
}

type registry struct {
	Schema             string          `json:"schema"`
	Language           string          `json:"language"`
	GeneratedAt        string          `json:"generated_at"`
	PathPattern        string          `json:"path_pattern"`
	DetailPathPattern  string          `json:"detail_path_pattern"`
	ListArchivePattern string          `json:"list_archive_pattern"`
	Shards             []registryShard `json:"shards"`
	TotalItems         uint64          `json:"total_items"`
	ShardCount         int             `json:"shard_count"`
	RegistryNote       string          `json:"registry_note,omitempty"`
}

type registryShard struct {
	Prefix    string `json:"prefix"`
	Repo      string `json:"repo"`
	Branch    string `json:"branch"`
	CommitSHA string `json:"commit_sha"`
	BaseURL   string `json:"base_url"`
	ItemCount uint64 `json:"item_count"`
	UpdatedAt string `json:"updated_at"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	reportPath := env("BOOK_CDN_REPORT_PATH", "book_cdn_export_report.json")
	root := env("BOOK_CDN_REGISTRY_ROOT", "Statground_CDN_Book")
	branch := env("BOOK_CDN_BRANCH", "main")
	commits := parseAssignments(os.Getenv("BOOK_CDN_SHARD_COMMITS"))
	if len(commits) == 0 {
		return errors.New("BOOK_CDN_SHARD_COMMITS is required")
	}
	reportBytes, err := os.ReadFile(reportPath)
	if err != nil {
		return err
	}
	var report exportReport
	if err := json.Unmarshal(reportBytes, &report); err != nil {
		return err
	}
	if report.Language == "" {
		report.Language = "ko"
	}
	if report.RegistryPath == "" {
		report.RegistryPath = "books/" + report.Language + "/registry.json"
	}
	registryPath := filepath.Join(root, filepath.FromSlash(report.RegistryPath))
	reg := readRegistry(registryPath, report.Language)
	now := time.Now().UTC().Format(time.RFC3339)
	existing := map[string]registryShard{}
	for _, shard := range reg.Shards {
		if shard.Prefix != "" {
			existing[shard.Prefix] = shard
		}
	}
	for prefix, shardReport := range report.Shards {
		commit := strings.TrimSpace(commits[prefix])
		if commit == "" || shardReport.Files == 0 {
			continue
		}
		repo := strings.TrimSpace(shardReport.Repo)
		if repo == "" {
			continue
		}
		itemCount := uint64(shardReport.Files)
		if old, ok := existing[prefix]; ok {
			if report.SinceHours > 0 {
				itemCount = old.ItemCount + uint64(shardReport.New)
			} else if old.ItemCount > itemCount && shardReport.Changed == 0 {
				itemCount = old.ItemCount
			}
		}
		existing[prefix] = registryShard{
			Prefix:    prefix,
			Repo:      repo,
			Branch:    branch,
			CommitSHA: commit,
			BaseURL:   "https://cdn.jsdelivr.net/gh/" + repo + "@" + commit,
			ItemCount: itemCount,
			UpdatedAt: now,
		}
	}
	prefixes := make([]string, 0, len(existing))
	for prefix := range existing {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	reg.Shards = make([]registryShard, 0, len(prefixes))
	reg.TotalItems = 0
	for _, prefix := range prefixes {
		shard := existing[prefix]
		reg.Shards = append(reg.Shards, shard)
		reg.TotalItems += shard.ItemCount
	}
	reg.Schema = "statground.book.registry.v1"
	reg.Language = report.Language
	reg.GeneratedAt = now
	reg.PathPattern = "books/{language}/items/{h0}/{h1}/{h2}/{h3}/{isbn}.json"
	reg.DetailPathPattern = "books/{language}/items/{h0}/{h1}/{h2}/{h3}/{isbn}.json"
	reg.ListArchivePattern = "books/{language}/lists/{yyyy}/{mm}/{dd}/{kind}.json"
	reg.ShardCount = len(reg.Shards)
	reg.RegistryNote = "Root repo contains registry and future list archives; ISBN detail payloads are encrypted in detail shard repositories."

	body, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(registryPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(registryPath, append(body, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Println(string(body))
	return nil
}

func readRegistry(path, language string) registry {
	raw, err := os.ReadFile(path)
	if err != nil {
		return registry{Language: language}
	}
	var reg registry
	if err := json.Unmarshal(raw, &reg); err != nil {
		return registry{Language: language}
	}
	return reg
}

func parseAssignments(raw string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.ToLower(strings.TrimSpace(value))
		if isHexPrefix(key) && isHexSHA(value) {
			out[key] = value
		}
	}
	return out
}

func isHexPrefix(value string) bool {
	if value == "" || len(value) > 8 {
		return false
	}
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return false
	}
	return true
}

func isHexSHA(value string) bool {
	if len(value) < 7 || len(value) > 40 {
		return false
	}
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return false
	}
	return true
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
