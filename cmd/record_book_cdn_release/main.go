package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
)

type registry struct {
	Language   string `json:"language"`
	ShardCount int    `json:"shard_count"`
	TotalItems uint64 `json:"total_items"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	client, err := ch.NewFromEnv()
	if err != nil {
		return err
	}
	commitSHA := normalizeSHA(envx.String("BOOK_CDN_ROOT_COMMIT_SHA", ""))
	if commitSHA == "" {
		return errors.New("BOOK_CDN_ROOT_COMMIT_SHA is required")
	}
	repo := envx.String("BOOK_CDN_ROOT_REPO", "statground/Statground_CDN_Book")
	branch := envx.String("BOOK_CDN_BRANCH", "main")
	registryPath := envx.String("BOOK_CDN_REGISTRY_PATH", "books/ko/registry.json")
	registryFile := envx.String("BOOK_CDN_REGISTRY_FILE", "Statground_CDN_Book/"+registryPath)
	raw, err := os.ReadFile(registryFile)
	if err != nil {
		return err
	}
	var reg registry
	if err := json.Unmarshal(raw, &reg); err != nil {
		return err
	}
	if strings.TrimSpace(reg.Language) == "" {
		reg.Language = "ko"
	}
	baseURL := "https://cdn.jsdelivr.net/gh/" + repo + "@" + commitSHA
	now := time.Now()
	publishedAt := now.Format("2006-01-02 15:04:05.000")
	releaseID := "book-cdn:" + reg.Language + ":" + commitSHA
	payload := map[string]any{
		"release_id":    releaseID,
		"language":      reg.Language,
		"cdn_repo":      repo,
		"cdn_branch":    branch,
		"commit_sha":    commitSHA,
		"base_url":      baseURL,
		"registry_path": registryPath,
		"shard_count":   reg.ShardCount,
		"item_count":    reg.TotalItems,
		"payload_json":  compactJSON(reg),
		"published_at":  publishedAt,
		"active":        1,
		"version":       uint64(now.UnixMilli()),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	sql := "INSERT INTO Data_Book_NAVER_Service.book_cdn_release_log FORMAT JSONEachRow\n" + string(body) + "\n"
	if err := client.Exec(sql); err != nil {
		return err
	}
	fmt.Println(string(body))
	return nil
}

func compactJSON(v any) string {
	body, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(body)
}

func normalizeSHA(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) < 7 || len(value) > 40 {
		return ""
	}
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return ""
	}
	return value
}
