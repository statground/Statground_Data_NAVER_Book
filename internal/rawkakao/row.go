package rawkakao

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"statground_naver_book_go/internal/bookmodel"
	"statground_naver_book_go/internal/util"
)

type Evidence struct {
	RunUUID       string
	RequestUUID   string
	Mode          string
	Query         string
	Sort          string
	Target        string
	Page          int
	Size          int
	TotalCount    int
	PageableCount int
	IsEnd         bool
	Source        string
	LineageTopic  string
	CollectedAt   time.Time
}

type IDGenerator func() string

func BuildRow(document bookmodel.BookDocument, evidence Evidence, idGenerator IDGenerator) (map[string]any, error) {
	if idGenerator == nil {
		idGenerator = util.UUIDv7
	}
	if strings.TrimSpace(evidence.RunUUID) == "" || strings.TrimSpace(evidence.RequestUUID) == "" {
		return nil, fmt.Errorf("run_uuid and request_uuid are required")
	}
	if evidence.CollectedAt.IsZero() {
		evidence.CollectedAt = util.NowKST()
	}
	document = bookmodel.NormalizeDocument(document)
	contentHash, err := ContentHash(document)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}

	nowString := util.FormatCHDateTime64Millis(evidence.CollectedAt)
	uuid := idGenerator()
	eventUUID := idGenerator()
	version := uint64(evidence.CollectedAt.UnixMicro())
	if version == 0 {
		version = 1
	}

	var publishedAt any
	if document.PublishedAt != nil {
		publishedAt = util.FormatCHDateTime64Millis(*document.PublishedAt)
	}
	return map[string]any{
		"uuid":               uuid,
		"provider":           "kakao",
		"version":            version,
		"created_at":         nowString,
		"updated_at":         nowString,
		"collected_at":       nowString,
		"title":              document.Title,
		"contents":           document.Contents,
		"source_url":         document.SourceURL,
		"thumbnail_url":      document.ThumbnailURL,
		"isbn_raw":           document.ISBNRaw,
		"isbn10":             document.ISBN10,
		"isbn13":             document.ISBN13,
		"canonical_isbn":     document.CanonicalISBN,
		"isbn_aliases":       ISBNAliases(document),
		"isbn_valid":         boolUInt8(document.ISBNValid),
		"published_at":       publishedAt,
		"authors":            append([]string(nil), document.Authors...),
		"publisher":          document.Publisher,
		"translators":        append([]string(nil), document.Translators...),
		"list_price":         document.ListPrice,
		"sale_price":         document.SalePrice,
		"status_raw":         document.StatusRaw,
		"content_hash":       contentHash,
		"search_mode":        evidence.Mode,
		"search_query":       evidence.Query,
		"search_sort":        evidence.Sort,
		"search_target":      evidence.Target,
		"search_page":        evidence.Page,
		"search_size":        evidence.Size,
		"api_total_count":    nonNegativeInt(evidence.TotalCount),
		"api_pageable_count": nonNegativeInt(evidence.PageableCount),
		"api_is_end":         boolUInt8(evidence.IsEnd),
		"source":             defaultString(evidence.Source, "github_actions"),
		"run_uuid":           evidence.RunUUID,
		"request_uuid":       evidence.RequestUUID,
		"event_uuid":         eventUUID,
		"lineage_topic":      defaultString(evidence.LineageTopic, "direct.statground_book.kakao_book"),
		"lineage_partition":  0,
		"lineage_offset":     0,
		"payload":            string(payload),
		"ingested_at":        nowString,
	}, nil
}

func ContentHash(document bookmodel.BookDocument) (string, error) {
	document = bookmodel.NormalizeDocument(document)
	payload, err := json.Marshal(struct {
		Title         string
		Contents      string
		SourceURL     string
		ThumbnailURL  string
		ISBNRaw       string
		ISBN10        string
		ISBN13        string
		CanonicalISBN string
		PublishedAt   *time.Time
		Authors       []string
		Publisher     string
		Translators   []string
		ListPrice     *uint64
		SalePrice     *uint64
		StatusRaw     string
	}{
		Title:         document.Title,
		Contents:      document.Contents,
		SourceURL:     document.SourceURL,
		ThumbnailURL:  document.ThumbnailURL,
		ISBNRaw:       document.ISBNRaw,
		ISBN10:        document.ISBN10,
		ISBN13:        document.ISBN13,
		CanonicalISBN: document.CanonicalISBN,
		PublishedAt:   document.PublishedAt,
		Authors:       document.Authors,
		Publisher:     document.Publisher,
		Translators:   document.Translators,
		ListPrice:     document.ListPrice,
		SalePrice:     document.SalePrice,
		StatusRaw:     document.StatusRaw,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func ISBNAliases(document bookmodel.BookDocument) []string {
	aliases := make([]string, 0, 2)
	seen := map[string]struct{}{document.CanonicalISBN: {}}
	for _, value := range []string{document.ISBN10, document.ISBN13} {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		aliases = append(aliases, value)
	}
	return aliases
}

func boolUInt8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func nonNegativeInt(value int) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

func defaultString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
