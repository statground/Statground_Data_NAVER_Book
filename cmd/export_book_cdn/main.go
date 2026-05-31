package main

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/util"
)

const (
	encryptedSchema   = "statground.book.encrypted.v1"
	itemSchema        = "statground.book.item.v1"
	keyPurpose        = "statground.book.cdn.v1"
	defaultTable      = "Data_Book_NAVER_Service.naver_book_latest"
	defaultLanguage   = "ko"
	defaultShardOwner = "statground/Statground_CDN_Book_Detail_"
)

var reISBNToken = regexp.MustCompile(`[0-9Xx][0-9Xx\-\s]{6,24}[0-9Xx]`)

type encryptedDocument struct {
	Schema      string `json:"schema"`
	Alg         string `json:"alg"`
	Compression string `json:"compression,omitempty"`
	Nonce       string `json:"nonce"`
	Ciphertext  string `json:"ciphertext"`
	PayloadTag  string `json:"payload_tag"`
}

type bookPayload struct {
	Schema        string   `json:"schema"`
	Provider      string   `json:"provider"`
	CanonicalISBN string   `json:"canonical_isbn"`
	ISBN          string   `json:"isbn"`
	ISBNAliases   []string `json:"isbn_aliases"`
	ISBNRaw       string   `json:"isbn_raw"`
	UUID          string   `json:"uuid,omitempty"`
	Title         string   `json:"title,omitempty"`
	Author        string   `json:"author,omitempty"`
	Publisher     string   `json:"publisher,omitempty"`
	Pubdate       string   `json:"pubdate,omitempty"`
	Description   string   `json:"description,omitempty"`
	CoverURL      string   `json:"cover_url,omitempty"`
	Link          string   `json:"link,omitempty"`
	Version       uint64   `json:"version,omitempty"`
	UpdatedAt     string   `json:"updated_at,omitempty"`
}

type shardReport struct {
	Prefix    string `json:"prefix"`
	Repo      string `json:"repo"`
	Dir       string `json:"dir"`
	Files     int    `json:"files"`
	New       int    `json:"new"`
	Changed   int    `json:"changed"`
	Unchanged int    `json:"unchanged"`
}

type exportReport struct {
	Schema        string                  `json:"schema"`
	Language      string                  `json:"language"`
	Table         string                  `json:"table"`
	SinceHours    int                     `json:"since_hours"`
	MaxRows       int                     `json:"max_rows"`
	Rows          int                     `json:"rows"`
	Files         int                     `json:"files"`
	New           int                     `json:"new"`
	Changed       int                     `json:"changed"`
	Unchanged     int                     `json:"unchanged"`
	SkippedNoISBN int                     `json:"skipped_no_isbn"`
	GeneratedAt   string                  `json:"generated_at"`
	RegistryPath  string                  `json:"registry_path"`
	Shards        map[string]*shardReport `json:"shards"`
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
	secret := strings.TrimSpace(os.Getenv("STATGROUND_BOOK_CONTENT_KEY"))
	if secret == "" {
		return errors.New("STATGROUND_BOOK_CONTENT_KEY is required")
	}
	key := deriveKey(secret)
	table := safeIdentifierPath(envx.String("BOOK_CDN_TABLE", defaultTable))
	if table == "" {
		return errors.New("BOOK_CDN_TABLE is invalid")
	}
	language := strings.ToLower(strings.TrimSpace(envx.String("BOOK_CDN_LANGUAGE", defaultLanguage)))
	if language == "" {
		language = defaultLanguage
	}
	outputRoot := envx.String("BOOK_CDN_OUTPUT_ROOT", ".")
	dirTemplate := envx.String("BOOK_CDN_SHARD_DIR_TEMPLATE", "Statground_CDN_Book_%s")
	repoPrefix := envx.String("BOOK_CDN_SHARD_REPO_PREFIX", defaultShardOwner)
	prefixes := parsePrefixes(envx.String("BOOK_CDN_HASH_PREFIXES", "0123456789abcdef"))
	sinceHours := envx.Int("BOOK_CDN_SINCE_HOURS", 8)
	maxRows := envx.Int("BOOK_CDN_MAX_ROWS", 0)
	reportPath := envx.String("BOOK_CDN_REPORT_PATH", "book_cdn_export_report.json")

	report := exportReport{
		Schema:       "statground.book-cdn.export-report.v1",
		Language:     language,
		Table:        table,
		SinceHours:   sinceHours,
		MaxRows:      maxRows,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		RegistryPath: fmt.Sprintf("books/%s/registry.json", language),
		Shards:       map[string]*shardReport{},
	}
	for prefix := range prefixes {
		dir := filepath.Join(outputRoot, fmt.Sprintf(dirTemplate, prefix))
		report.Shards[prefix] = &shardReport{
			Prefix: prefix,
			Repo:   repoPrefix + prefix,
			Dir:    dir,
		}
	}

	sql := buildExportSQL(table, sinceHours, maxRows, prefixes)
	err = client.QueryJSONEachRowStream(sql, func(row map[string]any) error {
		report.Rows++
		rawISBN := rowString(row, "isbn")
		aliases := normalizeISBNs(rawISBN)
		if len(aliases) == 0 {
			report.SkippedNoISBN++
			return nil
		}
		payload := payloadFromRow(row, aliases)
		plain, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		payloadTag := payloadHMACTag(plain, key)
		for _, isbn := range aliases {
			hash := sha256Hex([]byte(isbn))
			prefix, ok := shardPrefixForHash(hash, prefixes)
			if !ok {
				continue
			}
			shard, ok := report.Shards[prefix]
			if !ok {
				continue
			}
			relPath := itemRelPath(language, hash, isbn)
			dst := filepath.Join(shard.Dir, filepath.FromSlash(relPath))
			changed, isNew, err := writeEncryptedIfChanged(dst, relPath, plain, payloadTag, key)
			if err != nil {
				return err
			}
			report.Files++
			shard.Files++
			if isNew {
				report.New++
				shard.New++
			}
			if changed {
				report.Changed++
				shard.Changed++
			} else {
				report.Unchanged++
				shard.Unchanged++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	reportBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(reportPath, append(reportBytes, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Println(string(reportBytes))
	return nil
}

func buildExportSQL(table string, sinceHours, maxRows int, prefixes map[string]bool) string {
	where := "WHERE notEmpty(isbn)"
	if sinceHours > 0 {
		where += fmt.Sprintf("\n  AND toString(updated_at) >= formatDateTime(now64(3, 'Asia/Seoul') - INTERVAL %d HOUR, '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul')", sinceHours)
	}
	limit := ""
	if maxRows > 0 {
		limit = fmt.Sprintf("\nLIMIT %d", maxRows)
	}
	return fmt.Sprintf(`
SELECT
    base64Encode(ifNull(toString(uuid), '')) AS uuid_b64,
    base64Encode(trim(BOTH ' ' FROM ifNull(isbn, ''))) AS isbn_b64,
    base64Encode(trim(BOTH ' ' FROM ifNull(title, ''))) AS title_b64,
    base64Encode(trim(BOTH ' ' FROM ifNull(author, ''))) AS author_b64,
    base64Encode(trim(BOTH ' ' FROM ifNull(publisher, ''))) AS publisher_b64,
    base64Encode(trim(BOTH ' ' FROM ifNull(pubdate, ''))) AS pubdate_b64,
    base64Encode(trim(BOTH ' ' FROM ifNull(description, ''))) AS description_b64,
    base64Encode(trim(BOTH ' ' FROM ifNull(image, ''))) AS cover_url_b64,
    base64Encode(trim(BOTH ' ' FROM ifNull(link, ''))) AS link_b64,
    toUInt64(version) AS version,
    base64Encode(left(toString(updated_at), 19)) AS updated_at_b64
FROM %s
%s
%s
SETTINGS max_threads = 2, max_execution_time = 900, timeout_overflow_mode = 'break'
`, table, where, limit)
}

func payloadFromRow(row map[string]any, aliases []string) bookPayload {
	canonical := aliases[0]
	for _, alias := range aliases {
		if len(alias) == 13 {
			canonical = alias
			break
		}
	}
	return bookPayload{
		Schema:        itemSchema,
		Provider:      "naver",
		CanonicalISBN: canonical,
		ISBN:          canonical,
		ISBNAliases:   aliases,
		ISBNRaw:       rowString(row, "isbn"),
		UUID:          rowString(row, "uuid"),
		Title:         rowString(row, "title"),
		Author:        rowString(row, "author"),
		Publisher:     rowString(row, "publisher"),
		Pubdate:       rowString(row, "pubdate"),
		Description:   rowString(row, "description"),
		CoverURL:      rowString(row, "cover_url"),
		Link:          rowString(row, "link"),
		Version:       toUint64(row["version"]),
		UpdatedAt:     rowString(row, "updated_at"),
	}
}

func rowString(row map[string]any, key string) string {
	if value := util.ToString(row[key+"_b64"]); strings.TrimSpace(value) != "" {
		if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
			return string(decoded)
		}
	}
	return util.ToString(row[key])
}

func writeEncryptedIfChanged(path, aadPath string, plain []byte, payloadTag string, key []byte) (bool, bool, error) {
	existingTag, exists := existingPayloadTag(path)
	if exists && existingTag == payloadTag {
		return false, false, nil
	}
	doc, err := encryptDocument(plain, key, aadPath, payloadTag)
	if err != nil {
		return false, false, err
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return false, false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, false, err
	}
	return true, !exists, os.WriteFile(path, append(body, '\n'), 0o644)
}

func existingPayloadTag(path string) (string, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var doc encryptedDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", false
	}
	if doc.Schema != encryptedSchema {
		return "", false
	}
	return strings.TrimSpace(doc.PayloadTag), true
}

func encryptDocument(plain, key []byte, aadPath, payloadTag string) (encryptedDocument, error) {
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(plain); err != nil {
		return encryptedDocument{}, err
	}
	if err := gz.Close(); err != nil {
		return encryptedDocument{}, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return encryptedDocument{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return encryptedDocument{}, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return encryptedDocument{}, err
	}
	ciphertext := gcm.Seal(nil, nonce, compressed.Bytes(), []byte(normalizePath(aadPath)))
	return encryptedDocument{
		Schema:      encryptedSchema,
		Alg:         "AES-256-GCM",
		Compression: "gzip",
		Nonce:       base64.RawURLEncoding.EncodeToString(nonce),
		Ciphertext:  base64.RawURLEncoding.EncodeToString(ciphertext),
		PayloadTag:  payloadTag,
	}, nil
}

func deriveKey(secret string) []byte {
	sum := sha256.Sum256([]byte(strings.TrimSpace(secret) + "\x00" + keyPurpose))
	return sum[:]
}

func itemRelPath(language, hash, isbn string) string {
	return fmt.Sprintf("books/%s/items/%s/%s/%s/%s/%s.json", language, hash[:1], hash[1:2], hash[2:3], hash[3:4], isbn)
}

func normalizeISBNs(raw string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, 2)
	for _, match := range reISBNToken.FindAllString(raw, -1) {
		cleaned := strings.ToUpper(match)
		cleaned = strings.ReplaceAll(cleaned, "-", "")
		cleaned = strings.Join(strings.Fields(cleaned), "")
		if !validISBNToken(cleaned) || seen[cleaned] {
			continue
		}
		seen[cleaned] = true
		out = append(out, cleaned)
	}
	return out
}

func validISBNToken(value string) bool {
	if len(value) != 10 && len(value) != 13 {
		return false
	}
	allSame := true
	for i, r := range value {
		if i > 0 && byte(r) != value[0] {
			allSame = false
		}
		if (r >= '0' && r <= '9') || (len(value) == 10 && i == 9 && r == 'X') {
			continue
		}
		return false
	}
	return !allSame
}

func parsePrefixes(raw string) map[string]bool {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" || raw == "all" || raw == "*" {
		raw = "0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f"
	}
	var candidates []string
	if strings.ContainsAny(raw, ", \t\r\n") {
		candidates = strings.FieldsFunc(raw, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t' || r == '\r' || r == '\n'
		})
	} else {
		candidates = make([]string, 0, len(raw))
		for _, r := range raw {
			candidates = append(candidates, string(r))
		}
	}
	out := map[string]bool{}
	for _, candidate := range candidates {
		candidate = strings.ToLower(strings.TrimSpace(candidate))
		if isHexPrefix(candidate) {
			out[candidate] = true
		}
	}
	if len(out) == 0 {
		out["0"] = true
	}
	return out
}

func shardPrefixForHash(hash string, prefixes map[string]bool) (string, bool) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	best := ""
	for prefix := range prefixes {
		if prefix == "" || !strings.HasPrefix(hash, prefix) {
			continue
		}
		if len(prefix) > len(best) {
			best = prefix
		}
	}
	return best, best != ""
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

func safeIdentifierPath(value string) string {
	parts := strings.Split(strings.TrimSpace(value), ".")
	if len(parts) == 0 || len(parts) > 2 {
		return ""
	}
	for _, part := range parts {
		if part == "" {
			return ""
		}
		for _, r := range part {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
				continue
			}
			return ""
		}
	}
	return strings.Join(parts, ".")
}

func normalizePath(path string) string {
	path = strings.TrimSpace(strings.TrimLeft(path, "/"))
	for strings.Contains(path, "//") {
		path = strings.ReplaceAll(path, "//", "/")
	}
	return path
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func payloadHMACTag(data, key []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func toUint64(v any) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case int64:
		if x > 0 {
			return uint64(x)
		}
	case float64:
		if x > 0 {
			return uint64(x)
		}
	case json.Number:
		if n, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return n
		}
	case string:
		if n, err := strconv.ParseUint(strings.TrimSpace(x), 10, 64); err == nil {
			return n
		}
	}
	return 0
}
