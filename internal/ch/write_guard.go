package ch

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"statground_naver_book_go/internal/writerlease"
)

func (c *Client) writerAdmission() writerlease.Admission {
	if c.WriterAdmission != nil {
		return c.WriterAdmission
	}
	return writerlease.Default()
}

func (c *Client) RequireWriterLease(ctx context.Context) error {
	if c == nil {
		return errors.New("Book writer lease client missing")
	}
	return c.writerAdmission().Assert(ctx)
}

func (c *Client) WriterContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := c.RequireWriterLease(ctx); err != nil {
		return nil, nil, err
	}
	if guard, ok := c.writerAdmission().(*writerlease.Guard); ok {
		child, cancel := guard.Context(ctx)
		return child, cancel, nil
	}
	child, cancel := context.WithCancel(ctx)
	return child, cancel, nil
}

func CloseWriterLease() error { return writerlease.Default().Close() }

// RunWriterCommand drains admitted requests and reports release failures even
// when the command itself completed successfully.
func RunWriterCommand(run func() error) error {
	err := run()
	return errors.Join(err, CloseWriterLease())
}

// sqlTokens inspects SQL only. JSONEachRow payloads are never interpreted as
// SQL; string literals and comments cannot turn a write into a read.
func sqlTokens(body string) ([]string, error) {
	var tokens []string
	for i := 0; i < len(body); {
		b := body[i]
		if unicode.IsSpace(rune(b)) {
			i++
			continue
		}
		if strings.HasPrefix(body[i:], "--") {
			if end := strings.IndexByte(body[i:], '\n'); end >= 0 {
				i += end + 1
				continue
			}
			break
		}
		if strings.HasPrefix(body[i:], "/*") {
			end := strings.Index(body[i+2:], "*/")
			if end < 0 {
				return nil, errors.New("Book SQL comment invalid")
			}
			i += end + 4
			continue
		}
		if b == '\'' || b == '`' || b == '"' {
			quote, start := b, i+1
			i++
			for i < len(body) {
				if body[i] == '\\' {
					i += 2
					continue
				}
				if body[i] == quote {
					if i+1 < len(body) && body[i+1] == quote {
						i += 2
						continue
					}
					break
				}
				i++
			}
			if i >= len(body) {
				return nil, errors.New("Book SQL quote invalid")
			}
			if quote == '\'' {
				tokens = append(tokens, "<literal>")
			} else {
				tokens = append(tokens, "\x00"+body[start:i])
			}
			i++
			continue
		}
		start := i
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' {
			i++
			for i < len(body) && (body[i] >= 'a' && body[i] <= 'z' || body[i] >= 'A' && body[i] <= 'Z' || body[i] >= '0' && body[i] <= '9' || body[i] == '_') {
				i++
			}
		} else {
			i++
		}
		tokens = append(tokens, body[start:i])
		if len(tokens) >= 2 && strings.EqualFold(tokens[len(tokens)-2], "FORMAT") && strings.EqualFold(tokens[len(tokens)-1], "JSONEachRow") && statementCommand(tokens) == "INSERT" {
			break
		}
	}
	for i, token := range tokens {
		if token == ";" && i != len(tokens)-1 {
			return nil, errors.New("Book SQL multiple statements rejected")
		}
	}
	return tokens, nil
}

func statementCommand(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	if !strings.EqualFold(tokens[0], "WITH") {
		return strings.ToUpper(tokens[0])
	}
	depth := 0
	for _, token := range tokens[1:] {
		if token == "(" {
			depth++
		} else if token == ")" {
			depth--
		}
		if depth == 0 && (strings.EqualFold(token, "SELECT") || strings.EqualFold(token, "INSERT")) {
			return strings.ToUpper(token)
		}
	}
	return ""
}

func mutationTarget(body, database string) (string, error) {
	tokens, err := sqlTokens(body)
	if err != nil || len(tokens) == 0 {
		return "", errors.New("Book SQL classification failed")
	}
	position := 0
	if strings.EqualFold(tokens[0], "WITH") {
		depth := 0
		position = -1
		for i := 1; i < len(tokens); i++ {
			if tokens[i] == "(" {
				depth++
			} else if tokens[i] == ")" {
				depth--
			}
			if depth == 0 && (strings.EqualFold(tokens[i], "SELECT") || strings.EqualFold(tokens[i], "INSERT")) {
				position = i
				break
			}
		}
		if position < 0 {
			return "", errors.New("Book SQL classification failed")
		}
	}
	command := strings.ToUpper(tokens[position])
	switch command {
	case "SELECT", "SHOW", "DESCRIBE", "DESC", "EXISTS", "EXPLAIN":
		return "", nil
	case "CHECK":
		if position+1 < len(tokens) && strings.EqualFold(tokens[position+1], "GRANT") {
			return "", nil
		}
	case "SYSTEM":
		// These four native outputs are disjoint from this resource. Their
		// scheduler and the existing manual-refresh lease remain independent.
		if len(tokens) == 6 && strings.EqualFold(tokens[2], "VIEW") && tokens[4] == "." {
			view := strings.TrimPrefix(tokens[3], "\x00") + "." + strings.TrimPrefix(tokens[5], "\x00")
			if strings.EqualFold(tokens[1], "WAIT") || (strings.EqualFold(tokens[1], "REFRESH") || strings.EqualFold(tokens[1], "START")) && nativeViewDisjoint(view) {
				return "", nil
			}
		}
	case "INSERT", "ALTER":
		keyword := "INTO"
		if command == "ALTER" {
			keyword = "TABLE"
		}
		if position+2 >= len(tokens) || !strings.EqualFold(tokens[position+1], keyword) {
			break
		}
		index := position + 2
		name := strings.TrimPrefix(tokens[index], "\x00")
		if index+2 < len(tokens) && tokens[index+1] == "." {
			name += "." + strings.TrimPrefix(tokens[index+2], "\x00")
			index += 2
		}
		qualified, err := QualifiedTableIdentifier(name, database)
		if err != nil {
			break
		}
		if command == "ALTER" && (index+1 >= len(tokens) || !strings.EqualFold(tokens[index+1], "UPDATE")) {
			break
		}
		for i := index + 1; i+2 < len(tokens); i++ {
			key := strings.ToLower(tokens[i])
			if tokens[i+1] != "=" {
				continue
			}
			expected, forced := map[string]string{"async_insert": "0", "insert_distributed_sync": "1", "wait_end_of_query": "1", "mutations_sync": "2", "materialized_views_ignore_errors": "0"}[strings.TrimPrefix(key, "\x00")]
			if forced {
				if tokens[i+2] != expected || i+3 < len(tokens) && tokens[i+3] != "," && tokens[i+3] != ";" && !strings.EqualFold(tokens[i+3], "FORMAT") {
					return "", errors.New("Book write synchronous settings conflict")
				}
			}
		}
		return strings.ReplaceAll(qualified, "`", ""), nil
	}
	return "", errors.New("Book SQL mutation unsupported")
}

func mutationSettings(extra url.Values) url.Values {
	result := make(url.Values)
	for key, values := range extra {
		result[key] = append([]string(nil), values...)
	}
	result.Set("async_insert", "0")
	result.Set("insert_distributed_sync", "1")
	result.Set("mutations_sync", "2")
	result.Set("wait_end_of_query", "1")
	result.Set("materialized_views_ignore_errors", "0")
	result.Set("buffer_size", "1048576")
	return result
}

func nativeViewDisjoint(view string) bool {
	switch view {
	case "Data_Book_Service.mv_book_catalog_latest_refresh", "webr_book.mv_naver_r_book_catalog_refresh", "mirtype_book.mv_naver_language_book_catalog_refresh", "Data_Book_Service.mv_book_bibliography_discovery_refresh":
		return true
	}
	return false
}

func newWriteIntent(target, body string) (writerlease.Intent, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return writerlease.Intent{}, errors.New("Book write entropy unavailable")
	}
	id[6], id[8] = id[6]&0x0f|0x40, id[8]&0x3f|0x80
	value := hex.EncodeToString(id[:])
	operation := fmt.Sprintf("%s-%s-%s-%s-%s", value[:8], value[8:12], value[12:16], value[16:20], value[20:])
	sum := sha256.Sum256([]byte(body))
	return writerlease.Intent{Operation: operation, Target: target, QueryID: operation, RequestSHA256: hex.EncodeToString(sum[:])}, nil
}
