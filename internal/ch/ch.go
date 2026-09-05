package ch

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/util"
)

var (
	identifierPattern          = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	clickHouseErrorCodePattern = regexp.MustCompile(`(?i)\bcode:\s*([0-9]+)\b`)
	directEndpointHostPattern  = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,251}[A-Za-z0-9])?$`)
)

const maxClickHouseErrorBodyBytes = 64 * 1024

// HTTPError preserves only the bounded status/code contract needed by callers.
// ClickHouse exception text is deliberately not retained because it can contain
// schema details or values from a rejected row.
type HTTPError struct {
	StatusCode int
	Code       int
	Truncated  bool
}

func (e *HTTPError) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("clickhouse http status=%d code=%d", e.StatusCode, e.Code)
	}
	return fmt.Sprintf("clickhouse http status=%d", e.StatusCode)
}

// deliveryUnknownError means http.Client.Do started, so a write may have
// reached ClickHouse even though no authoritative response was received.
// INSERT callers must reconcile or preserve the batch; they must not retry it
// immediately.
type deliveryUnknownError struct {
	err error
}

func (e *deliveryUnknownError) Error() string { return e.err.Error() }
func (e *deliveryUnknownError) Unwrap() error { return e.err }

// IsAmbiguousInsertError reports failures for which ClickHouse may already have
// accepted some or all of an INSERT. UNKNOWN_STATUS_OF_INSERT is the explicit
// server-side case; transport/read failures after Do begins are conservative
// client-side cases.
func IsAmbiguousInsertError(err error) bool {
	if err == nil {
		return false
	}
	var deliveryErr *deliveryUnknownError
	if errors.As(err, &deliveryErr) {
		return true
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	switch httpErr.Code {
	case 319, 394, 677, 734, 735:
		return true
	default:
		return false
	}
}

type Client struct {
	Host       string
	Port       int
	Protocol   string
	HTTPPath   string
	User       string
	Password   string
	Database   string
	HTTPClient *http.Client
}

func New(host string, port int, user, password, database string) *Client {
	protocol := envx.String("CH_PROTOCOL", envx.String("CLICKHOUSE_PROTOCOL", "http"))
	httpPath := envx.String("CH_HTTP_URL_PATH", envx.String("CLICKHOUSE_HTTP_URL_PATH", ""))
	return &Client{
		Host:       host,
		Port:       port,
		Protocol:   protocol,
		HTTPPath:   httpPath,
		User:       user,
		Password:   password,
		Database:   database,
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

func NewOptionalFromEnv() (*Client, error) {
	host := envx.String("CH_HOST", envx.String("CLICKHOUSE_HOST", ""))
	if strings.TrimSpace(host) == "" {
		return nil, nil
	}
	port := envx.Int("CH_PORT", envx.Int("CLICKHOUSE_PORT", 8123))
	user := envx.String("CH_USER", envx.String("CLICKHOUSE_USER", "default"))
	password := envx.String("CH_PASSWORD", envx.String("CLICKHOUSE_PASSWORD", ""))
	database := envx.String("CH_DATABASE", envx.String("CLICKHOUSE_DATABASE", "Data_Book_NAVER_Raw"))
	return New(host, port, user, password, database), nil
}

func NewFromEnv() (*Client, error) {
	host, err := requireStringAny("CH_HOST", "CLICKHOUSE_HOST")
	if err != nil {
		return nil, err
	}
	port, err := requireIntAny("CH_PORT", "CLICKHOUSE_PORT")
	if err != nil {
		return nil, err
	}
	user, err := requireStringAny("CH_USER", "CLICKHOUSE_USER")
	if err != nil {
		return nil, err
	}
	password, err := requireStringAny("CH_PASSWORD", "CLICKHOUSE_PASSWORD")
	if err != nil {
		return nil, err
	}
	database, err := requireStringAny("CH_DATABASE", "CLICKHOUSE_DATABASE")
	if err != nil {
		return nil, err
	}
	return New(host, port, user, password, database), nil
}

func requireStringAny(names ...string) (string, error) {
	for _, name := range names {
		value := envx.String(name, "")
		if strings.TrimSpace(value) != "" {
			return value, nil
		}
	}
	return "", fmt.Errorf("missing required environment variable: %s", strings.Join(names, " or "))
}

func requireIntAny(names ...string) (int, error) {
	for _, name := range names {
		value := envx.Int(name, 0)
		if value > 0 {
			return value, nil
		}
	}
	return 0, fmt.Errorf("missing required integer environment variable: %s", strings.Join(names, " or "))
}

func (c *Client) endpoint(extra url.Values) string {
	q := url.Values{}
	if c.Database != "" {
		q.Set("database", c.Database)
	}
	q.Set("date_time_input_format", "best_effort")
	q.Set("input_format_skip_unknown_fields", "1")
	q.Set("output_format_json_quote_64bit_integers", "0")
	for key, values := range extra {
		for _, v := range values {
			q.Add(key, v)
		}
	}
	return c.baseURL() + "?" + q.Encode()
}

func (c *Client) baseURL() string {
	host := strings.TrimSpace(c.Host)
	protocol := strings.TrimSpace(c.Protocol)
	if protocol == "" {
		protocol = "http"
	}
	path := normalizeHTTPPath(c.HTTPPath)
	if strings.Contains(host, "://") {
		parsed, err := url.Parse(host)
		if err == nil && parsed.Scheme != "" && parsed.Host != "" {
			if c.HTTPPath == "" {
				path = normalizeHTTPPath(parsed.Path)
			}
			return (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host, Path: path}).String()
		}
	}
	host = normalizeHostPort(host, c.Port)
	return (&url.URL{Scheme: protocol, Host: host, Path: path}).String()
}

func normalizeHTTPPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimRight(path, "/") + "/"
}

func normalizeHostPort(host string, port int) string {
	host = strings.TrimSpace(strings.TrimRight(host, "/"))
	if host == "" {
		return ""
	}
	if port <= 0 {
		port = 8123
	}
	if strings.HasPrefix(host, "[") {
		if strings.Contains(host, "]:") {
			return host
		}
		return fmt.Sprintf("%s:%d", host, port)
	}
	if strings.Count(host, ":") == 1 {
		parts := strings.Split(host, ":")
		if len(parts) == 2 && allDigits(parts[1]) {
			return host
		}
	}
	if strings.Contains(host, ":") {
		return fmt.Sprintf("[%s]:%d", host, port)
	}
	return fmt.Sprintf("%s:%d", host, port)
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ValidateDirectEndpointHostnameContext proves that the configured HTTP
// endpoint terminates on the one physical ClickHouse node that owns the local
// outbox. A load-balancing gateway is not a valid endpoint for this contract.
func (c *Client) ValidateDirectEndpointHostnameContext(ctx context.Context, expected string) error {
	if expected == "" || expected != strings.TrimSpace(expected) {
		return fmt.Errorf("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME is required")
	}
	if !directEndpointHostPattern.MatchString(expected) || strings.Contains(strings.ToLower(expected), "gateway") {
		return fmt.Errorf("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME must identify one physical ClickHouse node")
	}
	if c == nil {
		return fmt.Errorf("clickhouse direct endpoint client is not configured")
	}
	rows, err := c.QueryJSONEachRowContext(ctx, "SELECT hostName() AS value SETTINGS max_threads = 1, max_execution_time = 5")
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("clickhouse direct endpoint hostname mismatch")
	}
	actual, ok := rows[0]["value"].(string)
	if !ok || actual != expected {
		return fmt.Errorf("clickhouse direct endpoint hostname mismatch")
	}
	return nil
}

func (c *Client) post(body string, extra url.Values) ([]byte, error) {
	return c.postContext(context.Background(), body, extra)
}

func (c *Client) postContext(ctx context.Context, body string, extra url.Values) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(extra), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	if c.User != "" {
		req.SetBasicAuth(c.User, c.Password)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	requestClient := *httpClient
	requestClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := requestClient.Do(req)
	if err != nil {
		return nil, &deliveryUnknownError{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, maxClickHouseErrorBodyBytes+1))
		if readErr != nil {
			return nil, &deliveryUnknownError{err: readErr}
		}
		truncated := len(payload) > maxClickHouseErrorBodyBytes
		if truncated {
			payload = payload[:maxClickHouseErrorBodyBytes]
		}
		if matches := clickHouseErrorCodePattern.FindSubmatch(payload); len(matches) == 2 {
			code, _ := strconv.Atoi(string(matches[1]))
			return nil, &HTTPError{StatusCode: resp.StatusCode, Code: code, Truncated: truncated}
		}
		return nil, &HTTPError{StatusCode: resp.StatusCode, Truncated: truncated}
	}
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &deliveryUnknownError{err: err}
	}
	return payload, nil
}

func ensureJSONEachRow(sql string) string {
	up := strings.ToUpper(sql)
	if strings.Contains(up, "FORMAT JSONEACHROW") {
		return sql
	}
	return strings.TrimSpace(sql) + "\nFORMAT JSONEachRow"
}

func (c *Client) Exec(sql string) error {
	return c.ExecContext(context.Background(), sql)
}

func (c *Client) ExecContext(ctx context.Context, sql string) error {
	_, err := c.postContext(ctx, strings.TrimSpace(sql), nil)
	return err
}

// ExecSingleAttempt is the explicit no-retry path for heavy INSERT SELECT
// operations. Callers must treat any transport or server failure as ambiguous
// and must not advance a durable checkpoint automatically.
func (c *Client) ExecSingleAttempt(sql string) error {
	_, err := c.post(strings.TrimSpace(sql), nil)
	return err
}

func (c *Client) QueryJSONEachRow(sql string) ([]map[string]any, error) {
	return c.QueryJSONEachRowContext(context.Background(), sql)
}

func (c *Client) QueryJSONEachRowContext(ctx context.Context, sql string) ([]map[string]any, error) {
	return c.queryJSONEachRowContext(ctx, sql, false)
}

func (c *Client) queryJSONEachRowNumberContext(ctx context.Context, sql string) ([]map[string]any, error) {
	return c.queryJSONEachRowContext(ctx, sql, true)
}

func (c *Client) queryJSONEachRowContext(ctx context.Context, sql string, useNumber bool) ([]map[string]any, error) {
	payload, err := c.postContext(ctx, ensureJSONEachRow(sql), nil)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0)
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	scanner.Buffer(make([]byte, 1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		decoder := json.NewDecoder(bytes.NewReader(line))
		if useNumber {
			decoder.UseNumber()
		}
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("decode json row: %w; line_prefix=%s", err, truncateForError(line, 512))
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func truncateForError(value []byte, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return string(value)
	}
	return string(value[:limit]) + "...(truncated)"
}

func (c *Client) QueryScalarInt(sql string) (int64, error) {
	rows, err := c.QueryJSONEachRow("SELECT value FROM (" + strings.TrimSpace(sql) + ")")
	if err == nil && len(rows) > 0 {
		if v, ok := rows[0]["value"]; ok {
			return util.ToInt64(v), nil
		}
	}
	rows, err = c.QueryJSONEachRow(strings.TrimSpace(sql) + " AS value")
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return util.ToInt64(rows[0]["value"]), nil
}

func (c *Client) QuerySingleRow(sql string) (map[string]any, error) {
	return c.QuerySingleRowContext(context.Background(), sql)
}

func (c *Client) QuerySingleRowContext(ctx context.Context, sql string) (map[string]any, error) {
	rows, err := c.QueryJSONEachRowContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return map[string]any{}, nil
	}
	return rows[0], nil
}

func (c *Client) QueryScalarValue(sql string) (any, error) {
	return c.QueryScalarValueContext(context.Background(), sql)
}

func (c *Client) QueryScalarValueContext(ctx context.Context, sql string) (any, error) {
	row, err := c.QuerySingleRowContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	if v, ok := row["value"]; ok {
		return v, nil
	}
	if len(row) == 1 {
		for _, v := range row {
			return v, nil
		}
	}
	return nil, nil
}

// QualifiedTableIdentifier validates and quotes a table identifier before it is
// embedded in ClickHouse SQL. Only the unquoted database.table form (or a table
// name plus a safe default database) is accepted.
func QualifiedTableIdentifier(raw, defaultDatabase string) (string, error) {
	raw = strings.TrimSpace(raw)
	defaultDatabase = strings.TrimSpace(defaultDatabase)

	parts := strings.Split(raw, ".")
	var database, table string
	switch len(parts) {
	case 1:
		database, table = defaultDatabase, strings.TrimSpace(parts[0])
	case 2:
		database, table = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	default:
		return "", fmt.Errorf("invalid ClickHouse table identifier")
	}
	if !identifierPattern.MatchString(database) || !identifierPattern.MatchString(table) {
		return "", fmt.Errorf("invalid ClickHouse table identifier")
	}
	return fmt.Sprintf("`%s`.`%s`", database, table), nil
}

// TableExists asks ClickHouse to resolve one exact table name. It deliberately
// avoids querying system.tables because a metadata-wide scan can block startup
// while unrelated table metadata is recovering.
func (c *Client) TableExists(table string) (bool, error) {
	return c.TableExistsContext(context.Background(), table)
}

func (c *Client) TableExistsContext(ctx context.Context, table string) (bool, error) {
	qualified, err := QualifiedTableIdentifier(table, c.Database)
	if err != nil {
		return false, err
	}
	value, err := c.QueryScalarValueContext(ctx, "EXISTS TABLE "+qualified)
	if err != nil {
		return false, err
	}
	return util.ToInt64(value) == 1, nil
}

func (c *Client) QueryColumnNames(table string) (map[string]bool, error) {
	database, tableName := SplitQualifiedTable(table, c.Database)
	sql := fmt.Sprintf(`
        SELECT name
        FROM system.columns
        WHERE database = %s
          AND table = %s
    `, util.SQLString(database), util.SQLString(tableName))
	rows, err := c.QueryJSONEachRow(sql)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(rows))
	for _, row := range rows {
		name := strings.TrimSpace(util.ToString(row["name"]))
		if name != "" {
			out[name] = true
		}
	}
	return out, nil
}

func SplitQualifiedTable(raw, defaultDatabase string) (database, table string) {
	raw = strings.TrimSpace(strings.ReplaceAll(raw, "`", ""))
	if raw == "" {
		return strings.TrimSpace(defaultDatabase), ""
	}
	parts := strings.Split(raw, ".")
	if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return strings.TrimSpace(defaultDatabase), raw
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case time.Time:
		return util.FormatCHDateTime64Millis(x)
	default:
		return x
	}
}

func jsonEachRowColumns(rows []map[string]any) []string {
	colSet := map[string]struct{}{}
	for _, row := range rows {
		for column := range row {
			colSet[column] = struct{}{}
		}
	}
	columns := make([]string, 0, len(colSet))
	for column := range colSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

// encodeJSONEachRow preserves the pre-outbox client behavior for all existing
// callers. Outbox producers pass already-canonical rows into this encoder.
func encodeJSONEachRow(rows []map[string]any) ([]string, []byte, error) {
	columns := jsonEachRowColumns(rows)
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	for _, row := range rows {
		normalized := make(map[string]any, len(columns))
		for _, column := range columns {
			normalized[column] = normalizeValue(row[column])
		}
		if err := encoder.Encode(normalized); err != nil {
			return nil, nil, err
		}
	}
	return columns, buffer.Bytes(), nil
}

// CanonicalJSONEachRow prepares one immutable logical JSONEachRow batch. It
// fills the same sorted union of columns in every row, applies ClickHouse time
// formatting, and round-trips through JSON so an outbox replay cannot change
// Go-specific value representations (for example time.Time or json.Marshaler).
// The returned payload is exactly what insertJSONEachRow sends after preparing
// the returned rows again.
func CanonicalJSONEachRow(rows []map[string]any) (columns []string, canonicalRows []map[string]any, payload []byte, err error) {
	if len(rows) == 0 {
		return nil, nil, nil, nil
	}
	columns = jsonEachRowColumns(rows)

	canonicalRows = make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		normalized := make(map[string]any, len(columns))
		for _, column := range columns {
			normalized[column] = normalizeValue(row[column])
		}
		encoded, encodeErr := json.Marshal(normalized)
		if encodeErr != nil {
			return nil, nil, nil, encodeErr
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		canonical := make(map[string]any, len(columns))
		if decodeErr := decoder.Decode(&canonical); decodeErr != nil {
			return nil, nil, nil, decodeErr
		}
		canonicalRows = append(canonicalRows, canonical)
	}

	encodedColumns, payload, encodeErr := encodeJSONEachRow(canonicalRows)
	if encodeErr != nil {
		return nil, nil, nil, encodeErr
	}
	return encodedColumns, canonicalRows, payload, nil
}

// JSONEachRowDeduplicationToken binds a token to the exact normalized payload
// and target contract used by the ClickHouse insert.
func JSONEachRowDeduplicationToken(table string, columns []string, payload []byte) string {
	lineage := []byte(table + "\x1f" + strings.Join(columns, "\x1f") + "\x1f")
	return fmt.Sprintf("%x", sha256.Sum256(append(lineage, payload...)))
}

// JSONEachRowPayloadAsArray wraps canonical single-line JSONEachRow objects in
// one JSON array without re-marshalling their values. Each stored array element
// therefore remains byte-identical to the corresponding target payload row.
func JSONEachRowPayloadAsArray(payload []byte) ([]byte, error) {
	if len(payload) == 0 || payload[len(payload)-1] != '\n' {
		return nil, fmt.Errorf("JSONEachRow payload must end with a newline")
	}
	lines := bytes.Split(payload[:len(payload)-1], []byte{'\n'})
	var array bytes.Buffer
	array.WriteByte('[')
	for index, line := range lines {
		if len(line) == 0 || !json.Valid(line) {
			return nil, fmt.Errorf("JSONEachRow payload contains an invalid row")
		}
		if index > 0 {
			array.WriteByte(',')
		}
		array.Write(line)
	}
	array.WriteByte(']')
	if !json.Valid(array.Bytes()) {
		return nil, fmt.Errorf("JSONEachRow payload cannot be represented as an array")
	}
	return array.Bytes(), nil
}

func (c *Client) InsertJSONEachRow(table string, rows []map[string]any) error {
	return c.InsertJSONEachRowContext(context.Background(), table, rows)
}

func (c *Client) InsertJSONEachRowContext(ctx context.Context, table string, rows []map[string]any) error {
	return c.insertJSONEachRowContext(ctx, table, rows, "", false, false)
}

// InsertJSONEachRowSynchronous writes one deterministic logical batch through
// a Distributed table without leaving an asynchronous coordinator queue file.
// A caller that receives an error must preserve the same rows and token in a
// durable outbox before treating the logical batch as accepted.
func (c *Client) InsertJSONEachRowSynchronous(table string, rows []map[string]any, deduplicationToken string) error {
	return c.InsertJSONEachRowSynchronousContext(context.Background(), table, rows, deduplicationToken)
}

func (c *Client) InsertJSONEachRowSynchronousContext(ctx context.Context, table string, rows []map[string]any, deduplicationToken string) error {
	if len(rows) == 0 {
		return nil
	}
	deduplicationToken = strings.TrimSpace(deduplicationToken)
	if !isLowerHexSHA256(deduplicationToken) {
		return fmt.Errorf("invalid synchronous insert deduplication token")
	}
	return c.insertJSONEachRowContext(ctx, table, rows, deduplicationToken, false, true)
}

// InsertJSONEachRowDurable applies the fixed foreground/quorum settings used by
// resumable bulk import ledgers and raw data. The caller-supplied token must be
// a lowercase SHA-256 of immutable logical lineage, never volatile row JSON.
func (c *Client) InsertJSONEachRowDurable(table string, rows []map[string]any, deduplicationToken string) error {
	return c.InsertJSONEachRowDurableContext(context.Background(), table, rows, deduplicationToken)
}

func (c *Client) InsertJSONEachRowDurableContext(ctx context.Context, table string, rows []map[string]any, deduplicationToken string) error {
	if len(rows) == 0 {
		return nil
	}
	deduplicationToken = strings.TrimSpace(deduplicationToken)
	if !isLowerHexSHA256(deduplicationToken) {
		return fmt.Errorf("invalid durable insert deduplication token")
	}
	return c.insertJSONEachRowContext(ctx, table, rows, deduplicationToken, true, false)
}

func (c *Client) insertJSONEachRowContext(ctx context.Context, table string, rows []map[string]any, deduplicationToken string, durable, distributedSync bool) error {
	if len(rows) == 0 {
		return nil
	}
	columns, payload, err := encodeJSONEachRow(rows)
	if err != nil {
		return err
	}
	token := deduplicationToken
	if token == "" {
		token = JSONEachRowDeduplicationToken(table, columns, payload)
	}
	settings := fmt.Sprintf(
		"insert_deduplicate = 1, insert_deduplication_token = '%s'",
		token,
	)
	if distributedSync {
		settings += ", insert_distributed_sync = 1"
	}
	if durable {
		// The runtime profile defaults to a 30-second query deadline. A durable
		// quorum write has a ten-minute acknowledgement budget; align the query
		// deadline so a committed checkpoint is not reported as failed at 30s.
		settings += ", distributed_foreground_insert = 1, insert_quorum = 2, insert_quorum_parallel = 1, insert_quorum_timeout = 600000, max_execution_time = 600, timeout_overflow_mode = 'throw', max_threads = 1, parallel_view_processing = 1, receive_timeout = 660, send_timeout = 660, load_balancing = 'first_or_random', load_balancing_first_offset = 0, prefer_localhost_replica = 0"
	}
	body := fmt.Sprintf("INSERT INTO %s (%s) SETTINGS %s FORMAT JSONEachRow\n%s",
		table, strings.Join(columns, ", "), settings, payload)
	_, err = c.postContext(ctx, body, nil)
	return err
}

func isLowerHexSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
