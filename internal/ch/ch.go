package ch

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/util"
)

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

func (c *Client) post(body string, extra url.Values) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, c.endpoint(extra), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	if c.User != "" {
		req.SetBasicAuth(c.User, c.Password)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse http %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
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
	_, err := c.post(strings.TrimSpace(sql), nil)
	return err
}

func (c *Client) QueryJSONEachRow(sql string) ([]map[string]any, error) {
	payload, err := c.post(ensureJSONEachRow(sql), nil)
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
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("decode json row: %w; line=%s", err, string(line))
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) QueryJSONEachRowStream(sql string, handle func(map[string]any) error) error {
	req, err := http.NewRequest(http.MethodPost, c.endpoint(nil), strings.NewReader(ensureJSONEachRow(sql)))
	if err != nil {
		return err
	}
	if c.User != "" {
		req.SetBasicAuth(c.User, c.Password)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse http %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			return fmt.Errorf("decode json row: %w; line=%s", err, string(line))
		}
		if err := handle(row); err != nil {
			return err
		}
	}
	return scanner.Err()
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
	rows, err := c.QueryJSONEachRow(sql)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return map[string]any{}, nil
	}
	return rows[0], nil
}

func (c *Client) QueryScalarValue(sql string) (any, error) {
	row, err := c.QuerySingleRow(sql)
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

func (c *Client) QueryColumnNames(table string) (map[string]bool, error) {
	sql := fmt.Sprintf(`
        SELECT name
        FROM system.columns
        WHERE database = %s
          AND table = %s
    `, util.SQLString(c.Database), util.SQLString(table))
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

func normalizeValue(v any) any {
	switch x := v.(type) {
	case time.Time:
		return util.FormatCHDateTime64Millis(x)
	default:
		return x
	}
}

func (c *Client) InsertJSONEachRow(table string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	colSet := map[string]struct{}{}
	for _, row := range rows {
		for k := range row {
			colSet[k] = struct{}{}
		}
	}
	columns := make([]string, 0, len(colSet))
	for col := range colSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "INSERT INTO %s (%s) FORMAT JSONEachRow\n", table, strings.Join(columns, ", "))
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, row := range rows {
		normalized := make(map[string]any, len(columns))
		for _, col := range columns {
			normalized[col] = normalizeValue(row[col])
		}
		if err := enc.Encode(normalized); err != nil {
			return err
		}
	}
	_, err := c.post(buf.String(), nil)
	return err
}
