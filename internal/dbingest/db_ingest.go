package dbingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/util"
)

type Config struct {
	RawTable            string
	CollectLogTable     string
	PublisherCacheTable string
	DirectTopic         string
	ProducerSource      string
	ProducerHost        string
	ProducerIP          string
}

type Writer struct {
	Client *ch.Client
	Cfg    Config
}

type Event struct {
	EventUUID string
	Source    string
	Host      string
	UUIDUser  string
	IP        string
	URL       string
	EventType string
	Payload   string
	CreatedAt string
}

func NewFromEnv(client *ch.Client, rawTable string) (*Writer, error) {
	if client == nil {
		return nil, fmt.Errorf("ClickHouse connection is required for direct DB ingestion; configure CH_* or CLICKHOUSE_* environment variables")
	}
	if strings.TrimSpace(rawTable) == "" {
		rawTable = "naver_book_raw"
	}
	cfg := Config{
		RawTable:            rawTable,
		CollectLogTable:     envx.String("NAVER_COLLECT_LOG_TABLE", "Data_Book_NAVER_Log.naver_collect_log"),
		PublisherCacheTable: envx.String("ALADIN_CACHE_TABLE", "Data_Book_NAVER_Log.aladin_publisher_cache"),
		DirectTopic:         envx.String("DIRECT_INGEST_TOPIC", "direct.statground_book.naver_book"),
		ProducerSource:      envx.String("PRODUCER_SOURCE", "github_actions"),
		ProducerHost:        envx.String("PRODUCER_HOST", producerHost()),
		ProducerIP:          envx.String("PRODUCER_IP", "::"),
	}
	return &Writer{Client: client, Cfg: cfg}, nil
}

func (w *Writer) Validate(ctx context.Context) error {
	if w == nil || w.Client == nil {
		return fmt.Errorf("ClickHouse writer is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, table := range []string{w.Cfg.RawTable, w.Cfg.CollectLogTable, w.Cfg.PublisherCacheTable} {
		if err := w.validateTableExists(table); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) validateTableExists(table string) error {
	database, tableName := ch.SplitQualifiedTable(table, w.Client.Database)
	if strings.TrimSpace(database) == "" || strings.TrimSpace(tableName) == "" {
		return fmt.Errorf("invalid ClickHouse table name %q", table)
	}
	sql := fmt.Sprintf(`
        SELECT count() AS value
        FROM system.tables
        WHERE database = %s
          AND name = %s
    `, util.SQLString(database), util.SQLString(tableName))
	count, err := w.Client.QueryScalarInt(sql)
	if err != nil {
		return fmt.Errorf("direct DB ingest preflight failed for %s.%s: %w", database, tableName, err)
	}
	if count == 0 {
		return fmt.Errorf("direct DB ingest preflight failed: table %s.%s does not exist", database, tableName)
	}
	return nil
}

func (w *Writer) NewEvent(eventType, eventUUID, sourceURL, createdAt string, payload map[string]any) (Event, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	if strings.TrimSpace(eventUUID) == "" {
		eventUUID = util.UUIDv7()
	}
	if strings.TrimSpace(createdAt) == "" {
		createdAt = util.FormatCHDateTime64Millis(util.NowKST())
	}
	return Event{
		EventUUID: eventUUID,
		Source:    w.Cfg.ProducerSource,
		Host:      w.Cfg.ProducerHost,
		UUIDUser:  "",
		IP:        w.Cfg.ProducerIP,
		URL:       sourceURL,
		EventType: eventType,
		Payload:   string(payloadJSON),
		CreatedAt: createdAt,
	}, nil
}

func (w *Writer) InsertRawRows(rows []map[string]any) error {
	return w.Client.InsertJSONEachRow(w.Cfg.RawTable, rows)
}

func (w *Writer) InsertCollectLogRows(rows []map[string]any) error {
	return w.Client.InsertJSONEachRow(w.Cfg.CollectLogTable, rows)
}

func (w *Writer) InsertPublisherCacheRows(rows []map[string]any) error {
	return w.Client.InsertJSONEachRow(w.Cfg.PublisherCacheTable, rows)
}

func (w *Writer) WithTimeout(timeout time.Duration) *Writer {
	if w == nil || w.Client == nil || timeout <= 0 {
		return w
	}
	clientCopy := *w.Client
	if w.Client.HTTPClient != nil {
		httpClientCopy := *w.Client.HTTPClient
		httpClientCopy.Timeout = timeout
		clientCopy.HTTPClient = &httpClientCopy
	} else {
		clientCopy.HTTPClient = &http.Client{Timeout: timeout}
	}
	return &Writer{Client: &clientCopy, Cfg: w.Cfg}
}

func producerHost() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "github-actions"
	}
	return host
}
