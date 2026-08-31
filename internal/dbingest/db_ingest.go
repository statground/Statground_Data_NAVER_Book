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
	ExpectedEndpointHostname     string
	RawTable                     string
	RawLocalTable                string
	CollectLogTable              string
	CollectLogLocalTable         string
	PublisherCacheTable          string
	PublisherCacheLocal          string
	OutboxTable                  string
	OutboxReplayLimit            int
	OutboxMaxReplicaQueue        int
	OutboxMaxReplicaDelaySeconds int
	DirectTopic                  string
	ProducerSource               string
	ProducerHost                 string
	ProducerIP                   string
}

const replicaHealthSelectPrivilege = "SELECT(database, table, is_readonly, is_session_expired, parts_to_check, queue_size, absolute_delay)"

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
	rawDatabase, rawTableName := ch.SplitQualifiedTable(rawTable, client.Database)
	rawLocalTable := rawDatabase + "." + rawTableName + "_local"
	collectLogTable := envx.String("NAVER_COLLECT_LOG_TABLE", "Data_Book_NAVER_Log.naver_collect_log")
	collectDatabase, collectTableName := ch.SplitQualifiedTable(collectLogTable, client.Database)
	publisherCacheTable := envx.String("ALADIN_CACHE_TABLE", "Data_Book_NAVER_Log.aladin_publisher_cache")
	publisherDatabase, publisherTableName := ch.SplitQualifiedTable(publisherCacheTable, client.Database)
	cfg := Config{
		ExpectedEndpointHostname:     os.Getenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME"),
		RawTable:                     rawTable,
		RawLocalTable:                envx.String("RAW_NAVER_LOCAL_TABLE", rawLocalTable),
		CollectLogTable:              collectLogTable,
		CollectLogLocalTable:         envx.String("NAVER_COLLECT_LOG_LOCAL_TABLE", collectDatabase+"."+collectTableName+"_local"),
		PublisherCacheTable:          publisherCacheTable,
		PublisherCacheLocal:          envx.String("ALADIN_CACHE_LOCAL_TABLE", publisherDatabase+"."+publisherTableName+"_local"),
		OutboxTable:                  envx.String("NAVER_DIRECT_OUTBOX_TABLE", "Data_Book_NAVER_Log.naver_direct_insert_outbox"),
		OutboxReplayLimit:            boundedIntEnv("NAVER_OUTBOX_REPLAY_LIMIT", 25, 1, 100),
		OutboxMaxReplicaQueue:        boundedIntEnv("NAVER_OUTBOX_MAX_REPLICA_QUEUE", 1000, 0, 100000),
		OutboxMaxReplicaDelaySeconds: boundedIntEnv("NAVER_OUTBOX_MAX_REPLICA_DELAY_SECONDS", 900, 0, 86400),
		DirectTopic:                  envx.String("DIRECT_INGEST_TOPIC", "direct.statground_book.naver_book"),
		ProducerSource:               envx.String("PRODUCER_SOURCE", "github_actions"),
		ProducerHost:                 envx.String("PRODUCER_HOST", producerHost()),
		ProducerIP:                   envx.String("PRODUCER_IP", "::"),
	}
	for name, table := range map[string]string{
		"raw":                   cfg.RawTable,
		"raw local":             cfg.RawLocalTable,
		"collect log":           cfg.CollectLogTable,
		"collect log local":     cfg.CollectLogLocalTable,
		"publisher cache":       cfg.PublisherCacheTable,
		"publisher cache local": cfg.PublisherCacheLocal,
		"direct outbox":         cfg.OutboxTable,
	} {
		if _, err := ch.QualifiedTableIdentifier(table, client.Database); err != nil {
			return nil, fmt.Errorf("invalid NAVER %s table identifier", name)
		}
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
	if err := w.validateEndpointIdentity(ctx); err != nil {
		return err
	}
	writableTables := []string{
		w.Cfg.RawTable,
		w.Cfg.RawLocalTable,
		w.Cfg.CollectLogTable,
		w.Cfg.CollectLogLocalTable,
		w.Cfg.PublisherCacheTable,
		w.Cfg.PublisherCacheLocal,
	}
	for _, table := range append(writableTables, w.Cfg.OutboxTable) {
		var lastErr error
		for attempt := 1; attempt <= 3; attempt++ {
			lastErr = w.validateTableExists(ctx, table)
			if lastErr == nil {
				break
			}
			if !retryablePreflightError(lastErr) || attempt == 3 {
				return lastErr
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 2 * time.Second):
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	for _, table := range writableTables {
		if err := w.validateGrant(ctx, "INSERT", table); err != nil {
			return err
		}
	}
	for _, table := range []string{w.Cfg.RawLocalTable, w.Cfg.CollectLogLocalTable, w.Cfg.PublisherCacheLocal} {
		if err := w.validateGrant(ctx, "SELECT", table); err != nil {
			return err
		}
	}
	for _, privilege := range []string{"SELECT", "INSERT", "ALTER UPDATE"} {
		if err := w.validateGrant(ctx, privilege, w.Cfg.OutboxTable); err != nil {
			return err
		}
	}
	if err := w.validateGrant(ctx, replicaHealthSelectPrivilege, "system.replicas"); err != nil {
		return err
	}
	if err := w.validateRemoteGrant(ctx); err != nil {
		return err
	}
	if err := w.replayOutbox(ctx); err != nil {
		return err
	}
	return nil
}

func (w *Writer) validateEndpointIdentity(ctx context.Context) error {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		lastErr = w.Client.ValidateDirectEndpointHostnameContext(ctx, w.Cfg.ExpectedEndpointHostname)
		if lastErr == nil {
			return nil
		}
		if !retryablePreflightError(lastErr) || attempt == 3 {
			return writerOperationError("preflight_endpoint_identity", lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 2 * time.Second):
		}
	}
	return writerOperationError("preflight_endpoint_identity", lastErr)
}

func retryablePreflightError(err error) bool {
	return retryableWriterError(err)
}

func (w *Writer) validateTableExists(ctx context.Context, table string) error {
	database, tableName := ch.SplitQualifiedTable(table, w.Client.Database)
	exists, err := w.Client.TableExistsContext(ctx, table)
	if err != nil {
		return fmt.Errorf("direct DB ingest preflight failed for %s.%s: %w", database, tableName, err)
	}
	if !exists {
		return fmt.Errorf("direct DB ingest preflight failed: table %s.%s does not exist", database, tableName)
	}
	return nil
}

func (w *Writer) validateGrant(ctx context.Context, privilege, table string) error {
	qualified, err := ch.QualifiedTableIdentifier(table, w.Client.Database)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		lastErr = w.Client.ExecContext(ctx, fmt.Sprintf("CHECK GRANT %s ON %s", privilege, qualified))
		if lastErr == nil {
			return nil
		}
		if !retryablePreflightError(lastErr) || attempt == 3 {
			return writerOperationError("preflight_grant", lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 2 * time.Second):
		}
	}
	return writerOperationError("preflight_grant", lastErr)
}

func (w *Writer) validateRemoteGrant(ctx context.Context) error {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		lastErr = w.Client.ExecContext(ctx, "CHECK GRANT REMOTE ON *.*")
		if lastErr == nil {
			return nil
		}
		if !retryablePreflightError(lastErr) || attempt == 3 {
			return writerOperationError("preflight_grant", lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 2 * time.Second):
		}
	}
	return writerOperationError("preflight_grant", lastErr)
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
	ctx, cancel := w.writeContext()
	defer cancel()
	return w.InsertRawRowsContext(ctx, rows)
}

func (w *Writer) InsertRawRowsContext(ctx context.Context, rows []map[string]any) error {
	return w.insertRowsWithOutbox(ctx, "insert_raw", w.Cfg.RawTable, w.Cfg.RawLocalTable, rows)
}

func (w *Writer) InsertCollectLogRows(rows []map[string]any) error {
	ctx, cancel := w.writeContext()
	defer cancel()
	return w.InsertCollectLogRowsContext(ctx, rows)
}

func (w *Writer) InsertCollectLogRowsContext(ctx context.Context, rows []map[string]any) error {
	return w.insertRowsWithOutbox(ctx, "insert_collect_log", w.Cfg.CollectLogTable, w.Cfg.CollectLogLocalTable, rows)
}

func (w *Writer) InsertPublisherCacheRows(rows []map[string]any) error {
	ctx, cancel := w.writeContext()
	defer cancel()
	return w.InsertPublisherCacheRowsContext(ctx, rows)
}

func (w *Writer) InsertPublisherCacheRowsContext(ctx context.Context, rows []map[string]any) error {
	return w.insertRowsWithOutbox(ctx, "insert_publisher_cache", w.Cfg.PublisherCacheTable, w.Cfg.PublisherCacheLocal, rows)
}

func (w *Writer) writeContext() (context.Context, context.CancelFunc) {
	timeout := 60 * time.Second
	if w != nil && w.Client != nil && w.Client.HTTPClient != nil && w.Client.HTTPClient.Timeout > 0 {
		timeout = w.Client.HTTPClient.Timeout
	}
	return context.WithTimeout(context.Background(), timeout)
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

func boundedIntEnv(name string, fallback, minimum, maximum int) int {
	value := envx.Int(name, fallback)
	if value < minimum || value > maximum {
		return fallback
	}
	return value
}
