package kafkaingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"

	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/util"
)

type Config struct {
	IngestMode           string
	KafkaBrokers         []string
	KafkaUsername        string
	KafkaPassword        string
	KafkaTopic           string
	KafkaClientID        string
	KafkaBatchSize       int
	KafkaBatchTimeout    time.Duration
	KafkaWriteAttempts   int
	KafkaWriteBackoffMin time.Duration
	KafkaWriteBackoffMax time.Duration
	ProducerSource       string
	ProducerIP           string
}

type Publisher struct {
	Cfg Config
}

type kafkaMessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type Event struct {
	EventUUID string `json:"event_uuid"`
	Source    string `json:"source"`
	Host      string `json:"host"`
	UUIDUser  string `json:"uuid_user"`
	IP        string `json:"ip"`
	URL       string `json:"url"`
	EventType string `json:"event_type"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}

func NewFromEnv() (*Publisher, error) {
	cfg := Config{
		IngestMode:           strings.ToLower(envx.String("INGEST_MODE", "kafka")),
		KafkaBrokers:         splitCSV(envx.String("KAFKA_BROKERS", "")),
		KafkaUsername:        envx.String("KAFKA_USERNAME", envx.String("KAFKA_EXTERNAL_USER", "")),
		KafkaPassword:        envx.String("KAFKA_PASSWORD", envx.String("KAFKA_EXTERNAL_PASSWORD", "")),
		KafkaTopic:           envx.String("KAFKA_TOPIC", "book.events"),
		KafkaClientID:        envx.String("KAFKA_CLIENT_ID", "statground-naver-book-crawler"),
		KafkaBatchSize:       positiveInt(envx.String("KAFKA_BATCH_SIZE", "100"), 100),
		KafkaBatchTimeout:    secondsDefault(envx.String("KAFKA_BATCH_TIMEOUT", "1.0"), time.Second),
		KafkaWriteAttempts:   positiveInt(envx.String("KAFKA_WRITE_ATTEMPTS", "5"), 5),
		KafkaWriteBackoffMin: secondsDefault(envx.String("KAFKA_WRITE_BACKOFF_MIN", envx.String("KAFKA_WRITE_BACKOFF_MIN_SECONDS", "1.0")), time.Second),
		KafkaWriteBackoffMax: secondsDefault(envx.String("KAFKA_WRITE_BACKOFF_MAX", envx.String("KAFKA_WRITE_BACKOFF_MAX_SECONDS", "12.0")), 12*time.Second),
		ProducerSource:       envx.String("PRODUCER_SOURCE", "github_actions"),
		ProducerIP:           envx.String("PRODUCER_IP", "::"),
	}
	if cfg.KafkaWriteBackoffMax < cfg.KafkaWriteBackoffMin {
		cfg.KafkaWriteBackoffMax = cfg.KafkaWriteBackoffMin
	}
	switch strings.ToLower(strings.TrimSpace(cfg.IngestMode)) {
	case "kafka", "kafka_clickhouse", "kafka-clickhouse", "event", "events":
		cfg.IngestMode = "kafka"
	default:
		return nil, fmt.Errorf("unsupported INGEST_MODE=%q; Statground NAVER Book crawler supports Kafka ingestion only", cfg.IngestMode)
	}
	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("missing required env: KAFKA_BROKERS")
	}
	if strings.TrimSpace(cfg.KafkaTopic) == "" {
		return nil, fmt.Errorf("missing required env: KAFKA_TOPIC")
	}
	return &Publisher{Cfg: cfg}, nil
}

func (p *Publisher) Validate(ctx context.Context) error {
	if len(p.Cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is empty")
	}
	if strings.TrimSpace(p.Cfg.KafkaTopic) == "" {
		return fmt.Errorf("KAFKA_TOPIC is empty")
	}
	for _, broker := range p.Cfg.KafkaBrokers {
		if isLoopbackBrokerEndpoint(broker) {
			return fmt.Errorf("KAFKA_BROKERS must be an externally reachable Kafka bootstrap address, not %q; set KAFKA_PUBLIC_HOST on the Kafka server to the public IP/domain", broker)
		}
	}

	dialer := &kafka.Dialer{
		ClientID: p.Cfg.KafkaClientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(p.Cfg.KafkaBrokers, 10*time.Second),
	}
	if strings.TrimSpace(p.Cfg.KafkaUsername) != "" || strings.TrimSpace(p.Cfg.KafkaPassword) != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: p.Cfg.KafkaUsername, Password: p.Cfg.KafkaPassword}
	}

	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(probeCtx, "tcp", p.Cfg.KafkaBrokers[0])
	if err != nil {
		return fmt.Errorf("kafka preflight failed to connect to bootstrap broker %q: %w", p.Cfg.KafkaBrokers[0], err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(p.Cfg.KafkaTopic)
	if err != nil {
		return fmt.Errorf("kafka preflight failed to read metadata for topic %q: %w", p.Cfg.KafkaTopic, err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka preflight found zero partitions for topic %q", p.Cfg.KafkaTopic)
	}
	if err := validateKafkaAdvertisedLeaders(partitions, p.Cfg.KafkaBrokers, "kafka broker metadata"); err != nil {
		return err
	}
	fmt.Printf("[kafka] preflight ok topic=%s partitions=%d bootstrap=%s\n", p.Cfg.KafkaTopic, len(partitions), p.Cfg.KafkaBrokers[0])
	return nil
}

func (p *Publisher) NewEvent(eventType, eventUUID, sourceURL, createdAt string, payload map[string]any) (Event, error) {
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
		Source:    p.Cfg.ProducerSource,
		Host:      producerHost(),
		UUIDUser:  "",
		IP:        p.Cfg.ProducerIP,
		URL:       sourceURL,
		EventType: eventType,
		Payload:   string(payloadJSON),
		CreatedAt: createdAt,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	messages := make([]kafka.Message, 0, len(events))
	for _, ev := range events {
		body, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(eventKey(ev)),
			Value: body,
			Time:  util.NowKST(),
		})
	}
	return p.writeMessagesWithRetry(ctx, messages, func() kafkaMessageWriter {
		return p.writer()
	}, sleepContext)
}

func (p *Publisher) writeMessagesWithRetry(ctx context.Context, messages []kafka.Message, newWriter func() kafkaMessageWriter, sleep func(context.Context, time.Duration) error) error {
	if len(messages) == 0 {
		return nil
	}
	attempts := p.Cfg.KafkaWriteAttempts
	if attempts <= 0 {
		attempts = 5
	}
	backoffMin := p.Cfg.KafkaWriteBackoffMin
	if backoffMin <= 0 {
		backoffMin = time.Second
	}
	backoffMax := p.Cfg.KafkaWriteBackoffMax
	if backoffMax <= 0 {
		backoffMax = 12 * time.Second
	}
	if backoffMax < backoffMin {
		backoffMax = backoffMin
	}

	pending := messages
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		w := newWriter()
		err := w.WriteMessages(ctx, pending...)
		_ = w.Close()
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[kafka] publish retry succeeded attempt=%d messages=%d\n", attempt, len(pending))
			}
			return nil
		}

		lastErr = err
		failed, retryable := retryableFailedMessages(pending, err)
		if len(failed) == 0 || !retryable || attempt == attempts {
			return err
		}

		fmt.Printf("[kafka] retrying publish attempt=%d/%d failed_messages=%d reason=%s\n", attempt+1, attempts, len(failed), kafkaRetryReason(err))
		if err := sleep(ctx, kafkaBackoffDuration(attempt, backoffMin, backoffMax)); err != nil {
			return err
		}
		pending = failed
	}
	return lastErr
}

func (p *Publisher) writer() *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(p.Cfg.KafkaBrokers...),
		Topic:                  p.Cfg.KafkaTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchSize:              p.Cfg.KafkaBatchSize,
		BatchTimeout:           p.Cfg.KafkaBatchTimeout,
	}
	transport := &kafka.Transport{
		ClientID: p.Cfg.KafkaClientID,
		Dial:     kafkaAdvertisedBrokerDialFunc(p.Cfg.KafkaBrokers, 10*time.Second),
	}
	if strings.TrimSpace(p.Cfg.KafkaUsername) != "" || strings.TrimSpace(p.Cfg.KafkaPassword) != "" {
		transport.SASL = plain.Mechanism{Username: p.Cfg.KafkaUsername, Password: p.Cfg.KafkaPassword}
	}
	w.Transport = transport
	return w
}

func eventKey(ev Event) string {
	if strings.TrimSpace(ev.URL) != "" {
		return ev.EventType + ":" + ev.URL
	}
	return ev.EventType + ":" + ev.EventUUID
}

func producerHost() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "github-actions"
	}
	return host
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func positiveInt(raw string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func secondsDefault(raw string, fallback time.Duration) time.Duration {
	f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || f <= 0 {
		return fallback
	}
	return time.Duration(f * float64(time.Second))
}

func retryableFailedMessages(messages []kafka.Message, err error) ([]kafka.Message, bool) {
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if len(writeErrs) != len(messages) {
			return messages, retryableKafkaWriteError(err)
		}
		failed := make([]kafka.Message, 0, writeErrs.Count())
		retryable := true
		for i, writeErr := range writeErrs {
			if writeErr == nil {
				continue
			}
			failed = append(failed, messages[i])
			if !retryableKafkaWriteError(writeErr) {
				retryable = false
			}
		}
		return failed, retryable
	}
	return messages, retryableKafkaWriteError(err)
}

func retryableKafkaWriteError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if writeErrs.Count() == 0 {
			return false
		}
		for _, writeErr := range writeErrs {
			if writeErr != nil && !retryableKafkaWriteError(writeErr) {
				return false
			}
		}
		return true
	}
	var tempErr interface{ Temporary() bool }
	if errors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}
	return errors.Is(err, io.EOF) || isRetryableKafkaErrorText(err.Error())
}

func isRetryableKafkaErrorText(message string) bool {
	msg := strings.ToLower(message)
	return strings.Contains(msg, "not leader for partition") ||
		strings.Contains(msg, "leader not available") ||
		strings.Contains(msg, "metadata are likely out of date") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "eof")
}

func kafkaRetryReason(err error) string {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "not leader for partition"), strings.Contains(msg, "metadata are likely out of date"):
		return "leader-metadata-stale"
	case strings.Contains(msg, "leader not available"):
		return "leader-not-available"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "eof"), strings.Contains(msg, "connection reset"), strings.Contains(msg, "broken pipe"):
		return "network"
	default:
		return "temporary-kafka-error"
	}
}

func kafkaBackoffDuration(attempt int, minDelay, maxDelay time.Duration) time.Duration {
	if minDelay <= 0 {
		minDelay = time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 12 * time.Second
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	delay := minDelay
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isLoopbackBrokerEndpoint(raw string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		host = strings.TrimSpace(raw)
		if strings.Contains(host, ":") {
			host = strings.Split(host, ":")[0]
		}
	}
	return isLoopbackHost(host)
}

func validateKafkaAdvertisedLeaders(partitions []kafka.Partition, brokers []string, label string) error {
	bootstrap := kafkaBootstrapEndpointSet(brokers)
	nonBootstrapLeaders := 0
	topics := map[string]bool{}
	for _, partition := range partitions {
		leaderHost := strings.TrimSpace(partition.Leader.Host)
		if isLoopbackHost(leaderHost) {
			return fmt.Errorf("%s advertises loopback listener %s:%d for topic=%s partition=%d; fix Kafka server KAFKA_PUBLIC_HOST/KAFKA_ADVERTISED_LISTENERS and force-recreate Kafka_Platform", label, leaderHost, partition.Leader.Port, partition.Topic, partition.ID)
		}
		leaderEndpoint := normalizedKafkaEndpoint(leaderHost, strconv.Itoa(partition.Leader.Port))
		if len(bootstrap) > 0 && !bootstrap[leaderEndpoint] {
			nonBootstrapLeaders++
			topics[partition.Topic] = true
		}
	}
	if nonBootstrapLeaders > 0 {
		fmt.Printf("[kafka] %s metadata has %d non-bootstrap advertised broker entries across %d topic(s); producer will dial via bootstrap rewrite\n", label, nonBootstrapLeaders, len(topics))
	}
	return nil
}

func kafkaBootstrapEndpointSet(brokers []string) map[string]bool {
	endpoints := make(map[string]bool, len(brokers))
	for _, broker := range brokers {
		host, port, ok := splitKafkaEndpoint(broker)
		if ok {
			endpoints[normalizedKafkaEndpoint(host, port)] = true
		}
	}
	return endpoints
}

func splitKafkaEndpoint(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		if strings.Count(raw, ":") != 1 {
			return "", "", false
		}
		parts := strings.SplitN(raw, ":", 2)
		host, port = parts[0], parts[1]
	}
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	return host, port, host != "" && port != ""
}

func normalizedKafkaEndpoint(host, port string) string {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	port = strings.TrimSpace(port)
	return host + ":" + port
}

func kafkaAdvertisedBrokerDialFunc(brokers []string, timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	if len(brokers) != 1 {
		return dialer.DialContext
	}
	bootstrapHost, bootstrapPort, ok := splitKafkaEndpoint(brokers[0])
	if !ok {
		return dialer.DialContext
	}
	bootstrapAddress := net.JoinHostPort(strings.Trim(bootstrapHost, "[]"), bootstrapPort)
	bootstrapEndpoint := normalizedKafkaEndpoint(bootstrapHost, bootstrapPort)
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		target := address
		if host, port, ok := splitKafkaEndpoint(address); ok {
			endpoint := normalizedKafkaEndpoint(host, port)
			if port == bootstrapPort && endpoint != bootstrapEndpoint {
				target = bootstrapAddress
			}
		}
		return dialer.DialContext(ctx, network, target)
	}
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	switch host {
	case "", "localhost", "127.0.0.1", "::1", "0.0.0.0":
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
}
