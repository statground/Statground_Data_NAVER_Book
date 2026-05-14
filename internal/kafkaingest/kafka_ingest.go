package kafkaingest

import (
	"context"
	"encoding/json"
	"fmt"
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
	IngestMode        string
	KafkaBrokers      []string
	KafkaUsername     string
	KafkaPassword     string
	KafkaTopic        string
	KafkaClientID     string
	KafkaBatchSize    int
	KafkaBatchTimeout time.Duration
	ProducerSource    string
	ProducerIP        string
}

type Publisher struct {
	Cfg Config
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
		IngestMode:        strings.ToLower(envx.String("INGEST_MODE", "kafka")),
		KafkaBrokers:      splitCSV(envx.String("KAFKA_BROKERS", "")),
		KafkaUsername:     envx.String("KAFKA_USERNAME", envx.String("KAFKA_EXTERNAL_USER", "")),
		KafkaPassword:     envx.String("KAFKA_PASSWORD", envx.String("KAFKA_EXTERNAL_PASSWORD", "")),
		KafkaTopic:        envx.String("KAFKA_TOPIC", "book.events"),
		KafkaClientID:     envx.String("KAFKA_CLIENT_ID", "statground-naver-book-crawler"),
		KafkaBatchSize:    positiveInt(envx.String("KAFKA_BATCH_SIZE", "100"), 100),
		KafkaBatchTimeout: secondsDefault(envx.String("KAFKA_BATCH_TIMEOUT", "1.0"), time.Second),
		ProducerSource:    envx.String("PRODUCER_SOURCE", "github_actions"),
		ProducerIP:        envx.String("PRODUCER_IP", "::"),
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
	w := p.writer()
	defer w.Close()

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
	return w.WriteMessages(ctx, messages...)
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
	for _, partition := range partitions {
		leaderHost := strings.TrimSpace(partition.Leader.Host)
		if isLoopbackHost(leaderHost) {
			return fmt.Errorf("%s advertises loopback listener %s:%d for topic=%s partition=%d; fix Kafka server KAFKA_PUBLIC_HOST/KAFKA_ADVERTISED_LISTENERS and force-recreate Kafka_Platform", label, leaderHost, partition.Leader.Port, partition.Topic, partition.ID)
		}
		leaderEndpoint := normalizedKafkaEndpoint(leaderHost, strconv.Itoa(partition.Leader.Port))
		if len(bootstrap) > 0 && !bootstrap[leaderEndpoint] {
			fmt.Printf("[kafka] %s advertises %s for topic=%s partition=%d, but KAFKA_BROKERS bootstrap is %s; producer will dial via bootstrap rewrite\n", label, leaderEndpoint, partition.Topic, partition.ID, strings.Join(brokers, ","))
		}
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
				fmt.Printf("[kafka] rewrite advertised broker dial %s -> %s\n", endpoint, bootstrapEndpoint)
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
