package kafkaingest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeKafkaWriter struct {
	err    error
	writes *[][]string
}

func (w *fakeKafkaWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	keys := make([]string, 0, len(messages))
	for _, msg := range messages {
		keys = append(keys, string(msg.Key))
	}
	*w.writes = append(*w.writes, keys)
	return w.err
}

func (w *fakeKafkaWriter) Close() error {
	return nil
}

func TestWriteMessagesWithRetryRetriesOnlyFailedWriteErrors(t *testing.T) {
	pub := Publisher{Cfg: Config{
		KafkaWriteAttempts:   3,
		KafkaWriteBackoffMin: time.Millisecond,
		KafkaWriteBackoffMax: time.Millisecond,
	}}
	messages := []kafka.Message{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	}
	errs := []error{
		kafka.WriteErrors{nil, kafka.NotLeaderForPartition, kafka.NotLeaderForPartition},
		nil,
	}
	writes := make([][]string, 0, len(errs))
	attempt := 0
	sleeps := 0

	err := pub.writeMessagesWithRetry(context.Background(), messages, func() kafkaMessageWriter {
		if attempt >= len(errs) {
			t.Fatalf("unexpected writer attempt %d", attempt+1)
		}
		err := errs[attempt]
		attempt++
		return &fakeKafkaWriter{err: err, writes: &writes}
	}, func(context.Context, time.Duration) error {
		sleeps++
		return nil
	})
	if err != nil {
		t.Fatalf("writeMessagesWithRetry returned error: %v", err)
	}

	wantWrites := [][]string{{"a", "b", "c"}, {"b", "c"}}
	if !reflect.DeepEqual(writes, wantWrites) {
		t.Fatalf("writes mismatch\nwant: %#v\n got: %#v", wantWrites, writes)
	}
	if sleeps != 1 {
		t.Fatalf("sleep count mismatch: want 1 got %d", sleeps)
	}
}

func TestWriteMessagesWithRetryDoesNotRetryNonTemporaryWriteError(t *testing.T) {
	pub := Publisher{Cfg: Config{
		KafkaWriteAttempts:   3,
		KafkaWriteBackoffMin: time.Millisecond,
		KafkaWriteBackoffMax: time.Millisecond,
	}}
	messages := []kafka.Message{{Key: []byte("a"), Value: []byte("1")}}
	writes := make([][]string, 0, 1)
	attempts := 0

	err := pub.writeMessagesWithRetry(context.Background(), messages, func() kafkaMessageWriter {
		attempts++
		return &fakeKafkaWriter{
			err:    kafka.WriteErrors{kafka.SASLAuthenticationFailed},
			writes: &writes,
		}
	}, func(context.Context, time.Duration) error {
		t.Fatal("sleep should not be called for non-temporary errors")
		return nil
	})
	if err == nil {
		t.Fatal("expected non-temporary write error")
	}
	if attempts != 1 {
		t.Fatalf("attempt count mismatch: want 1 got %d", attempts)
	}
	if len(writes) != 1 {
		t.Fatalf("write count mismatch: want 1 got %d", len(writes))
	}
}

func TestRetryableKafkaWriteErrorRecognizesWrappedEOF(t *testing.T) {
	err := errors.New("dial tcp: EOF")
	if !retryableKafkaWriteError(err) {
		t.Fatal("expected EOF-like network error to be retryable")
	}
}

func TestKafkaBackoffDurationCapsAtMax(t *testing.T) {
	got := kafkaBackoffDuration(5, time.Second, 5*time.Second)
	if got != 5*time.Second {
		t.Fatalf("backoff mismatch: want 5s got %s", got)
	}
}
