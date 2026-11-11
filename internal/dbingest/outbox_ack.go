package dbingest

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"statground_naver_book_go/internal/ch"
)

const maxOutboxACKBatch = 25

type outboxACKKey struct {
	client *ch.Client
	table  string
}

// Only already-ACKed target writes enter pending. The original CH outbox is
// durable throughout; losing this small group requires ordinary reconciliation,
// not payload recovery or a new raw write identity.
type outboxACKScope struct {
	command   *ch.WriterCommandScope
	writer    *Writer
	maximum   int
	gate      chan struct{}
	pending   []string
	parents   []context.Context
	budget    time.Duration
	latched   error
	finish    sync.Once
	finishErr error
}

func outboxACKBatchFromEnv() (int, error) {
	raw := strings.TrimSpace(os.Getenv("NAVER_OUTBOX_ACK_BATCH_SIZE"))
	if raw == "" {
		return 1, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > maxOutboxACKBatch {
		return 0, errors.New("NAVER outbox acknowledgement batch is invalid")
	}
	return n, nil
}

func bindOutboxACKScope(writer *Writer, maximum int) (*outboxACKScope, error) {
	if maximum == 1 {
		return nil, nil
	}
	command := ch.CurrentWriterCommand()
	if command == nil || !command.Active() {
		return nil, errors.New("NAVER grouped acknowledgement requires a writer command")
	}
	key := outboxACKKey{client: writer.Client, table: writer.Cfg.OutboxTable}
	value, err := command.Shared(key, func() ch.WriterCommandParticipant {
		scope := &outboxACKScope{command: command, writer: writer, maximum: maximum,
			gate: make(chan struct{}, 1), pending: make([]string, 0, maximum)}
		scope.gate <- struct{}{}
		return scope
	})
	if err != nil {
		return nil, err
	}
	scope, ok := value.(*outboxACKScope)
	if !ok || scope.maximum != maximum {
		return nil, errors.New("NAVER acknowledgement scope mismatch")
	}
	return scope, nil
}

func (s *outboxACKScope) acquire(ctx context.Context, final bool) (func(), error) {
	if ctx.Err() != nil || (!final && !s.command.Active()) {
		return nil, writerOperationError("ack_scope", context.Canceled)
	}
	select {
	case <-ctx.Done():
		return nil, writerOperationError("ack_scope", ctx.Err())
	case <-s.gate:
	}
	release := func() { s.gate <- struct{}{} }
	if ctx.Err() != nil || (!final && !s.command.Active()) {
		release()
		return nil, writerOperationError("ack_scope", context.Canceled)
	}
	if s.latched != nil {
		err := s.latched
		release()
		return nil, err
	}
	return release, nil
}

// Caller owns gate across payload preservation, target write and ACK admission.
// Only this bounded logical section is serial; provider requests remain outside.
func (s *outboxACKScope) delivered(ctx context.Context, uuid string, clientTimeout time.Duration, retainParent bool) error {
	s.pending = append(s.pending, uuid)
	if retainParent && ctx.Done() != nil {
		s.parents = append(s.parents, ctx)
	}
	budget := 60 * time.Second // Existing writeContext default, never a larger marker cap.
	if clientTimeout > 0 && clientTimeout < budget {
		budget = clientTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < budget {
			budget = remaining
		}
	}
	if budget <= 0 || ctx.Err() != nil {
		s.latched = writerOperationError("mark_outbox", context.DeadlineExceeded)
		return s.latched
	}
	if s.budget == 0 || budget < s.budget {
		s.budget = budget
	}
	if len(s.pending) == s.maximum {
		return s.flush(ctx)
	}
	return nil
}

func (s *outboxACKScope) flush(ctx context.Context) error {
	if s.latched != nil {
		return s.latched
	}
	if len(s.pending) == 0 {
		return nil
	}
	// Marker admission is deferred, but its new request duration cannot exceed
	// the shortest retained member's original request/client budget.
	// Explicit Insert*Context cancellation/deadlines remain authoritative at
	// the later marker barrier. Automatic wrapper contexts are deliberately
	// excluded: their per-request cancel runs as soon as that wrapper returns.
	deadline := time.Now().Add(s.budget)
	for _, parent := range s.parents {
		if err := parent.Err(); err != nil {
			s.latched = writerOperationError("mark_outbox", err)
			return s.latched
		}
		if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
			deadline = parentDeadline
		}
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for _, parent := range s.parents {
		stop := context.AfterFunc(parent, cancel)
		defer stop()
	}
	if err := s.writer.markOutboxReplayed(ctx, s.pending); err != nil {
		// A timeout can mean committed mutation. Preserve the group, latch the
		// first outcome, and never repeat it from the command finalizer.
		s.latched = writerOperationError("mark_outbox", err)
		return s.latched
	}
	s.pending = s.pending[:0]
	s.parents = s.parents[:0]
	s.budget = 0
	return nil
}

func (s *outboxACKScope) FinishWriterCommand(success bool) error {
	s.finish.Do(func() {
		ctx, cancel := s.writer.writeContext()
		defer cancel()
		release, err := s.acquire(ctx, true)
		if err != nil {
			s.finishErr = err
			return
		}
		defer release()
		if success {
			s.finishErr = s.flush(ctx)
		}
	})
	return s.finishErr
}
