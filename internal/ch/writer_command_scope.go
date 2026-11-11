package ch

import (
	"errors"
	"sync"
)

// WriterCommandParticipant owns bounded state that must finish before a writer
// command can succeed or release its lease. A failed command must retain its
// durable pending rows; FinishWriterCommand(false) must not issue new writes.
type WriterCommandParticipant interface {
	FinishWriterCommand(success bool) error
}

type WriterCommandScope struct {
	mu           sync.Mutex
	sealed       bool
	values       map[any]WriterCommandParticipant
	participants []WriterCommandParticipant
}

var activeWriterCommand struct {
	sync.Mutex
	scope *WriterCommandScope
}

func CurrentWriterCommand() *WriterCommandScope {
	activeWriterCommand.Lock()
	defer activeWriterCommand.Unlock()
	return activeWriterCommand.scope
}

func (s *WriterCommandScope) Active() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.sealed
}

// Shared creates only local participant state. Callers use a comparable key
// bound to their exact client and ledger; factories must perform no I/O.
func (s *WriterCommandScope) Shared(key any, create func() WriterCommandParticipant) (WriterCommandParticipant, error) {
	if s == nil || create == nil {
		return nil, errors.New("Book writer command scope unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return nil, errors.New("Book writer command scope closed")
	}
	if value := s.values[key]; value != nil {
		return value, nil
	}
	if len(s.participants) >= 64 {
		return nil, errors.New("Book writer command participant limit")
	}
	value := create()
	if value == nil {
		return nil, errors.New("Book writer command participant unavailable")
	}
	s.values[key] = value
	s.participants = append(s.participants, value)
	return value, nil
}

func (s *WriterCommandScope) finish(success bool) error {
	s.mu.Lock()
	s.sealed = true
	participants := append([]WriterCommandParticipant(nil), s.participants...)
	s.mu.Unlock()
	var result error
	for _, participant := range participants {
		err := participant.FinishWriterCommand(success && result == nil)
		result = errors.Join(result, err)
	}
	return result
}

func (s *WriterCommandScope) seal() {
	s.mu.Lock()
	s.sealed = true
	s.mu.Unlock()
}

func runWriterCommand(run func() error, release func() error) (result error) {
	activeWriterCommand.Lock()
	if activeWriterCommand.scope != nil || run == nil {
		activeWriterCommand.Unlock()
		return errors.New("Book writer command already active or invalid")
	}
	scope := &WriterCommandScope{values: make(map[any]WriterCommandParticipant)}
	activeWriterCommand.scope = scope
	activeWriterCommand.Unlock()
	completed := false
	finalized := false
	defer func() {
		activeWriterCommand.Lock()
		activeWriterCommand.scope = nil
		activeWriterCommand.Unlock()
	}()
	defer func() {
		if completed && finalized {
			result = errors.Join(result, release())
		}
	}()
	defer func() {
		// Preserve the original panic path: no new marker work or lease release.
		// Durable outbox rows remain pending for the existing reconciliation.
		if !completed {
			scope.seal()
			return
		}
		result = errors.Join(result, scope.finish(completed && result == nil))
		finalized = true
	}()
	result = run()
	completed = true
	return result
}
