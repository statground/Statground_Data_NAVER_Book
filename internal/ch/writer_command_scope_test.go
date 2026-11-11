package ch

import (
	"errors"
	"reflect"
	"testing"
)

type commandParticipantFunc func(bool) error

func (f commandParticipantFunc) FinishWriterCommand(success bool) error { return f(success) }

func TestWriterCommandParticipantsFinishBeforeLeaseRelease(t *testing.T) {
	var scope *WriterCommandScope
	var order []string
	created := 0
	err := runWriterCommand(func() error {
		scope = CurrentWriterCommand()
		factory := func() WriterCommandParticipant {
			created++
			return commandParticipantFunc(func(success bool) error {
				if !success || scope.Active() {
					t.Fatal("participant did not receive a sealed successful command")
				}
				order = append(order, "finish")
				return nil
			})
		}
		first, err := scope.Shared("ledger", factory)
		if err != nil {
			return err
		}
		second, err := scope.Shared("ledger", factory)
		if err != nil || reflect.ValueOf(first).Pointer() != reflect.ValueOf(second).Pointer() {
			t.Fatalf("shared participant: %v", err)
		}
		return nil
	}, func() error {
		order = append(order, "release")
		return nil
	})
	if err != nil || created != 1 || !reflect.DeepEqual(order, []string{"finish", "release"}) || CurrentWriterCommand() != nil || scope.Active() {
		t.Fatalf("err=%v created=%d order=%v", err, created, order)
	}
	if _, err := scope.Shared("late", func() WriterCommandParticipant { return commandParticipantFunc(nil) }); err == nil {
		t.Fatal("closed command admitted another participant")
	}
}

func TestWriterCommandFailureAndPanicDoNotCompleteParticipants(t *testing.T) {
	for _, panicRun := range []bool{false, true} {
		t.Run(map[bool]string{false: "business_error", true: "panic"}[panicRun], func(t *testing.T) {
			business, releaseErr := errors.New("business"), errors.New("release")
			finished, released, panicked := false, false, false
			func() {
				defer func() {
					if got := recover(); got != nil {
						panicked = true
						if got != business {
							t.Fatalf("panic changed: %v", got)
						}
					}
				}()
				err := runWriterCommand(func() error {
					_, err := CurrentWriterCommand().Shared("ledger", func() WriterCommandParticipant {
						return commandParticipantFunc(func(success bool) error {
							if success {
								t.Fatal("failed command attempted completion")
							}
							finished = true
							return nil
						})
					})
					if err != nil {
						return err
					}
					if panicRun {
						panic(business)
					}
					return business
				}, func() error { released = true; return releaseErr })
				if !errors.Is(err, business) || !errors.Is(err, releaseErr) {
					t.Fatalf("original failures lost: %v", err)
				}
			}()
			if finished != !panicRun || released != !panicRun || panicked != panicRun || CurrentWriterCommand() != nil {
				t.Fatalf("finished=%t released=%t panicked=%t", finished, released, panicked)
			}
		})
	}
}

func TestWriterCommandFirstFinalizerFailurePreventsLaterCompletion(t *testing.T) {
	firstErr := errors.New("unknown marker")
	var outcomes []bool
	err := runWriterCommand(func() error {
		for _, key := range []int{1, 2} {
			key := key
			_, err := CurrentWriterCommand().Shared(key, func() WriterCommandParticipant {
				return commandParticipantFunc(func(success bool) error {
					outcomes = append(outcomes, success)
					if key == 1 {
						return firstErr
					}
					return nil
				})
			})
			if err != nil {
				return err
			}
		}
		return nil
	}, func() error { return nil })
	if !errors.Is(err, firstErr) || !reflect.DeepEqual(outcomes, []bool{true, false}) {
		t.Fatalf("error=%v outcomes=%v", err, outcomes)
	}
}

func TestWriterCommandRejectsNestedOwnerWithoutClosingItsLease(t *testing.T) {
	released := 0
	err := runWriterCommand(func() error {
		owner := CurrentWriterCommand()
		if err := runWriterCommand(func() error { t.Fatal("nested command ran"); return nil }, func() error { released++; return nil }); err == nil {
			t.Fatal("nested owner admitted")
		}
		if CurrentWriterCommand() != owner || !owner.Active() || released != 0 {
			t.Fatal("nested refusal changed the active owner")
		}
		return nil
	}, func() error { released++; return nil })
	if err != nil || released != 1 || CurrentWriterCommand() != nil {
		t.Fatalf("err=%v releases=%d", err, released)
	}
}
