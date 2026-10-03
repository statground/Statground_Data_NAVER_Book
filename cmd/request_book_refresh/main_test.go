package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"statground_naver_book_go/internal/bookrefreshreceipt"
)

type nativeFake struct {
	queries, commands []string
	states            map[string]refreshState
	failCommand       string
	staleAfterRequest bool
	confirmedRows     int
	confirmedLatest   int64
	pendingOutbox     bool
	clock             int64
}

func fakeNative() *nativeFake {
	f := &nativeFake{states: map[string]refreshState{}, confirmedRows: 7, confirmedLatest: 2000000001500, clock: 2000000100000}
	for _, stage := range stages {
		f.states[stage.View] = refreshState{Status: "Scheduled", Observed: 2000000100000, Started: 2000000000000, Success: 2000000001000}
	}
	return f
}
func (f *nativeFake) QueryJSONEachRowContext(_ context.Context, query string) ([]map[string]any, error) {
	f.queries = append(f.queries, query)
	if strings.Contains(query, "replayed_at IS NULL") {
		if f.pendingOutbox {
			return []map[string]any{{"pending": 1}}, nil
		}
		return nil, nil
	}
	if strings.Contains(query, "confirmed_source_millis") {
		return []map[string]any{{"confirmed_rows": f.confirmedRows, "confirmed_source_millis": f.confirmedLatest, "observed_millis": int64(2000000100000)}}, nil
	}
	if !strings.Contains(query, "FROM system.view_refreshes") {
		return []map[string]any{{"row_count": 5, "generation_epoch": 2000000100000}}, nil
	}
	for view, s := range f.states {
		database, name, _ := strings.Cut(view, ".")
		if strings.Contains(query, "database='"+database+"'") && strings.Contains(query, "view='"+name+"'") {
			f.clock += 1000
			s.Observed = f.clock
			f.states[view] = s
			return []map[string]any{{"status": s.Status, "observed_epoch": s.Observed, "started_epoch": s.Started, "success_epoch": s.Success, "failed": map[bool]int{false: 0, true: 1}[s.Failed]}}, nil
		}
	}
	return nil, errors.New("unknown view")
}
func (f *nativeFake) ExecContext(_ context.Context, query string) error {
	f.commands = append(f.commands, query)
	if f.failCommand == query {
		return errors.New("private transport failure")
	}
	if strings.HasPrefix(query, "SYSTEM REFRESH VIEW ") && !f.staleAfterRequest {
		view := strings.TrimPrefix(query, "SYSTEM REFRESH VIEW ")
		s := f.states[view]
		s.Started = f.clock
		s.Success = f.clock
		f.states[view] = s
	}
	if strings.HasPrefix(query, "SYSTEM WAIT VIEW ") {
		view := strings.TrimPrefix(query, "SYSTEM WAIT VIEW ")
		s := f.states[view]
		s.Status = "Scheduled"
		f.states[view] = s
	}
	return nil
}
func sourceReceipt() bookrefreshreceipt.Receipt {
	return bookrefreshreceipt.Receipt{Version: 1, RunUUID: "01a00000-0000-7000-8000-000000000001", State: "completed", StartedMillis: 2000000001000, Inserted: 7, LatestInsertedMillis: 2000000001500, CompletedMillis: 2000000002000}
}
func TestNativeRefreshRequestsFourStagesWithPressureAndTargetReadback(t *testing.T) {
	fake := fakeNative()
	gates := []string{}
	err := requestRefreshes(context.Background(), fake, sourceReceipt(), func(_ context.Context, stage refreshStage) error {
		if len(fake.commands) != len(gates)*2 {
			t.Fatal("a refresh was issued before its pressure gate")
		}
		gates = append(gates, stage.View)
		return nil
	}, 0)
	if err != nil || len(gates) != 4 || len(fake.commands) != 8 {
		t.Fatalf("gates=%v commands=%v err=%v", gates, fake.commands, err)
	}
	for index, stage := range stages {
		if fake.commands[index*2] != "SYSTEM REFRESH VIEW "+stage.View || fake.commands[index*2+1] != "SYSTEM WAIT VIEW "+stage.View {
			t.Fatalf("stage %d commands=%v", index, fake.commands)
		}
		found := false
		for _, query := range fake.queries {
			found = found || strings.Contains(query, "FROM "+stage.Target+" SETTINGS")
		}
		if !found {
			t.Fatalf("target %s not read back", stage.Target)
		}
	}
}
func TestNativeRefreshWaitsForOldRunningGenerationThenRequestsFreshOne(t *testing.T) {
	fake := fakeNative()
	s := fake.states[stages[0].View]
	s.Status = "Running"
	fake.states[stages[0].View] = s
	if err := requestRefreshes(context.Background(), fake, sourceReceipt(), func(context.Context, refreshStage) error { return nil }, 0); err != nil {
		t.Fatal(err)
	}
	if len(fake.commands) != 9 || fake.commands[0] != "SYSTEM WAIT VIEW "+stages[0].View || fake.commands[1] != "SYSTEM REFRESH VIEW "+stages[0].View {
		t.Fatalf("older generation passed as the collection's generation: %v", fake.commands)
	}
}
func TestNativeRefreshPressureFailureAndDisabledSchedulesDoNotMutate(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		fake := fakeNative()
		if disabled {
			s := fake.states[stages[0].View]
			s.Status = "Disabled"
			fake.states[stages[0].View] = s
		}
		err := requestRefreshes(context.Background(), fake, sourceReceipt(), func(context.Context, refreshStage) error { return errors.New("pressure") }, 0)
		if err == nil || len(fake.commands) != 0 {
			t.Fatalf("disabled=%t commands=%v err=%v", disabled, fake.commands, err)
		}
	}
}
func TestNativeRefreshStaleCompletionAndAmbiguousRequestFailClosed(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		fake := fakeNative()
		fake.staleAfterRequest = !ambiguous
		if ambiguous {
			fake.failCommand = "SYSTEM REFRESH VIEW " + stages[0].View
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		err := requestRefreshes(ctx, fake, sourceReceipt(), func(context.Context, refreshStage) error { return nil }, time.Millisecond)
		cancel()
		if err == nil {
			t.Fatal("unverified refresh completion passed")
		}
		requests := 0
		for _, query := range fake.commands {
			if strings.HasPrefix(query, "SYSTEM REFRESH VIEW ") {
				requests++
			}
			if !strings.HasPrefix(query, "SYSTEM REFRESH VIEW ") && !strings.HasPrefix(query, "SYSTEM WAIT VIEW ") {
				t.Fatalf("schedule mutation issued: %s", query)
			}
		}
		if requests != 1 {
			t.Fatalf("ambiguous=%t refresh requests=%d", ambiguous, requests)
		}
	}
}

func TestNativeRefreshWaitsForOtherRunningChainStageBeforeRequest(t *testing.T) {
	fake := fakeNative()
	s := fake.states[stages[2].View]
	s.Status = "Running"
	fake.states[stages[2].View] = s
	if err := requestRefreshes(context.Background(), fake, sourceReceipt(), func(context.Context, refreshStage) error { return nil }, 0); err != nil {
		t.Fatal(err)
	}
	if len(fake.commands) != 9 || fake.commands[0] != "SYSTEM WAIT VIEW "+stages[2].View || fake.commands[1] != "SYSTEM REFRESH VIEW "+stages[0].View {
		t.Fatalf("native request overlapped an existing chain stage: %v", fake.commands)
	}
}

func TestEmptyCollectionStillRequiresNativeGenerationAfterRunStart(t *testing.T) {
	fake := fakeNative()
	fake.confirmedRows, fake.confirmedLatest = 0, 0
	receipt := sourceReceipt()
	receipt.Inserted = 0
	receipt.LatestInsertedMillis = 0
	err := requestRefreshes(context.Background(), fake, receipt, func(context.Context, refreshStage) error { return nil }, 0)
	if err != nil || len(fake.commands) != 8 {
		t.Fatalf("empty intake accepted a generation before run start: commands=%v err=%v", fake.commands, err)
	}
}

func TestNativeRefreshPendingOutboxAndMissingSourceRowsFailBeforeRequest(t *testing.T) {
	for _, pending := range []bool{false, true} {
		fake := fakeNative()
		fake.pendingOutbox = pending
		if !pending {
			fake.confirmedRows--
		}
		err := requestRefreshes(context.Background(), fake, sourceReceipt(), func(context.Context, refreshStage) error { return nil }, 0)
		if err == nil || len(fake.commands) != 0 {
			t.Fatalf("unconfirmed raw/outbox source refreshed: pending=%t commands=%v err=%v", pending, fake.commands, err)
		}
	}
}
