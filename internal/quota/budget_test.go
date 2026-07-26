package quota

import "testing"

func TestBudgetDefaultBoundariesAndReserves(t *testing.T) {
	t.Parallel()

	budget, err := NewBudget(DefaultConfig(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := budget.RemainingAutoCalls(), DefaultAutoDailyBudget; got != want {
		t.Fatalf("remaining=%d want=%d", got, want)
	}
	if got, want := budget.RemainingRunCalls(), DefaultMaxRequestsPerRun; got != want {
		t.Fatalf("run remaining=%d want=%d", got, want)
	}
	if !budget.Reserve(DefaultMaxRequestsPerRun) {
		t.Fatal("expected full per-run reservation to succeed")
	}
	if budget.Reserve(1) {
		t.Fatal("reservation over per-run cap succeeded")
	}
	snapshot := budget.Snapshot()
	if snapshot.ManualDailyReserve != DefaultManualDailyReserve ||
		snapshot.DaumSearchSharedReserve != DefaultDaumSearchSharedReserve ||
		snapshot.EmergencyReserve != DefaultEmergencyReserve {
		t.Fatalf("reserves changed: %+v", snapshot)
	}
}

func TestBudgetRestartsFromPersistedObservedCalls(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.MaxRequestsPerRun = cfg.AutoDailyBudget
	budget, err := NewBudget(cfg, 1_840)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := budget.RemainingAutoCalls(), 10_160; got != want {
		t.Fatalf("remaining=%d want=%d", got, want)
	}
}

func TestBudgetTrimsAtAutoBoundary(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.MaxRequestsPerRun = cfg.AutoDailyBudget
	budget, err := NewBudget(cfg, 11_999)
	if err != nil {
		t.Fatal(err)
	}
	if !budget.Reserve(1) {
		t.Fatal("boundary reservation failed")
	}
	if budget.Reserve(1) {
		t.Fatal("reservation beyond auto budget succeeded")
	}
}

func TestBudgetMarkExhaustedStopsNewCalls(t *testing.T) {
	t.Parallel()

	budget, err := NewBudget(DefaultConfig(), 0)
	if err != nil {
		t.Fatal(err)
	}
	budget.MarkExhausted()
	if !budget.IsExhausted() {
		t.Fatal("budget was not marked exhausted")
	}
	if budget.Reserve(1) {
		t.Fatal("exhausted budget allowed a reservation")
	}
	if got := budget.RemainingAutoCalls(); got != 0 {
		t.Fatalf("remaining=%d want=0", got)
	}
}

func TestBudgetRejectsUnsafeConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.EmergencyReserve++
	if _, err := NewBudget(cfg, 0); err == nil {
		t.Fatal("expected overcommitted config error")
	}
}
