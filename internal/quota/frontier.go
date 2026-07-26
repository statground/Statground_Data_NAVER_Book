package quota

import "time"

type FrontierState struct {
	LastSuccessAt        time.Time
	CallsLastRun         int
	NewISBNLastRun       int
	ChangedISBNLastRun   int
	DuplicateRatio       float64
	ConsecutiveZeroYield int
	Active               bool
}

func NextDueAt(state FrontierState, now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now()
	}
	base := state.LastSuccessAt
	if base.IsZero() {
		base = now
	}
	if !state.Active {
		return base.Add(90 * 24 * time.Hour)
	}
	if state.ConsecutiveZeroYield >= 6 {
		return base.Add(30 * 24 * time.Hour)
	}
	if state.ConsecutiveZeroYield >= 3 {
		return base.Add(14 * 24 * time.Hour)
	}
	calls := state.CallsLastRun
	if calls <= 0 {
		calls = 1
	}
	yieldPerCall := float64(state.NewISBNLastRun+state.ChangedISBNLastRun) / float64(calls)
	if yieldPerCall >= 0.5 {
		return base.Add(24 * time.Hour)
	}
	if yieldPerCall > 0 {
		return base.Add(3 * 24 * time.Hour)
	}
	return base.Add(7 * 24 * time.Hour)
}

func AdjustPageCap(state FrontierState, configuredCap int) int {
	if configuredCap <= 0 {
		return 1
	}
	switch {
	case state.ConsecutiveZeroYield >= 3:
		return 1
	case state.DuplicateRatio >= 0.85:
		return 1
	case state.DuplicateRatio >= 0.60:
		adjusted := configuredCap / 2
		if adjusted < 1 {
			return 1
		}
		return adjusted
	default:
		return configuredCap
	}
}
