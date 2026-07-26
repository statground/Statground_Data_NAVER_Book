package quota

import (
	"testing"
	"time"
)

func TestNextDueAtAdaptiveBackoff(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		state FrontierState
		want  time.Duration
	}{
		{
			name:  "high yield",
			state: FrontierState{Active: true, LastSuccessAt: base, CallsLastRun: 2, NewISBNLastRun: 1},
			want:  24 * time.Hour,
		},
		{
			name:  "ordinary yield",
			state: FrontierState{Active: true, LastSuccessAt: base, CallsLastRun: 10, NewISBNLastRun: 1},
			want:  3 * 24 * time.Hour,
		},
		{
			name:  "three zero yield",
			state: FrontierState{Active: true, LastSuccessAt: base, ConsecutiveZeroYield: 3},
			want:  14 * 24 * time.Hour,
		},
		{
			name:  "six zero yield",
			state: FrontierState{Active: true, LastSuccessAt: base, ConsecutiveZeroYield: 6},
			want:  30 * 24 * time.Hour,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NextDueAt(tc.state, base); !got.Equal(base.Add(tc.want)) {
				t.Fatalf("next=%s want=%s", got, base.Add(tc.want))
			}
		})
	}
}

func TestAdjustPageCapForDuplicateAndZeroYield(t *testing.T) {
	t.Parallel()

	if got := AdjustPageCap(FrontierState{DuplicateRatio: 0.90}, 4); got != 1 {
		t.Fatalf("high duplicate cap=%d", got)
	}
	if got := AdjustPageCap(FrontierState{DuplicateRatio: 0.70}, 4); got != 2 {
		t.Fatalf("medium duplicate cap=%d", got)
	}
	if got := AdjustPageCap(FrontierState{ConsecutiveZeroYield: 3}, 4); got != 1 {
		t.Fatalf("zero-yield cap=%d", got)
	}
}
