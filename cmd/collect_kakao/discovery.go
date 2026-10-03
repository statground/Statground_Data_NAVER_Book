package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/lexicon"
	"statground_naver_book_go/internal/quota"
)

type discoveryLoader func(context.Context, string) ([]lexicon.Candidate, string, error)

type discoveryBatch struct {
	Selections []lexicon.Selection
	SnapshotID string
	RunID      string
	Degraded   bool
}

// The dictionary replaces part of the existing query batch. It never adds
// queries to that batch, changes page caps, or bypasses frontier/quota checks.
func selectDiscoveryQueries(ctx context.Context, curated []string, runKind string, now time.Time, load discoveryLoader) (discoveryBatch, error) {
	batch := discoveryBatch{RunID: discoveryRunID(now)}
	enabled := boolEnv("KAKAO_LEXICON_ENABLED", runKind == "scheduled")
	count := len(curated)
	if enabled {
		count = envx.Int("KAKAO_DISCOVERY_KEYWORD_LIMIT", count)
		if count < 1 || count > len(curated) {
			return batch, fmt.Errorf("discovery count must fit the existing query batch")
		}
		// A balanced pair fits within an odd cap; the spare request is unused.
		count -= count % 2
		if count == 0 {
			return batch, fmt.Errorf("balanced discovery requires at least two query slots")
		}
	}
	if !enabled {
		batch.Selections = curatedSelections(curated, batch.RunID, count)
		return batch, nil
	}
	pool, snapshotID, err := load(ctx, "kakao-book")
	if err != nil {
		return batch, fmt.Errorf("dictionary discovery deferred: eligible pool unavailable")
	}
	batch.SnapshotID = snapshotID
	selected, err := lexicon.Select(curated, pool, count, batch.RunID, "kakao-book")
	if err != nil {
		// A receipt/ledger conflict must not silently replace a retry's batch.
		return batch, err
	}
	for index := range selected {
		if selected[index].SnapshotID == "" {
			selected[index].SnapshotID = snapshotID
		}
		selected[index].RunID = batch.RunID
	}
	batch.Selections = selected
	return batch, nil
}

func curatedSelections(curated []string, runID string, count int) []lexicon.Selection {
	selected := make([]lexicon.Selection, 0, count)
	selectedAt := time.Now().UTC().Format("2006-01-02 15:04:05.000")
	for _, query := range curated {
		if len(selected) >= count {
			break
		}
		selected = append(selected, lexicon.Selection{Keyword: query, NormalizedWord: strings.ToLower(query), Language: "und", Origin: "curated", RunID: runID, Scope: "kakao-book", Sources: []string{}, SelectedAt: selectedAt})
	}
	return selected
}

func discoveryRunID(now time.Time) string {
	for _, name := range []string{"LEXICON_RUN_ID"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	// Shared reader and selector must use the same identity to restore a
	// database-backed receipt across GitHub retry attempts.
	if value := lexicon.RunID(); value != "" {
		return value
	}
	return "kakao:" + now.UTC().Format("2006-01-02")
}

func discoveryComposition(selections []lexicon.Selection) (curated, dictionary int) {
	for _, selection := range selections {
		if selection.Origin == "curated" {
			curated++
		} else {
			dictionary++
		}
	}
	return
}

func discoverySource(source string, selection lexicon.Selection) string {
	if selection.Origin != "lexicon" && selection.Origin != "dictionary" {
		return source
	}
	return source + ":lexicon:" + selection.Language + ":" + selection.SnapshotID + ":" + selection.RunID
}

func discoveryByQuery(selections []lexicon.Selection) map[string]lexicon.Selection {
	byQuery := make(map[string]lexicon.Selection, len(selections))
	for _, selection := range selections {
		byQuery[strings.ToLower(strings.Join(strings.Fields(selection.Keyword), " "))] = selection
	}
	return byQuery
}

// Due checks and exhausted quotas may remove one side of a pair. Retain only
// matched pairs for mixed discovery instead of letting those removals turn the
// external attempt plan into a dictionary-only or curated-only run.
func balanceDiscoveryCandidates(candidates []quota.Candidate, batch discoveryBatch) []quota.Candidate {
	curated, dictionary := discoveryComposition(batch.Selections)
	if curated == 0 || dictionary == 0 || batch.Degraded {
		return candidates
	}
	byQuery := discoveryByQuery(batch.Selections)
	legacy, nouns := []quota.Candidate{}, []quota.Candidate{}
	for _, candidate := range candidates {
		selection := byQuery[strings.ToLower(strings.Join(strings.Fields(candidate.Request.Query), " "))]
		if selection.Origin == "curated" {
			legacy = append(legacy, candidate)
		} else {
			nouns = append(nouns, candidate)
		}
	}
	pairs := min(len(legacy), len(nouns))
	balanced := make([]quota.Candidate, 0, pairs*2)
	for index := 0; index < pairs; index++ {
		balanced = append(balanced, legacy[index], nouns[index])
	}
	return balanced
}

func balanceDiscoveryPlan(plan quota.Plan, batch discoveryBatch) quota.Plan {
	candidates := make([]quota.Candidate, 0, len(plan.Selected))
	byFingerprint := make(map[string]quota.PlannedRequest, len(plan.Selected))
	for _, selected := range plan.Selected {
		candidates = append(candidates, quota.Candidate{Request: selected.Request, EstimatedCalls: selected.EstimatedCalls, Priority: selected.Priority})
		byFingerprint[strings.ToLower(strings.Join(strings.Fields(selected.Request.Query), " "))] = selected
	}
	balanced := balanceDiscoveryCandidates(candidates, batch)
	plan.Selected = nil
	plan.PlannedCalls = 0
	for _, candidate := range balanced {
		selected := byFingerprint[strings.ToLower(strings.Join(strings.Fields(candidate.Request.Query), " "))]
		plan.Selected = append(plan.Selected, selected)
		plan.PlannedCalls += selected.EstimatedCalls
	}
	return plan
}
