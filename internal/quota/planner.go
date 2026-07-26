package quota

import (
	"fmt"
	"sort"
	"strings"

	"statground_naver_book_go/internal/provider"
)

type PriorityFactors struct {
	DownstreamRelevance float64
	RecentUserDemand    float64
	HistoricalYield     float64
	FreshnessNeed       float64
	SourceDiversity     float64
}

func (f PriorityFactors) Score() float64 {
	return clamp01(f.DownstreamRelevance)*0.30 +
		clamp01(f.RecentUserDemand)*0.25 +
		clamp01(f.HistoricalYield)*0.20 +
		clamp01(f.FreshnessNeed)*0.15 +
		clamp01(f.SourceDiversity)*0.10
}

type Candidate struct {
	Request        provider.SearchRequest
	EstimatedCalls int
	Priority       float64
}

type PlannedRequest struct {
	Request        provider.SearchRequest
	EstimatedCalls int
	Priority       float64
	Fingerprint    string
}

type Plan struct {
	Selected          []PlannedRequest
	SkippedDuplicate  int
	SkippedInvalid    int
	SkippedOverBudget int
	PlannedCalls      int
	RemainingCalls    int
}

// BuildPlan is deterministic and has no side effects beyond reserving the
// supplied in-memory budget. It never calls Kakao or a database.
func BuildPlan(candidates []Candidate, budget *Budget) Plan {
	plan := Plan{}
	if budget == nil {
		plan.SkippedOverBudget = len(candidates)
		return plan
	}

	unique := make(map[string]PlannedRequest, len(candidates))
	for _, candidate := range candidates {
		request, fingerprint, err := normalizeCandidate(candidate)
		if err != nil {
			plan.SkippedInvalid++
			continue
		}
		current, exists := unique[fingerprint]
		if exists {
			plan.SkippedDuplicate++
			if current.Priority > candidate.Priority ||
				(current.Priority == candidate.Priority && current.EstimatedCalls <= candidate.EstimatedCalls) {
				continue
			}
		}
		unique[fingerprint] = PlannedRequest{
			Request:        request,
			EstimatedCalls: candidate.EstimatedCalls,
			Priority:       candidate.Priority,
			Fingerprint:    fingerprint,
		}
	}

	ordered := make([]PlannedRequest, 0, len(unique))
	for _, candidate := range unique {
		ordered = append(ordered, candidate)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Priority != ordered[j].Priority {
			return ordered[i].Priority > ordered[j].Priority
		}
		if ordered[i].EstimatedCalls != ordered[j].EstimatedCalls {
			return ordered[i].EstimatedCalls < ordered[j].EstimatedCalls
		}
		return ordered[i].Fingerprint < ordered[j].Fingerprint
	})

	for _, candidate := range ordered {
		if !budget.Reserve(candidate.EstimatedCalls) {
			plan.SkippedOverBudget++
			continue
		}
		plan.Selected = append(plan.Selected, candidate)
		plan.PlannedCalls += candidate.EstimatedCalls
	}
	plan.RemainingCalls = budget.RemainingRunCalls()
	return plan
}

func normalizeCandidate(candidate Candidate) (provider.SearchRequest, string, error) {
	request := candidate.Request
	request.Query = strings.Join(strings.Fields(request.Query), " ")
	request.Sort = strings.ToLower(strings.TrimSpace(request.Sort))
	request.Target = strings.ToLower(strings.TrimSpace(request.Target))
	if request.Sort == "" {
		request.Sort = "accuracy"
	}
	if request.Page == 0 {
		request.Page = 1
	}
	if request.Size == 0 {
		request.Size = 10
	}
	if candidate.EstimatedCalls <= 0 {
		return provider.SearchRequest{}, "", fmt.Errorf("estimated calls must be positive")
	}
	if err := provider.ValidateSearchRequest(request); err != nil {
		return provider.SearchRequest{}, "", err
	}
	fingerprint := strings.Join([]string{
		strings.ToLower(request.Query),
		request.Sort,
		request.Target,
		fmt.Sprintf("%d", request.Page),
		fmt.Sprintf("%d", request.Size),
	}, "\x1f")
	return request, fingerprint, nil
}

func clamp01(value float64) float64 {
	switch {
	case value < 0:
		return 0
	case value > 1:
		return 1
	default:
		return value
	}
}
