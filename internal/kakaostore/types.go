package kakaostore

import (
	"context"
	"time"

	"statground_naver_book_go/internal/quota"
)

type CallLog struct {
	RequestUUID   string
	RunUUID       string
	Version       uint64
	RequestedAt   time.Time
	CompletedAt   *time.Time
	Mode          string
	QueryHash     string
	Target        string
	Sort          string
	Page          int
	Size          int
	HTTPStatus    int
	KakaoCode     int
	Success       bool
	Documents     int
	ElapsedMillis int64
	ErrorCategory string
	Status        string
	Source        string
}

type CollectLog struct {
	LogUUID          string
	RunUUID          string
	Version          uint64
	Provider         string
	Mode             string
	Query            string
	QueryHash        string
	Target           string
	Sort             string
	RequestedPageCap int
	PagesCalled      int
	TotalCount       int
	PageableCount    int
	FetchedCount     int
	InsertedCount    int
	NewISBNCount     int
	ChangedISBNCount int
	DuplicateCount   int
	Status           string
	ErrorCategory    string
	Source           string
	CollectedAt      time.Time
}

type FrontierKey struct {
	Provider string
	Mode     string
	Query    string
	Target   string
	Sort     string
}

type FrontierSnapshot struct {
	Found             bool
	State             quota.FrontierState
	NextDueAt         time.Time
	LastTotalCount    int
	LastPageableCount int
	PriorityScore     float64
}

type FrontierRecord struct {
	FrontierUUID       string
	RunUUID            string
	Version            uint64
	Key                FrontierKey
	QueryHash          string
	LastRunAt          time.Time
	LastSuccessAt      time.Time
	NextDueAt          time.Time
	LastTotalCount     int
	LastPageableCount  int
	CallsLastRun       int
	DocumentsLastRun   int
	NewISBNLastRun     int
	ChangedISBNLastRun int
	DuplicateRatio     float64
	YieldPerCall       float64
	ConsecutiveZero    int
	PriorityScore      float64
	Active             bool
	Source             string
}

type QuotaStop struct {
	Found     bool
	Category  string
	StoppedAt time.Time
}

type Store interface {
	Validate(context.Context) error
	ObservedCallsToday(context.Context, time.Time) (int, error)
	LatestQuotaStop(context.Context) (QuotaStop, error)
	LoadFrontier(context.Context, FrontierKey) (FrontierSnapshot, error)
	ExistingContentHashes(context.Context, []string) (map[string]string, error)
	InsertCallLog(context.Context, CallLog) error
	InsertRawRows(context.Context, []map[string]any) error
	InsertCollectLog(context.Context, CollectLog) error
	InsertFrontier(context.Context, FrontierRecord) error
}

// QuotaStopBlocked applies a conservative operational hold. The hold is not a
// claim about Kakao's undocumented quota reset timezone.
func QuotaStopBlocked(stop QuotaStop, now time.Time, quotaHold, rateLimitHold time.Duration) bool {
	if !stop.Found || stop.StoppedAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	switch stop.Category {
	case "quota_exhausted":
		if quotaHold <= 0 {
			quotaHold = 24 * time.Hour
		}
		return now.Before(stop.StoppedAt.Add(quotaHold))
	case "rate_limited":
		if rateLimitHold <= 0 {
			rateLimitHold = 30 * time.Minute
		}
		return now.Before(stop.StoppedAt.Add(rateLimitHold))
	default:
		return false
	}
}
