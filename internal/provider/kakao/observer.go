package kakao

import (
	"context"
	"time"

	"statground_naver_book_go/internal/provider"
)

// AttemptInfo describes one external HTTP attempt. Retries are separate
// attempts because each can consume Kakao app quota.
type AttemptInfo struct {
	Request   provider.SearchRequest
	StartedAt time.Time
}

type AttemptResult struct {
	CompletedAt    time.Time
	HTTPStatus     int
	KakaoCode      int
	Success        bool
	DocumentsCount int
	Elapsed        time.Duration
	ErrorCategory  string
}

// AttemptObserver persists quota evidence immediately before and after each
// external HTTP attempt. BeforeAttempt returning an error prevents the call.
type AttemptObserver interface {
	BeforeAttempt(context.Context, AttemptInfo) (string, error)
	AfterAttempt(context.Context, string, AttemptResult) error
}

type attemptObserverContextKey struct{}

func WithAttemptObserver(ctx context.Context, observer AttemptObserver) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, attemptObserverContextKey{}, observer)
}

func attemptObserverFromContext(ctx context.Context) AttemptObserver {
	if ctx == nil {
		return nil
	}
	observer, _ := ctx.Value(attemptObserverContextKey{}).(AttemptObserver)
	return observer
}
