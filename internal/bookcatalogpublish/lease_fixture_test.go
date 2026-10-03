package bookcatalogpublish

import (
	"context"
	"statground_naver_book_go/internal/writerlease"
)

// fixtureAdmission replaces the external control plane in transport fixtures.
// Tests of real admission and failure behavior live in writerlease and ch.
type fixtureAdmission struct{}

func (fixtureAdmission) Assert(context.Context) error { return nil }
func (fixtureAdmission) Begin(ctx context.Context, _ writerlease.Intent) (context.Context, writerlease.Operation, error) {
	return ctx, fixtureOperation{}, nil
}
func (fixtureAdmission) Close() error { return nil }

type fixtureOperation struct{}

func (fixtureOperation) Confirm(context.Context) error { return nil }
func (fixtureOperation) Abandon()                      {}
