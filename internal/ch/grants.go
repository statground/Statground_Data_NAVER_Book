package ch

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// CheckTableGrantContext checks one read/write privilege without executing it.
// CHECK GRANT does not accept a SQL FORMAT clause and returns HTTP 200 for both
// allowed and denied privileges. Require its actual scalar result.
func (c *Client) CheckTableGrantContext(ctx context.Context, privilege, table string) (bool, error) {
	if privilege != "SELECT" && privilege != "INSERT" {
		return false, fmt.Errorf("unsupported ClickHouse table privilege")
	}
	qualified, err := QualifiedTableIdentifier(table, c.Database)
	if err != nil {
		return false, err
	}
	payload, err := c.postContext(ctx, "CHECK GRANT "+privilege+" ON "+qualified, url.Values{"default_format": {"TabSeparated"}})
	if err != nil {
		return false, err
	}
	switch strings.TrimSpace(string(payload)) {
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return false, fmt.Errorf("invalid ClickHouse grant result")
	}
}
