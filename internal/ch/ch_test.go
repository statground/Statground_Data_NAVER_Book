package ch

import "testing"

func TestClientBaseURLUsesProtocolAndPath(t *testing.T) {
	c := &Client{
		Host:     "clickhouse.example.com",
		Port:     9440,
		Protocol: "https",
		HTTPPath: "/clickhouse",
	}

	got := c.baseURL()
	want := "https://clickhouse.example.com:9440/clickhouse/"
	if got != want {
		t.Fatalf("baseURL() = %q, want %q", got, want)
	}
}

func TestClientBaseURLAcceptsFullURLHost(t *testing.T) {
	c := &Client{
		Host: "https://clickhouse.example.com/proxy",
		Port: 8123,
	}

	got := c.baseURL()
	want := "https://clickhouse.example.com/proxy/"
	if got != want {
		t.Fatalf("baseURL() = %q, want %q", got, want)
	}
}

func TestNewFromEnvAcceptsClickHousePrefixedNames(t *testing.T) {
	t.Setenv("CLICKHOUSE_HOST", "clickhouse.example.com")
	t.Setenv("CLICKHOUSE_PORT", "9440")
	t.Setenv("CLICKHOUSE_USER", "book_user")
	t.Setenv("CLICKHOUSE_PASSWORD", "secret")
	t.Setenv("CLICKHOUSE_DATABASE", "book_db")
	t.Setenv("CLICKHOUSE_PROTOCOL", "https")
	t.Setenv("CLICKHOUSE_HTTP_URL_PATH", "clickhouse")

	c, err := NewFromEnv()
	if err != nil {
		t.Fatalf("NewFromEnv() error = %v", err)
	}
	if c.Host != "clickhouse.example.com" || c.Port != 9440 || c.User != "book_user" || c.Database != "book_db" {
		t.Fatalf("unexpected client from CLICKHOUSE_* env: %+v", c)
	}

	got := c.baseURL()
	want := "https://clickhouse.example.com:9440/clickhouse/"
	if got != want {
		t.Fatalf("baseURL() = %q, want %q", got, want)
	}
}
