package aladin

import (
	"strings"
	"testing"
)

func TestAladinHTTPStatusErrorCompactsHTML(t *testing.T) {
	err := aladinHTTPStatusError(503, []byte(`<!DOCTYPE HTML><HTML><HEAD><TITLE>Service Unavailable</TITLE></HEAD><BODY><h2>Service Unavailable</h2><p>HTTP Error 503.</p></BODY></HTML>`))
	msg := err.Error()
	if !strings.Contains(msg, "aladin http 503") {
		t.Fatalf("expected status prefix, got %q", msg)
	}
	if strings.Contains(msg, "<HTML>") || strings.Contains(msg, "<BODY>") {
		t.Fatalf("expected compact text error, got %q", msg)
	}
	if !strings.Contains(msg, "Service Unavailable") {
		t.Fatalf("expected status text, got %q", msg)
	}
}
