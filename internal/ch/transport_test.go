package ch

import (
	"context"
	"net"
	"testing"
)

func TestTransportRequiresHTTPSUnlessPrivateHTTPExplicitlyAllowed(t *testing.T) {
	for _, test := range []struct {
		name, host, protocol  string
		requireHTTPS, allowed bool
	}{
		{"default_tls", "database.invalid", "https", true, true},
		{"private_default_denied", "10.0.0.8", "http", true, false},
		{"private_explicit", "10.0.0.8", "http", false, true},
		{"loopback_explicit", "127.0.0.1", "http", false, true},
		{"loopback_ipv6", "[::1]:8123", "http", false, true},
		{"dns_loopback", "localhost", "http", false, true},
		{"public_http_denied", "8.8.8.8", "http", false, false},
		{"public_url_denied", "http://8.8.8.8:8123", "https", false, false},
		{"unspecified_denied", "0.0.0.0", "http", false, false},
		{"userinfo_denied", "https://name:secret@database.invalid", "https", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := (&Client{Host: test.host, Protocol: test.protocol}).ValidateTransportContext(context.Background(), test.requireHTTPS)
			if (err == nil) != test.allowed {
				t.Fatalf("allowed=%t err=%v", test.allowed, err)
			}
		})
	}
}

func TestPrivateResolutionRejectsAnyPublicOrMissingAddress(t *testing.T) {
	for _, addresses := range [][]net.IPAddr{
		nil,
		{{IP: net.ParseIP("10.0.0.8")}, {IP: net.ParseIP("8.8.8.8")}},
		{{IP: nil}},
	} {
		if allPrivateAddresses(addresses) {
			t.Fatal("incomplete or mixed private/public resolution accepted")
		}
	}
	if !allPrivateAddresses([]net.IPAddr{{IP: net.ParseIP("10.0.0.8")}, {IP: net.ParseIP("fd00::8")}}) {
		t.Fatal("all-private resolution rejected")
	}
}
