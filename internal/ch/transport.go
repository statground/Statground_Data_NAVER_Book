package ch

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

// ValidateTransportContext keeps HTTPS as the default. A deliberately enabled
// local runner may use HTTP only when every destination address is private or
// loopback. postContext independently rejects redirects for every request.
func (c *Client) ValidateTransportContext(ctx context.Context, requireHTTPS bool) error {
	host := strings.TrimSpace(c.Host)
	protocol := strings.ToLower(strings.TrimSpace(c.Protocol))
	if strings.Contains(host, "://") {
		u, err := url.Parse(host)
		if err != nil || u.User != nil || u.Hostname() == "" {
			return fmt.Errorf("invalid ClickHouse transport")
		}
		protocol, host = strings.ToLower(u.Scheme), u.Hostname()
	} else if name, _, err := net.SplitHostPort(host); err == nil {
		host = name
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return fmt.Errorf("invalid ClickHouse transport")
	}
	if protocol == "https" {
		return nil
	}
	if requireHTTPS || protocol != "http" {
		return fmt.Errorf("ClickHouse HTTPS required")
	}
	addresses := []net.IPAddr{}
	if ip := net.ParseIP(host); ip != nil {
		addresses = append(addresses, net.IPAddr{IP: ip})
	} else {
		var err error
		lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		addresses, err = net.DefaultResolver.LookupIPAddr(lookupCtx, host)
		if err != nil {
			return fmt.Errorf("ClickHouse private address resolution failed")
		}
	}
	if !allPrivateAddresses(addresses) {
		return fmt.Errorf("ClickHouse HTTP requires private addresses")
	}
	return nil
}

func allPrivateAddresses(addresses []net.IPAddr) bool {
	if len(addresses) == 0 {
		return false
	}
	for _, address := range addresses {
		if !address.IP.IsPrivate() && !address.IP.IsLoopback() {
			return false
		}
	}
	return true
}
