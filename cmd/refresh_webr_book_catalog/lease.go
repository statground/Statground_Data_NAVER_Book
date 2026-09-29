package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const bookMVLeaseResource = "statground-book-refresh-v1"

var bookMVLeaseInvoke = func(ctx context.Context, path string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, path, args...)
	return command.Output()
}

type bookMVLeaseReply struct {
	Resource      string   `json:"resource"`
	Holder        string   `json:"holder"`
	Fence         string   `json:"fence"`
	ExpiresEpoch  int64    `json:"expires_epoch"`
	FencedWriters []string `json:"fenced_writers"`
	Released      bool     `json:"released"`
}

type bookMVLease struct {
	helper, holder, fence string
	mu                    sync.Mutex
	lost                  error
	stop, done            chan struct{}
}

type bookMVWriterGuard interface{ Check() error }

func checkBookMVWriterLease(guard bookMVWriterGuard) error {
	if guard == nil {
		return errors.New("Book MV writer lease guard missing")
	}
	return guard.Check()
}

func trustedBookMVHelper(path string) error {
	if path == "" || !filepath.IsAbs(path) {
		return errors.New("BOOK_MV_LEASE_HELPER must be an absolute path")
	}
	link, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("Book MV lease helper stat: %w", err)
	}
	if link.Mode()&os.ModeSymlink != 0 && link.Sys().(*syscall.Stat_t).Uid != 0 {
		return errors.New("Book MV lease helper link is not root-owned")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("Book MV lease helper target stat: %w", err)
	}
	owner := info.Sys().(*syscall.Stat_t).Uid
	if !info.Mode().IsRegular() || owner != 0 && owner != uint32(os.Getuid()) ||
		info.Mode().Perm()&0022 != 0 || info.Mode().Perm()&0100 == 0 {
		return errors.New("Book MV lease helper is not trusted and executable")
	}
	return nil
}

func acquireBookMVLease() (*bookMVLease, error) {
	helper := os.Getenv("BOOK_MV_LEASE_HELPER")
	if err := trustedBookMVHelper(helper); err != nil {
		return nil, err
	}
	if os.Getenv("BOOK_MV_LEASE_CONFIG") == "" {
		return nil, errors.New("BOOK_MV_LEASE_CONFIG is required")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("Book MV lease holder generation: %w", err)
	}
	lease := &bookMVLease{helper: helper, holder: hex.EncodeToString(id[:]),
		stop: make(chan struct{}), done: make(chan struct{})}
	if _, err := lease.call("acquire"); err != nil {
		return nil, err
	}
	go lease.heartbeat()
	return lease, nil
}

func (lease *bookMVLease) call(phase string) (bookMVLeaseReply, error) {
	args := []string{phase, "--resource", bookMVLeaseResource, "--holder", lease.holder}
	if lease.fence != "" {
		args = append(args, "--fence", lease.fence)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	output, err := bookMVLeaseInvoke(ctx, lease.helper, args...)
	if err != nil {
		return bookMVLeaseReply{}, fmt.Errorf("Book MV lease %s failed", phase)
	}
	var reply bookMVLeaseReply
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		return bookMVLeaseReply{}, fmt.Errorf("Book MV lease %s reply invalid", phase)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return bookMVLeaseReply{}, fmt.Errorf("Book MV lease %s reply has trailing data", phase)
	}
	if reply.Resource != bookMVLeaseResource || reply.Holder != lease.holder {
		return bookMVLeaseReply{}, errors.New("Book MV lease identity changed")
	}
	if _, err := strconv.ParseUint(reply.Fence, 10, 64); err != nil || reply.Fence == "0" ||
		lease.fence != "" && reply.Fence != lease.fence {
		return bookMVLeaseReply{}, errors.New("Book MV lease fence changed")
	}
	if phase == "release" {
		if !reply.Released {
			return bookMVLeaseReply{}, errors.New("Book MV lease release not acknowledged")
		}
	} else {
		participants := make(map[string]bool, len(reply.FencedWriters))
		for _, writer := range reply.FencedWriters {
			participants[writer] = true
		}
		if reply.ExpiresEpoch <= time.Now().Unix()+60 || len(reply.FencedWriters) != 2 ||
			len(participants) != 2 || !participants["manual-refresh"] || !participants["mv-maintenance"] {
			return bookMVLeaseReply{}, errors.New("Book MV lease coverage or TTL invalid")
		}
	}
	if lease.fence == "" {
		lease.fence = reply.Fence
	}
	return reply, nil
}

func (lease *bookMVLease) heartbeat() {
	defer close(lease.done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-lease.stop:
			return
		case <-ticker.C:
			if _, err := lease.call("renew"); err != nil {
				lease.mu.Lock()
				lease.lost = err
				lease.mu.Unlock()
				return
			}
		}
	}
}

func (lease *bookMVLease) Check() error {
	if lease == nil {
		return errors.New("Book MV writer lease guard missing")
	}
	lease.mu.Lock()
	err := lease.lost
	lease.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = lease.call("assert")
	return err
}

func (lease *bookMVLease) Close() error {
	close(lease.stop)
	<-lease.done
	_, err := lease.call("release")
	return err
}
