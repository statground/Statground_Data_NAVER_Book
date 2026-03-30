package main

import (
	"fmt"
	"os"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/collector"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/naver"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	keyword, err := envx.Require("MANUAL_KEYWORD")
	if err != nil {
		return err
	}
	client, err := ch.NewFromEnv()
	if err != nil {
		return err
	}
	keys, err := naver.LoadAPIKeysFromEnv()
	if err != nil {
		return err
	}
	c, err := collector.New(client, "raw_naver", keys, time.Now().UnixNano())
	if err != nil {
		return err
	}
	return c.CollectManual(keyword)
}
