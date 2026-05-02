package main

import (
	"context"
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
	client, err := ch.NewOptionalFromEnv()
	if err != nil {
		return err
	}
	keys, err := naver.LoadAPIKeysFromEnv()
	if err != nil {
		return err
	}
	rawNaverTable := envx.String("RAW_NAVER_TABLE", "naver_book_raw")
	c, err := collector.New(client, rawNaverTable, keys, time.Now().UnixNano())
	if err != nil {
		return err
	}
	if err := c.ValidateIngest(context.Background()); err != nil {
		return err
	}
	return c.CollectManual(keyword)
}
