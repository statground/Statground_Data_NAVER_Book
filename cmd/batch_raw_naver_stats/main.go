package main

import (
	"fmt"
	"os"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/stats"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	client, err := ch.NewFromEnv()
	if err != nil {
		return err
	}
	return stats.Generate(client, "raw_naver", "stats")
}
