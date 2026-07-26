package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/provider"
	"statground_naver_book_go/internal/provider/kakao"
)

func main() {
	client, err := kakao.NewClientFromEnv()
	if err != nil {
		fail("configuration")
	}
	timeout := time.Duration(envx.Int("KAKAO_SMOKE_TIMEOUT_SECONDS", 30)) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	response, err := client.Search(ctx, provider.SearchRequest{
		Query: envx.String("KAKAO_SMOKE_QUERY", "통계학"),
		Sort:  "accuracy",
		Page:  1,
		Size:  1,
	})
	if err != nil {
		fail(kakao.ErrorCategory(err))
	}
	firstISBNPresent := false
	firstTitlePresent := false
	if len(response.Documents) > 0 {
		firstISBNPresent = response.Documents[0].ISBNRaw != ""
		firstTitlePresent = response.Documents[0].Title != ""
	}

	fmt.Println("provider=kakao")
	fmt.Println("http_status=200")
	fmt.Printf("total_count=%d\n", response.TotalCount)
	fmt.Printf("pageable_count=%d\n", response.PageableCount)
	fmt.Printf("documents=%d\n", len(response.Documents))
	fmt.Printf("first_isbn_present=%t\n", firstISBNPresent)
	fmt.Printf("first_title_present=%t\n", firstTitlePresent)
}

func fail(category string) {
	if category == "" {
		category = "unknown"
	}
	fmt.Fprintf(os.Stderr, "kakao book smoke failed category=%s\n", category)
	os.Exit(1)
}
