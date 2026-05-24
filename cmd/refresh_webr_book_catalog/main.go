package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/util"
)

var qualifiedIdentifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$`)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	settleSeconds := envx.Int("WEBR_BOOK_REFRESH_SETTLE_SECONDS", 20)
	if settleSeconds > 0 {
		fmt.Printf("[webr-book] waiting %ds for Kafka/ClickHouse ingestion to settle\n", settleSeconds)
		time.Sleep(time.Duration(settleSeconds) * time.Second)
	}

	client, err := ch.NewFromEnv()
	if err != nil {
		return err
	}
	timeoutSeconds := envx.Int("WEBR_BOOK_REFRESH_TIMEOUT_SECONDS", 300)
	if timeoutSeconds <= 0 {
		timeoutSeconds = 300
	}
	client.HTTPClient.Timeout = time.Duration(timeoutSeconds) * time.Second

	refreshView := envx.String("WEBR_BOOK_REFRESH_VIEW", "webr_book.mv_naver_r_book_catalog_refresh")
	countView := envx.String("WEBR_BOOK_COUNT_VIEW", "webr_book.v_naver_r_book_catalog")
	if err := validateQualifiedIdentifier(refreshView, "WEBR_BOOK_REFRESH_VIEW"); err != nil {
		return err
	}
	if err := validateQualifiedIdentifier(countView, "WEBR_BOOK_COUNT_VIEW"); err != nil {
		return err
	}

	fmt.Printf("[webr-book] refreshing view=%s\n", refreshView)
	if err := client.Exec("SYSTEM REFRESH VIEW " + refreshView); err != nil {
		return fmt.Errorf("refresh Web-R book catalog: %w", err)
	}

	rows, err := client.QueryJSONEachRow(fmt.Sprintf(`
        SELECT count() AS row_count,
               if(isNull(max(refresh_batch)), '', formatDateTime(max(refresh_batch), '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul')) AS latest_batch
          FROM %s
    `, countView))
	if err != nil {
		return fmt.Errorf("verify Web-R book catalog refresh: %w", err)
	}
	if len(rows) == 0 {
		fmt.Println("[webr-book] refresh ok row_count=0 latest_batch=")
		return nil
	}
	fmt.Printf("[webr-book] refresh ok row_count=%d latest_batch=%s\n", util.ToInt64(rows[0]["row_count"]), strings.TrimSpace(util.ToString(rows[0]["latest_batch"])))
	return nil
}

func validateQualifiedIdentifier(value, envName string) error {
	if !qualifiedIdentifierRe.MatchString(strings.TrimSpace(value)) {
		return fmt.Errorf("%s must be a qualified ClickHouse identifier like database.table", envName)
	}
	return nil
}
