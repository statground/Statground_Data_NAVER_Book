package main

import (
	"errors"
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
var errBookRefreshBusy = errors.New("another book refresh is already running")

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	settleSeconds := envx.Int("WEBR_BOOK_REFRESH_SETTLE_SECONDS", 20)
	if settleSeconds > 0 {
		fmt.Printf("[book-catalog] waiting %ds for direct ClickHouse ingestion to settle\n", settleSeconds)
		time.Sleep(time.Duration(settleSeconds) * time.Second)
	}

	client, err := ch.NewFromEnv()
	if err != nil {
		return err
	}
	timeoutSeconds := envx.Int("WEBR_BOOK_REFRESH_TIMEOUT_SECONDS", 900)
	if timeoutSeconds <= 0 {
		timeoutSeconds = 900
	}
	client.HTTPClient.Timeout = time.Duration(timeoutSeconds) * time.Second

	if err := refreshProviderCatalog(
		client,
		envx.String("BOOK_PROVIDER_REFRESH_VIEW", "Data_Book_Service.mv_book_catalog_latest_refresh"),
		envx.String("BOOK_PROVIDER_COUNT_VIEW", "Data_Book_Service.v_book_catalog_latest_current"),
	); err != nil {
		if errors.Is(err, errBookRefreshBusy) {
			fmt.Println("[book-catalog] refresh skipped because the serial Book refresh chain is already running")
			return nil
		}
		return err
	}
	if err := refreshCatalog(client, "webr-book", envx.String("WEBR_BOOK_REFRESH_VIEW", "webr_book.mv_naver_r_book_catalog_refresh"), envx.String("WEBR_BOOK_COUNT_VIEW", "webr_book.v_naver_r_book_catalog")); err != nil {
		return err
	}
	if mirtypeRefreshEnabled() {
		if err := refreshCatalog(client, "mirtype-book", envx.String("MIRTYPE_BOOK_REFRESH_VIEW", "mirtype_book.mv_naver_language_book_catalog_refresh"), envx.String("MIRTYPE_BOOK_COUNT_VIEW", "mirtype_book.v_naver_language_book_catalog")); err != nil {
			return err
		}
	}
	return nil
}

func refreshProviderCatalog(client *ch.Client, refreshView, countView string) error {
	if err := validateQualifiedIdentifier(refreshView, "BOOK_PROVIDER_REFRESH_VIEW"); err != nil {
		return err
	}
	if err := validateQualifiedIdentifier(countView, "BOOK_PROVIDER_COUNT_VIEW"); err != nil {
		return err
	}
	fmt.Printf("[book-provider] refreshing view=%s\n", refreshView)
	if err := triggerRefreshOnCoordinator(client, refreshView); err != nil {
		return fmt.Errorf("refresh provider-neutral book catalog: %w", err)
	}
	rows, err := client.QueryJSONEachRow(fmt.Sprintf(`
        SELECT
            count() AS row_count,
            countIf(provider = 'kakao') AS kakao_count,
            countIf(provider = 'nlk_lod') AS nlk_count,
            countIf(provider = 'naver_frozen') AS frozen_count,
            if(isNull(max(updated_at)), '', formatDateTime(max(updated_at), '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul')) AS latest_update
        FROM %s
    `, countView))
	if err != nil {
		return fmt.Errorf("verify provider-neutral book catalog refresh: %w", err)
	}
	if len(rows) == 0 {
		fmt.Println("[book-provider] refresh ok row_count=0 kakao_count=0 nlk_count=0 frozen_count=0 latest_update=")
		return nil
	}
	fmt.Printf(
		"[book-provider] refresh ok row_count=%d kakao_count=%d nlk_count=%d frozen_count=%d latest_update=%s\n",
		util.ToInt64(rows[0]["row_count"]),
		util.ToInt64(rows[0]["kakao_count"]),
		util.ToInt64(rows[0]["nlk_count"]),
		util.ToInt64(rows[0]["frozen_count"]),
		strings.TrimSpace(util.ToString(rows[0]["latest_update"])),
	)
	return nil
}

func refreshCatalog(client *ch.Client, label, refreshView, countView string) error {
	if err := validateQualifiedIdentifier(refreshView, strings.ToUpper(strings.ReplaceAll(label, "-", "_"))+"_REFRESH_VIEW"); err != nil {
		return err
	}
	if err := validateQualifiedIdentifier(countView, strings.ToUpper(strings.ReplaceAll(label, "-", "_"))+"_COUNT_VIEW"); err != nil {
		return err
	}
	fmt.Printf("[%s] refreshing view=%s\n", label, refreshView)
	if err := triggerRefreshOnCoordinator(client, refreshView); err != nil {
		return fmt.Errorf("refresh %s catalog: %w", label, err)
	}

	rows, err := client.QueryJSONEachRow(fmt.Sprintf(`
        SELECT count() AS row_count,
               if(isNull(max(refresh_batch)), '', formatDateTime(max(refresh_batch), '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul')) AS latest_batch
          FROM %s
    `, countView))
	if err != nil {
		return fmt.Errorf("verify %s catalog refresh: %w", label, err)
	}
	if len(rows) == 0 {
		fmt.Printf("[%s] refresh ok row_count=0 latest_batch=\n", label)
		return nil
	}
	fmt.Printf("[%s] refresh ok row_count=%d latest_batch=%s\n", label, util.ToInt64(rows[0]["row_count"]), strings.TrimSpace(util.ToString(rows[0]["latest_batch"])))
	return nil
}

func triggerRefreshOnCoordinator(client *ch.Client, refreshView string) error {
	attempts := envx.Int("BOOK_REFRESH_COORDINATOR_ATTEMPTS", 12)
	if attempts < 1 {
		attempts = 12
	}
	retryMilliseconds := envx.Int("BOOK_REFRESH_COORDINATOR_RETRY_MILLISECONDS", 500)
	if retryMilliseconds < 0 {
		retryMilliseconds = 500
	}
	return triggerRefreshWithRetry(client, refreshView, attempts, time.Duration(retryMilliseconds)*time.Millisecond)
}

// Refreshable materialized views intentionally live on one coordinator so a
// scheduled full refresh is not duplicated by every replica. The public
// ClickHouse endpoint can select any replica, so locate the coordinator through
// bounded connection retries before issuing SYSTEM REFRESH VIEW.
func triggerRefreshWithRetry(client *ch.Client, refreshView string, attempts int, retryDelay time.Duration) error {
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		// Resolve only the requested coordinator. A system.tables scan can wait
		// on unrelated failed metadata while ClickHouse is recovering, even
		// though this exact refreshable view is already usable.
		exists, err := client.TableExists(refreshView)
		if err != nil {
			lastErr = err
		} else if exists {
			busy, err := anyBookRefreshRunning(client)
			if err != nil {
				lastErr = err
			} else if busy {
				return errBookRefreshBusy
			} else if err := client.Exec("SYSTEM REFRESH VIEW " + refreshView); err == nil {
				if err := client.Exec("SYSTEM WAIT VIEW " + refreshView); err == nil {
					return nil
				} else {
					lastErr = err
				}
			} else {
				lastErr = err
			}
		} else {
			lastErr = fmt.Errorf("refresh coordinator unavailable")
		}
		if client.HTTPClient != nil {
			client.HTTPClient.CloseIdleConnections()
		}
		if attempt < attempts && retryDelay > 0 {
			time.Sleep(retryDelay)
		}
	}
	return lastErr
}

func anyBookRefreshRunning(client *ch.Client) (bool, error) {
	value, err := client.QueryScalarValue(`
		SELECT count() > 0 AS value
		FROM system.view_refreshes
		WHERE status IN ('Running', 'RunningOnAnotherReplica')
		  AND (
		        (database = 'Data_Book_Service' AND view IN (
		            'mv_book_catalog_latest_refresh',
		            'mv_book_bibliography_discovery_refresh'
		        ))
		     OR (database = 'webr_book' AND view = 'mv_naver_r_book_catalog_refresh')
		     OR (database = 'mirtype_book' AND view = 'mv_naver_language_book_catalog_refresh')
		  )
	`)
	if err != nil {
		return false, err
	}
	return util.ToInt64(value) > 0, nil
}

func mirtypeRefreshEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(envx.String("MIRTYPE_BOOK_REFRESH_ENABLED", "")))
	return value == "1" || value == "true" || value == "yes" || strings.TrimSpace(envx.String("MIRTYPE_BOOK_REFRESH_VIEW", "")) != ""
}

func validateQualifiedIdentifier(value, envName string) error {
	if !qualifiedIdentifierRe.MatchString(strings.TrimSpace(value)) {
		return fmt.Errorf("%s must be a qualified ClickHouse identifier like database.table", envName)
	}
	return nil
}
