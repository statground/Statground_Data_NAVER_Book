package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/util"
)

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run() (int, error) {
	if envx.Int("OPTIMIZE_ENABLED", 1) != 1 {
		fmt.Println("[OPTIMIZE] skipped (OPTIMIZE_ENABLED!=1)")
		return 0, nil
	}

	host := envx.String("CH_HOST", envx.String("CLICKHOUSE_HOST", ""))
	if host == "" {
		fmt.Println("[OPTIMIZE] missing CH_HOST (or CLICKHOUSE_HOST). skip.")
		return 0, nil
	}
	port := envx.Int("CH_PORT", envx.Int("CLICKHOUSE_PORT", 8123))
	user := envx.String("CH_USER", envx.String("CLICKHOUSE_USER", "default"))
	password := envx.String("CH_PASSWORD", envx.String("CLICKHOUSE_PASSWORD", ""))
	database := envx.String("CH_DATABASE", envx.String("CLICKHOUSE_DATABASE", ""))

	client := ch.New(host, port, user, password, database)
	table := envx.String("OPTIMIZE_TABLE", "")
	if table == "" {
		if database != "" {
			table = database + ".raw_naver"
		} else {
			table = "raw_naver"
		}
	}
	recentN := envx.Int("OPTIMIZE_RECENT_PARTITIONS", 6)
	doFinal := envx.Int("OPTIMIZE_FINAL", 1) == 1

	rows, err := client.QueryJSONEachRow(fmt.Sprintf(`
        SELECT toString(toYYYYMM(created_at)) AS p
        FROM %s
        WHERE created_at >= now() - INTERVAL 365 DAY
        GROUP BY p
        ORDER BY p DESC
        LIMIT %d
    `, table, recentN))
	if err != nil {
		return 0, err
	}
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		p := strings.TrimSpace(util.ToString(row["p"]))
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		fmt.Printf("[OPTIMIZE] no partitions found for table=%s. skip.\n", table)
		return 0, nil
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	part := parts[r.Intn(len(parts))]
	stmt := fmt.Sprintf("OPTIMIZE TABLE %s PARTITION %s", table, part)
	if doFinal {
		stmt += " FINAL"
	}

	fmt.Printf("[OPTIMIZE] partitions(recent=%d)=%v -> chosen=%s\n", recentN, parts, part)
	fmt.Printf("[OPTIMIZE] running: %s\n", stmt)
	if err := client.Exec(stmt); err != nil {
		return 0, err
	}
	fmt.Println("[OPTIMIZE] done.")
	return 0, nil
}
