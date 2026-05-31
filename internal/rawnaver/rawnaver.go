package rawnaver

import (
	"fmt"
	"sort"
	"strings"

	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/naver"
	"statground_naver_book_go/internal/util"
)

type ExistingInfo struct {
	UUID       string
	CreatedAt  string
	CreatedLog string
}

func FilterRowByColumns(row map[string]any, columns map[string]bool) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		if columns[k] {
			out[k] = v
		}
	}
	return out
}

func SampleTitleAuthorPublisher(client *ch.Client, table string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 1000
	}
	sql := fmt.Sprintf(`
        WITH toUInt64(intDiv(toUInt32(now('Asia/Seoul')), 3600)) AS sample_salt
        SELECT title, description, author, publisher
        FROM %s
        WHERE notEmpty(title)
          AND cityHash64(title, description, author, publisher, sample_salt) %% 997 = 0
        ORDER BY cityHash64(title, description, author, publisher, sample_salt)
        LIMIT %d
        SETTINGS max_execution_time = 20, timeout_overflow_mode = 'break'
    `, table, limit)
	return client.QueryJSONEachRow(sql)
}

func BuildExistingMap(client *ch.Client, table string, columns map[string]bool, isbns []string) (map[string]ExistingInfo, error) {
	cleaned := make([]string, 0, len(isbns))
	seen := map[string]struct{}{}
	for _, isbn := range isbns {
		isbn = strings.TrimSpace(isbn)
		if isbn == "" {
			continue
		}
		if _, ok := seen[isbn]; ok {
			continue
		}
		seen[isbn] = struct{}{}
		cleaned = append(cleaned, isbn)
	}
	if len(cleaned) == 0 {
		return map[string]ExistingInfo{}, nil
	}
	sort.Strings(cleaned)
	orderExpr := "created_at DESC"
	if columns["version"] {
		orderExpr = "version DESC"
	}
	sql := fmt.Sprintf(`
        SELECT
            isbn,
            uuid,
            formatDateTime(created_at, '%%Y-%%m-%%d %%H:%%i:%%S.%%f', 'Asia/Seoul') AS created_at,
            created_log
        FROM %s
        WHERE isbn IN (%s)
        ORDER BY %s
        LIMIT 1 BY isbn
    `, table, util.QuoteStringList(cleaned), orderExpr)
	rows, err := client.QueryJSONEachRow(sql)
	if err != nil {
		return nil, err
	}
	out := make(map[string]ExistingInfo, len(rows))
	for _, row := range rows {
		isbn := util.ToString(row["isbn"])
		if isbn == "" {
			continue
		}
		out[isbn] = ExistingInfo{
			UUID:       util.ToString(row["uuid"]),
			CreatedAt:  util.ToString(row["created_at"]),
			CreatedLog: util.ToString(row["created_log"]),
		}
	}
	return out, nil
}

func BuildUUIDMap(client *ch.Client, table string, isbns []string) (map[string]string, error) {
	cleaned := make([]string, 0, len(isbns))
	seen := map[string]struct{}{}
	for _, isbn := range isbns {
		isbn = strings.TrimSpace(isbn)
		if isbn == "" {
			continue
		}
		if _, ok := seen[isbn]; ok {
			continue
		}
		seen[isbn] = struct{}{}
		cleaned = append(cleaned, isbn)
	}
	if len(cleaned) == 0 {
		return map[string]string{}, nil
	}
	sql := fmt.Sprintf(`
        SELECT isbn, uuid
        FROM %s
        WHERE isbn IN (%s)
        LIMIT 100000
    `, table, util.QuoteStringList(cleaned))
	rows, err := client.QueryJSONEachRow(sql)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, row := range rows {
		isbn := util.ToString(row["isbn"])
		uuid := util.ToString(row["uuid"])
		if isbn != "" && uuid != "" {
			out[isbn] = uuid
		}
	}
	return out, nil
}

func BuildRawNaverRow(item naver.BookItem, nowStr string, version int64, createdAt string, createdLog, updatedLog, uuid string) map[string]any {
	var discount any
	if strings.TrimSpace(item.Discount) != "" {
		discount = util.ToInt64(item.Discount)
	} else {
		discount = nil
	}
	return map[string]any{
		"uuid":        uuid,
		"version":     version,
		"created_at":  createdAt,
		"created_log": createdLog,
		"updated_at":  nowStr,
		"updated_log": updatedLog,
		"title":       item.Title,
		"link":        item.Link,
		"image":       item.Image,
		"author":      item.Author,
		"discount":    discount,
		"publisher":   item.Publisher,
		"isbn":        item.ISBN,
		"description": item.Description,
		"pubdate":     item.Pubdate,
	}
}
