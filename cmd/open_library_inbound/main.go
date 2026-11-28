package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/bookmodel"
)

const (
	latestEditions = "https://openlibrary.org/data/ol_dump_editions_latest.txt.gz"
	maxArchive     = 20 << 30
	probeBytes     = 1 << 20
	probeRows      = 100
	maxCandidates  = 1000
	maxMatches     = 10000
)

var archiveName = regexp.MustCompile(`^ol_dump_editions_(\d{4}-\d{2}-\d{2})\.txt\.gz$`)
var editionKey = regexp.MustCompile(`^/books/OL[0-9]+M$`)

type edition struct {
	Key      string   `json:"key"`
	Revision int      `json:"revision"`
	ISBN10   []string `json:"isbn_10"`
	ISBN13   []string `json:"isbn_13"`
}

type matchedEdition struct {
	Provider       string          `json:"provider"`
	SourceSnapshot string          `json:"source_snapshot"`
	EditionKey     string          `json:"edition_key"`
	Revision       int             `json:"revision"`
	LastModified   string          `json:"last_modified"`
	MatchedISBN13  []string        `json:"matched_isbn_13"`
	SourceSHA256   string          `json:"source_row_sha256"`
	SourceRecord   json.RawMessage `json:"source_record"`
}

type receipt struct {
	Schema         string   `json:"schema"`
	Provider       string   `json:"provider"`
	Status         string   `json:"status"`
	SnapshotDate   string   `json:"snapshot_date"`
	SourceURL      string   `json:"source_url"`
	ArchiveBytes   int64    `json:"archive_bytes"`
	ArchiveSHA256  string   `json:"archive_sha256,omitempty"`
	EditionsSHA256 string   `json:"editions_jsonl_sha256,omitempty"`
	RowsScanned    int      `json:"rows_scanned"`
	Editions       int      `json:"matched_editions"`
	RequestedISBNs []string `json:"requested_isbn_13"`
	MatchedISBNs   []string `json:"matched_isbn_13"`
}

func candidates(text string) (map[string]bool, error) {
	result := make(map[string]bool)
	for _, field := range strings.FieldsFunc(text, func(c rune) bool {
		return c == ',' || c == '\r' || c == '\n' || c == '\t' || c == ' '
	}) {
		normalized := bookmodel.NormalizeISBN(field).EquivalentISBN13()
		if normalized == "" {
			return nil, fmt.Errorf("invalid candidate ISBN")
		}
		result[normalized] = false
		if len(result) > maxCandidates {
			return nil, fmt.Errorf("maximum %d candidate ISBNs", maxCandidates)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("at least one candidate ISBN is required")
	}
	return result, nil
}

func scanDump(source io.Reader, output io.Writer, seen map[string]bool, report *receipt, limit int) error {
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	encoder := json.NewEncoder(output)
	captured := make(map[string]bool)
	for scanner.Scan() {
		report.RowsScanned++
		row := scanner.Bytes()
		columns := bytes.SplitN(row, []byte{'\t'}, 5)
		if len(columns) != 5 || !bytes.Equal(columns[0], []byte("/type/edition")) {
			return fmt.Errorf("unexpected editions dump format at row %d", report.RowsScanned)
		}
		var record edition
		if err := json.Unmarshal(columns[4], &record); err != nil {
			return fmt.Errorf("invalid editions JSON at row %d: %w", report.RowsScanned, err)
		}
		if limit > 0 {
			if report.RowsScanned == limit {
				return nil
			}
			continue
		}

		var matches []string
		for _, raw := range append(record.ISBN13, record.ISBN10...) {
			canonical := bookmodel.NormalizeISBN(raw).EquivalentISBN13()
			if _, requested := seen[canonical]; !requested {
				continue
			}
			seen[canonical] = true
			matches = append(matches, canonical)
		}
		if len(matches) == 0 {
			continue
		}
		revision, err := strconv.Atoi(string(columns[2]))
		if err != nil || revision <= 0 || revision != record.Revision ||
			!editionKey.MatchString(record.Key) || string(columns[1]) != record.Key || len(columns[3]) == 0 {
			return fmt.Errorf("edition identity differs at row %d", report.RowsScanned)
		}
		if captured[record.Key] {
			return fmt.Errorf("duplicate source edition %s", record.Key)
		}
		captured[record.Key] = true
		report.Editions++
		if report.Editions > maxMatches {
			return fmt.Errorf("maximum %d matched editions", maxMatches)
		}
		sort.Strings(matches)
		matches = slices.Compact(matches)
		digest := sha256.Sum256(row)
		if err := encoder.Encode(matchedEdition{
			Provider: report.Provider, SourceSnapshot: report.SnapshotDate, EditionKey: record.Key,
			Revision: revision, LastModified: string(columns[3]), MatchedISBN13: matches,
			SourceSHA256: hex.EncodeToString(digest[:]), SourceRecord: json.RawMessage(columns[4]),
		}); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if report.RowsScanned == 0 || limit > 0 {
		return fmt.Errorf("editions dump ended before the expected rows were read")
	}
	return nil
}

func allowedSource(url *url.URL) bool {
	host := url.Hostname()
	return url.Scheme == "https" && url.User == nil && url.Port() == "" &&
		(host == "openlibrary.org" || host == "archive.org" || strings.HasSuffix(host, ".archive.org"))
}

func capture(dir, input string, probe bool) error {
	seen, err := candidates(input)
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: 165 * time.Minute,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 6 || !allowedSource(req.URL) {
				return errors.New("unexpected source redirect")
			}
			return nil
		},
	}
	req, err := http.NewRequest(http.MethodGet, latestEditions, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept-Encoding", "identity")
	if probe {
		req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", probeBytes-1))
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	want := http.StatusOK
	if probe {
		want = http.StatusPartialContent
	}
	if res.StatusCode != want {
		return fmt.Errorf("editions dump returned HTTP %d", res.StatusCode)
	}
	snapshot := archiveName.FindStringSubmatch(path.Base(res.Request.URL.Path))
	if len(snapshot) != 2 || path.Base(path.Dir(res.Request.URL.Path)) != "ol_dump_"+snapshot[1] {
		return fmt.Errorf("unexpected editions snapshot URL")
	}
	if _, err := time.Parse(time.DateOnly, snapshot[1]); err != nil {
		return err
	}
	size := res.ContentLength
	if probe {
		var start, last int64
		fields, err := fmt.Sscanf(res.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &last, &size)
		if fields != 3 || err != nil || start != 0 || last >= probeBytes || last < start ||
			last+1 != res.ContentLength || last >= size {
			return fmt.Errorf("invalid source range")
		}
	}
	if size <= 0 || size > maxArchive {
		return fmt.Errorf("editions dump size outside transfer budget")
	}
	report := receipt{
		Schema: "statground.book.open-library-editions-receipt.v1", Provider: "open-library",
		Status: "complete", SnapshotDate: snapshot[1], ArchiveBytes: size,
		SourceURL:      "https://archive.org/download/ol_dump_" + snapshot[1] + "/" + snapshot[0],
		RequestedISBNs: make([]string, 0, len(seen)), MatchedISBNs: []string{},
	}
	hash := sha256.New()
	compressed := &io.LimitedReader{R: res.Body, N: size + 1}
	source, err := gzip.NewReader(io.TeeReader(compressed, hash))
	if err != nil {
		return err
	}
	defer source.Close()

	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	if probe {
		report.Status = "source_sampled"
		if err := scanDump(source, io.Discard, seen, &report, probeRows); err != nil {
			return err
		}
	} else {
		if err := writeEditions(dir, source, seen, &report); err != nil {
			return err
		}
		if compressed.N != 1 {
			return fmt.Errorf("source length differs from recorded archive size")
		}
		report.ArchiveSHA256 = hex.EncodeToString(hash.Sum(nil))
	}
	for isbn, matched := range seen {
		report.RequestedISBNs = append(report.RequestedISBNs, isbn)
		if matched {
			report.MatchedISBNs = append(report.MatchedISBNs, isbn)
		}
	}
	sort.Strings(report.RequestedISBNs)
	sort.Strings(report.MatchedISBNs)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "receipt.json"), append(data, '\n'), 0600)
}

func writeEditions(dir string, source io.Reader, seen map[string]bool, report *receipt) error {
	output, err := os.OpenFile(filepath.Join(dir, "editions.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	digest := sha256.New()
	scanErr := scanDump(source, io.MultiWriter(output, digest), seen, report, 0)
	if err := errors.Join(scanErr, output.Close()); err != nil {
		return err
	}
	report.EditionsSHA256 = hex.EncodeToString(digest.Sum(nil))
	return nil
}

func main() {
	probe := flag.Bool("probe", false, "verify a small source prefix without capturing editions")
	isbns := flag.String("isbns", "", "additional comma-separated ISBNs")
	seed := flag.String("seed", "cmd/open_library_inbound/isbn-seeds.txt", "path to reviewed ISBN candidates")
	output := flag.String("output", "open-library-inbound", "new directory for capture output")
	flag.Parse()
	data, err := os.ReadFile(*seed)
	if err == nil {
		err = capture(*output, string(data)+"\n"+*isbns, *probe)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
