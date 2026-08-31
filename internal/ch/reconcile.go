package ch

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"statground_naver_book_go/internal/util"
)

const (
	ReconcileString     = "string"
	ReconcileUUID       = "uuid"
	ReconcileDateTime64 = "datetime64"
	ReconcileUInt64     = "uint64"

	maxReconciliationRows     = 2000
	maxReconciliationSQLBytes = 4 * 1024 * 1024
)

type ReconcileKeyColumn struct {
	Name string
	Kind string
}

type ReconcileSpec struct {
	Cluster       string
	LocalTable    string
	KeyColumns    []ReconcileKeyColumn
	VersionColumn string
	MaxRows       int
}

type ReconcileResult struct {
	MissingRows    []map[string]any
	AcceptedRows   int
	SupersededRows int
}

type reconciliationExpected struct {
	row     map[string]any
	digest  string
	version uint64
}

// ReconcileJSONEachRowContext compares one canonical outbox batch with every
// replica of its allowlisted local table. Identical physical replica copies are
// collapsed. Conflicting payloads at the same logical identity/version fail
// closed, while a strictly newer ReplacingMergeTree version supersedes an older
// pending row.
func (c *Client) ReconcileJSONEachRowContext(ctx context.Context, spec ReconcileSpec, rows []map[string]any) (ReconcileResult, error) {
	if err := ctx.Err(); err != nil {
		return ReconcileResult{}, err
	}
	limit := spec.MaxRows
	if limit < 1 || limit > maxReconciliationRows {
		limit = maxReconciliationRows
	}
	if len(rows) < 1 || len(rows) > limit || len(spec.KeyColumns) == 0 {
		return ReconcileResult{}, fmt.Errorf("reconciliation batch is outside the bounded contract")
	}
	if strings.TrimSpace(spec.Cluster) == "" {
		spec.Cluster = "statground_cluster"
	}
	if !identifierPattern.MatchString(spec.Cluster) {
		return ReconcileResult{}, fmt.Errorf("invalid reconciliation cluster")
	}
	database, table := SplitQualifiedTable(spec.LocalTable, c.Database)
	if !identifierPattern.MatchString(database) || !identifierPattern.MatchString(table) {
		return ReconcileResult{}, fmt.Errorf("invalid reconciliation local table")
	}
	if spec.VersionColumn != "" && !identifierPattern.MatchString(spec.VersionColumn) {
		return ReconcileResult{}, fmt.Errorf("invalid reconciliation version column")
	}
	for _, key := range spec.KeyColumns {
		if !identifierPattern.MatchString(key.Name) || !validReconciliationKind(key.Kind) {
			return ReconcileResult{}, fmt.Errorf("invalid reconciliation identity column")
		}
	}

	columns, err := reconciliationColumns(rows, spec)
	if err != nil {
		return ReconcileResult{}, err
	}
	expected := make(map[string]reconciliationExpected, len(rows))
	identities := make([]string, 0, len(rows))
	predicates := make([]string, 0, len(rows))
	for _, row := range rows {
		identity, predicate, identityErr := reconciliationIdentity(row, spec.KeyColumns)
		if identityErr != nil {
			return ReconcileResult{}, identityErr
		}
		if _, duplicate := expected[identity]; duplicate {
			return ReconcileResult{}, fmt.Errorf("outbox batch contains duplicate logical identity")
		}
		digest, digestErr := reconciliationDigest(spec.LocalTable, row)
		if digestErr != nil {
			return ReconcileResult{}, digestErr
		}
		var version uint64
		if spec.VersionColumn != "" {
			version, err = reconciliationVersion(row[spec.VersionColumn])
			if err != nil {
				return ReconcileResult{}, err
			}
		}
		expected[identity] = reconciliationExpected{row: row, digest: digest, version: version}
		identities = append(identities, identity)
		predicates = append(predicates, predicate)
	}

	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, "`"+column+"`")
	}
	query := fmt.Sprintf(`
        SELECT %s
        FROM clusterAllReplicas(%s, %s, %s)
        WHERE %s
        SETTINGS
            max_threads = 2,
            max_execution_time = 15,
            max_result_rows = %d,
            max_result_bytes = 67108864,
            result_overflow_mode = 'throw'
    `,
		strings.Join(quotedColumns, ", "),
		util.SQLString(spec.Cluster), util.SQLString(database), util.SQLString(table),
		strings.Join(predicates, " OR "), limit*16,
	)
	if len(query) > maxReconciliationSQLBytes {
		return ReconcileResult{}, fmt.Errorf("reconciliation query exceeds bounded size")
	}
	actualRows, err := c.queryJSONEachRowNumberContext(ctx, query)
	if err != nil {
		return ReconcileResult{}, err
	}

	actual := make(map[string]map[uint64]map[string]struct{}, len(expected))
	for _, row := range actualRows {
		identity, _, identityErr := reconciliationIdentity(row, spec.KeyColumns)
		if identityErr != nil {
			return ReconcileResult{}, identityErr
		}
		if _, ok := expected[identity]; !ok {
			return ReconcileResult{}, fmt.Errorf("reconciliation returned an unexpected logical identity")
		}
		var version uint64
		if spec.VersionColumn != "" {
			version, err = reconciliationVersion(row[spec.VersionColumn])
			if err != nil {
				return ReconcileResult{}, err
			}
		}
		digest, digestErr := reconciliationDigest(spec.LocalTable, row)
		if digestErr != nil {
			return ReconcileResult{}, digestErr
		}
		if actual[identity] == nil {
			actual[identity] = make(map[uint64]map[string]struct{})
		}
		if actual[identity][version] == nil {
			actual[identity][version] = make(map[string]struct{})
		}
		actual[identity][version][digest] = struct{}{}
	}

	result := ReconcileResult{MissingRows: make([]map[string]any, 0)}
	for _, identity := range identities {
		want := expected[identity]
		versions := actual[identity]
		if len(versions) == 0 {
			result.MissingRows = append(result.MissingRows, want.row)
			continue
		}
		maxVersion := uint64(0)
		for version := range versions {
			if version > maxVersion {
				maxVersion = version
			}
		}
		digests := versions[maxVersion]
		if len(digests) != 1 {
			return ReconcileResult{}, fmt.Errorf("reconciliation found conflicting duplicate payloads")
		}
		if spec.VersionColumn != "" && maxVersion > want.version {
			result.AcceptedRows++
			result.SupersededRows++
			continue
		}
		if spec.VersionColumn != "" && maxVersion < want.version {
			result.MissingRows = append(result.MissingRows, want.row)
			continue
		}
		if _, matches := digests[want.digest]; !matches {
			return ReconcileResult{}, fmt.Errorf("reconciliation found an equal-version payload mismatch")
		}
		result.AcceptedRows++
	}
	return result, nil
}

func validReconciliationKind(kind string) bool {
	switch kind {
	case ReconcileString, ReconcileUUID, ReconcileDateTime64, ReconcileUInt64:
		return true
	default:
		return false
	}
}

func reconciliationColumns(rows []map[string]any, spec ReconcileSpec) ([]string, error) {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for column := range row {
			if !identifierPattern.MatchString(column) {
				return nil, fmt.Errorf("invalid reconciliation payload column")
			}
			seen[column] = struct{}{}
		}
	}
	for _, key := range spec.KeyColumns {
		seen[key.Name] = struct{}{}
	}
	if spec.VersionColumn != "" {
		seen[spec.VersionColumn] = struct{}{}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns, nil
}

func reconciliationIdentity(row map[string]any, keys []ReconcileKeyColumn) (string, string, error) {
	identity := make([]string, 0, len(keys))
	predicate := make([]string, 0, len(keys))
	for _, key := range keys {
		value, literal, err := reconciliationValue(row[key.Name], key.Kind)
		if err != nil {
			return "", "", fmt.Errorf("invalid reconciliation identity %s: %w", key.Name, err)
		}
		identity = append(identity, key.Kind+":"+value)
		predicate = append(predicate, "`"+key.Name+"` = "+literal)
	}
	return strings.Join(identity, "\x1f"), "(" + strings.Join(predicate, " AND ") + ")", nil
}

func reconciliationValue(raw any, kind string) (string, string, error) {
	if raw == nil {
		return "", "", fmt.Errorf("empty value")
	}
	value := strings.TrimSpace(reconciliationString(raw))
	if value == "" {
		return "", "", fmt.Errorf("empty value")
	}
	switch kind {
	case ReconcileString:
		return value, util.SQLString(value), nil
	case ReconcileUUID:
		if !uuidTextPattern.MatchString(value) {
			return "", "", fmt.Errorf("invalid UUID")
		}
		return strings.ToLower(value), "toUUID(" + util.SQLString(value) + ")", nil
	case ReconcileDateTime64:
		return value, "toDateTime64(" + util.SQLString(value) + ", 3, 'Asia/Seoul')", nil
	case ReconcileUInt64:
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return "", "", err
		}
		value = strconv.FormatUint(parsed, 10)
		return value, "toUInt64(" + value + ")", nil
	default:
		return "", "", fmt.Errorf("unsupported kind")
	}
}

func reconciliationVersion(raw any) (uint64, error) {
	if raw == nil {
		return 0, fmt.Errorf("invalid reconciliation version")
	}
	value := strings.TrimSpace(reconciliationString(raw))
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid reconciliation version")
	}
	return version, nil
}

func reconciliationString(value any) string {
	switch typed := value.(type) {
	case json.Number:
		return typed.String()
	case string:
		return typed
	default:
		return fmt.Sprint(value)
	}
}

func reconciliationDigest(table string, row map[string]any) (string, error) {
	columns, _, payload, err := CanonicalJSONEachRow([]map[string]any{row})
	if err != nil {
		return "", err
	}
	return JSONEachRowDeduplicationToken(table, columns, payload), nil
}

var uuidTextPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
