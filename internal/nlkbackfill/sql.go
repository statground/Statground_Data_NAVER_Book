package nlkbackfill

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"

	"statground_naver_book_go/internal/util"
)

const DefaultRawTable = "Data_Book_NLK_Raw.nlk_resource_raw"

var projectionTargets = map[Projection]string{
	ProjectionAuthority:           "Data_Book_NLK_Service.nlk_authority",
	ProjectionBibliography:        "Data_Book_NLK_Service.nlk_bibliography",
	ProjectionLibrary:             "Data_Book_NLK_Service.nlk_library",
	ProjectionProviderLatest:      "Data_Book_Service.book_provider_latest",
	ProjectionBibliographyContext: "Data_Book_Service.book_bibliography_context",
	ProjectionKDCSummary:          "Data_Book_Service.book_kdc_summary",
	ProjectionISBNAlias:           "Data_Book_Service.book_isbn_alias",
}

var projectionLocalTargets = map[Projection]string{
	ProjectionAuthority:           "Data_Book_NLK_Service.nlk_authority_local",
	ProjectionBibliography:        "Data_Book_NLK_Service.nlk_bibliography_local",
	ProjectionLibrary:             "Data_Book_NLK_Service.nlk_library_local",
	ProjectionProviderLatest:      "Data_Book_Service.book_provider_latest_local",
	ProjectionBibliographyContext: "Data_Book_Service.book_bibliography_context_local",
	ProjectionKDCSummary:          "Data_Book_Service.book_kdc_summary_local",
	ProjectionISBNAlias:           "Data_Book_Service.book_isbn_alias_local",
}

func ProjectionTarget(projection Projection) (string, bool) {
	target, ok := projectionTargets[projection]
	return target, ok
}

func ProjectionLocalTarget(projection Projection) (string, bool) {
	target, ok := projectionLocalTargets[projection]
	return target, ok
}

// BuildProjectionSQL creates one INSERT SELECT over a contiguous raw primary-key
// interval. The table names come from a closed allowlist and every source query
// includes the complete archive lineage plus [start,end) record-index bounds.
func BuildProjectionSQL(
	projection Projection,
	transformVersion string,
	entry RawEntry,
	recordRange RecordRange,
	rawTable string,
	targetTable string,
) (string, error) {
	expectedTarget, ok := ProjectionTarget(projection)
	if !ok || rawTable != DefaultRawTable || targetTable != expectedTarget {
		return "", safeError("configuration")
	}
	applies, err := ProjectionAppliesToDataset(projection, entry.DatasetName)
	if err != nil {
		return "", err
	}
	if !applies {
		return "", safeError("projection_dataset_mismatch")
	}
	if entry.SnapshotDate.IsZero() ||
		strings.TrimSpace(entry.DatasetName) == "" ||
		strings.TrimSpace(entry.SourceArchive) == "" ||
		strings.TrimSpace(entry.SourceEntry) == "" ||
		recordRange.End <= recordRange.Start ||
		recordRange.End > entry.NextRecordIndex {
		return "", safeError("invalid_range")
	}
	transformVersion = strings.TrimSpace(transformVersion)
	if transformVersion == "" {
		return "", safeError("invalid_transform_version")
	}

	predicate := boundedRawPredicate(entry, recordRange)
	var query string
	switch projection {
	case ProjectionAuthority:
		query = authoritySQL(rawTable, targetTable, predicate)
	case ProjectionBibliography:
		query = bibliographySQL(rawTable, targetTable, predicate)
	case ProjectionLibrary:
		query = librarySQL(rawTable, targetTable, predicate)
	case ProjectionProviderLatest:
		query = providerLatestSQL(rawTable, targetTable, predicate)
	case ProjectionBibliographyContext:
		query = bibliographyContextSQL(rawTable, targetTable, predicate)
	case ProjectionKDCSummary:
		query = kdcSummarySQL(rawTable, targetTable, predicate)
	case ProjectionISBNAlias:
		query = isbnAliasSQL(rawTable, targetTable, predicate)
	default:
		return "", safeError("invalid_projection")
	}
	return strings.TrimSpace(query) + "\n" + projectionSettings(
		projectionDedupToken(projection, transformVersion, entry, recordRange),
		projection == ProjectionKDCSummary,
	), nil
}

func boundedRawPredicate(entry RawEntry, recordRange RecordRange) string {
	return fmt.Sprintf(
		`raw.dataset_snapshot_date = toDate(%s)
  AND raw.dataset_name = %s
  AND raw.source_archive = %s
  AND raw.source_entry = %s
  AND raw.source_record_index >= %d
  AND raw.source_record_index < %d`,
		util.SQLString(entry.SnapshotDate.Format("2006-01-02")),
		util.SQLString(entry.DatasetName),
		util.SQLString(entry.SourceArchive),
		util.SQLString(entry.SourceEntry),
		recordRange.Start,
		recordRange.End,
	)
}

func projectionSettings(token string, aggregate bool) string {
	externalGroupBy := ""
	if aggregate {
		externalGroupBy = "\n    max_bytes_before_external_group_by = 300000000,\n    distributed_aggregation_memory_efficient = 1,"
	}
	return fmt.Sprintf(`SETTINGS
    max_threads = 1,
    max_memory_usage = 2000000000,%s
    parallel_view_processing = 0,
    parallel_distributed_insert_select = 0,
    distributed_foreground_insert = 1,
    insert_quorum = 2,
    insert_quorum_parallel = 1,
    insert_quorum_timeout = 600000,
    insert_deduplicate = 1,
    insert_deduplication_token = '%s',
    receive_timeout = 660,
    send_timeout = 660,
    load_balancing = 'first_or_random',
    load_balancing_first_offset = 0,
    prefer_localhost_replica = 0,
    max_block_size = 8192,
    min_insert_block_size_rows = 50000,
    min_insert_block_size_bytes = 67108864,
    max_execution_time = 1800`,
		externalGroupBy,
		token,
	)
}

func projectionDedupToken(
	projection Projection,
	transformVersion string,
	entry RawEntry,
	recordRange RecordRange,
) string {
	lineage := strings.Join([]string{
		string(projection),
		transformVersion,
		entry.SnapshotDate.Format("2006-01-02"),
		entry.DatasetName,
		entry.SourceArchive,
		entry.SourceEntry,
		strconv.FormatUint(recordRange.Start, 10),
		strconv.FormatUint(recordRange.End, 10),
	}, "\x1f")
	sum := sha256.Sum256([]byte(lineage))
	return fmt.Sprintf("%x", sum[:])
}

func authoritySQL(rawTable, targetTable, predicate string) string {
	return fmt.Sprintf(`INSERT INTO %s
(
    authority_id, uuid, run_uuid, version, active, authority_type,
    preferred_label, alt_labels, description, same_as_ids, birth_raw,
    death_raw, jobs, fields, isni_ids, broader_ids, related_ids, keywords,
    dataset_name, dataset_snapshot_date, dataset_updated_at, source_url,
    license_name, license_url, attribution, content_hash, imported_at,
    ingested_at
)
SELECT
    raw.resource_id, raw.uuid, raw.run_uuid, raw.version, toUInt8(1),
    raw.resource_type, if(notEmpty(raw.label), raw.label, raw.title),
    raw.alt_labels, raw.description, raw.same_as_ids, raw.birth_raw,
    raw.death_raw, raw.jobs, raw.fields, raw.isni_ids, raw.broader_ids,
    raw.related_ids, raw.keywords, raw.dataset_name,
    raw.dataset_snapshot_date, raw.dataset_updated_at, raw.source_url,
    raw.license_name, raw.license_url, raw.attribution, raw.content_hash,
    raw.imported_at, now64(3, 'Asia/Seoul')
FROM %s AS raw
WHERE %s
  AND raw.resource_type IN ('person', 'organization', 'concept')`,
		targetTable, rawTable, predicate)
}

func bibliographySQL(rawTable, targetTable, predicate string) string {
	return fmt.Sprintf(`INSERT INTO %s
(
    resource_id, uuid, run_uuid, version, active, resource_type, title,
    label, description, creators, creator_ids, publisher, issued_raw,
    issued_year, languages, publication_places, keywords, subject_ids,
    kdc_codes, ddc_codes, series_titles, extents, related_resource_ids,
    isbn_raw, isbn10, isbn13, canonical_isbns, dataset_name,
    dataset_snapshot_date, dataset_updated_at, source_url, license_name,
    license_url, attribution, source_archive, source_entry,
    source_record_index, content_hash, imported_at, ingested_at
)
SELECT
    raw.resource_id, raw.uuid, raw.run_uuid, raw.version, toUInt8(1),
    raw.resource_type, if(notEmpty(raw.title), raw.title, raw.label),
    raw.label, raw.description, raw.creators, raw.creator_ids, raw.publisher,
    raw.issued_raw, raw.issued_year, raw.languages, raw.publication_places,
    raw.keywords, raw.subject_ids, raw.kdc_codes, raw.ddc_codes,
    raw.series_titles, raw.extents, raw.related_resource_ids, raw.isbn_raw,
    raw.isbn10, raw.isbn13, raw.canonical_isbns, raw.dataset_name,
    raw.dataset_snapshot_date, raw.dataset_updated_at, raw.source_url,
    raw.license_name, raw.license_url, raw.attribution, raw.source_archive,
    raw.source_entry, raw.source_record_index, raw.content_hash,
    raw.imported_at, now64(3, 'Asia/Seoul')
FROM %s AS raw
WHERE %s
  AND raw.resource_type IN (
      'book', 'offline_material', 'online_material', 'audiovisual',
      'government_publication', 'serial', 'thesis'
  )`, targetTable, rawTable, predicate)
}

func librarySQL(rawTable, targetTable, predicate string) string {
	return fmt.Sprintf(`INSERT INTO %s
(
    library_id, uuid, run_uuid, version, active, identifier, name,
    alt_labels, description, keywords, library_type, homepage, phone, fax,
    opening_year, closed_days, summer_hours, winter_hours, locations,
    same_as_ids, dataset_snapshot_date, dataset_updated_at, source_url,
    license_name, license_url, attribution, content_hash, imported_at,
    ingested_at
)
SELECT
    raw.resource_id, raw.uuid, raw.run_uuid, raw.version, toUInt8(1),
    raw.library_identifier, if(notEmpty(raw.label), raw.label, raw.title),
    raw.alt_labels, raw.description, raw.keywords, raw.library_type,
    raw.library_homepage, raw.library_phone, raw.library_fax,
    raw.library_opening_year, raw.library_closed_days,
    raw.library_summer_hours, raw.library_winter_hours,
    raw.library_locations, raw.same_as_ids, raw.dataset_snapshot_date,
    raw.dataset_updated_at, raw.source_url, raw.license_name,
    raw.license_url, raw.attribution, raw.content_hash, raw.imported_at,
    now64(3, 'Asia/Seoul')
FROM %s AS raw
WHERE %s
  AND raw.resource_type = 'library'`, targetTable, rawTable, predicate)
}

func providerLatestSQL(rawTable, targetTable, predicate string) string {
	return fmt.Sprintf(`INSERT INTO %s
SELECT
    arrayElement(raw.canonical_isbns, isbn_index),
    if(
        notEmpty(arrayElement(raw.isbn_raw, isbn_index)),
        arrayElement(raw.isbn_raw, isbn_index),
        arrayElement(raw.canonical_isbns, isbn_index)
    ),
    arrayElement(raw.isbn10, isbn_index),
    arrayElement(raw.isbn13, isbn_index),
    toUInt8(1),
    if(
        notEmpty(arrayElement(raw.isbn_raw, isbn_index)),
        arrayElement(raw.isbn_raw, isbn_index),
        arrayElement(raw.canonical_isbns, isbn_index)
    ),
    raw.uuid,
    'nlk_lod',
    raw.version,
    raw.dataset_updated_at,
    raw.dataset_updated_at,
    raw.imported_at,
    if(notEmpty(raw.title), raw.title, raw.label),
    raw.description,
    raw.resource_id,
    '',
    raw.creators,
    raw.publisher,
    CAST([], 'Array(String)'),
    CAST(NULL, 'Nullable(UInt64)'),
    CAST(NULL, 'Nullable(UInt64)'),
    '',
    raw.content_hash,
    if(
        isNull(raw.issued_year)
        OR assumeNotNull(raw.issued_year) < 1900
        OR assumeNotNull(raw.issued_year) > 2299,
        CAST(NULL, 'Nullable(DateTime64(3, ''Asia/Seoul''))'),
        parseDateTime64BestEffortOrNull(
            concat(toString(ifNull(raw.issued_year, toUInt16(0))), '-01-01 00:00:00'),
            3,
            'Asia/Seoul'
        )
    ),
    'bulk_snapshot',
    raw.dataset_name,
    'snapshot',
    'nlk_lod_bulk',
    raw.uuid,
    raw.run_uuid,
    raw.resource_id,
    '',
    arrayStringConcat(raw.creators, '^'),
    CAST(NULL, 'Nullable(UInt32)'),
    raw.description,
    if(isNull(raw.issued_year), '', toString(assumeNotNull(raw.issued_year))),
    now64(3, 'Asia/Seoul')
FROM %s AS raw
ARRAY JOIN arrayEnumerate(raw.canonical_isbns) AS isbn_index
WHERE %s
  AND raw.resource_type IN (
      'book', 'offline_material', 'online_material', 'audiovisual',
      'government_publication', 'serial', 'thesis'
  )
  AND notEmpty(arrayElement(raw.canonical_isbns, isbn_index))
  AND arrayElement(raw.isbn_valid, isbn_index) = 1
  AND length(raw.isbn_raw) = length(raw.canonical_isbns)
  AND length(raw.isbn10) = length(raw.canonical_isbns)
  AND length(raw.isbn13) = length(raw.canonical_isbns)
  AND length(raw.isbn_valid) = length(raw.canonical_isbns)`,
		targetTable, rawTable, predicate)
}

func bibliographyContextSQL(rawTable, targetTable, predicate string) string {
	return fmt.Sprintf(`INSERT INTO %s
SELECT
    arrayElement(raw.canonical_isbns, isbn_index),
    raw.resource_id,
    raw.uuid,
    raw.version,
    if(notEmpty(raw.title), raw.title, raw.label),
    raw.creators,
    raw.creator_ids,
    raw.publisher,
    raw.issued_year,
    raw.kdc_codes,
    raw.keywords,
    raw.languages,
    raw.publication_places,
    raw.series_titles,
    raw.extents,
    raw.subject_ids,
    raw.related_resource_ids,
    raw.dataset_snapshot_date,
    raw.dataset_updated_at,
    raw.source_url,
    raw.license_name,
    raw.license_url,
    raw.attribution,
    raw.content_hash,
    now64(3, 'Asia/Seoul')
FROM %s AS raw
ARRAY JOIN arrayFilter(
    i -> arrayElement(raw.isbn_valid, i) = 1
        AND notEmpty(arrayElement(raw.canonical_isbns, i)),
    arrayEnumerate(raw.canonical_isbns)
) AS isbn_index
WHERE %s
  AND raw.resource_type IN (
      'book', 'offline_material', 'online_material', 'audiovisual',
      'government_publication', 'serial', 'thesis'
  )
  AND length(raw.isbn_valid) = length(raw.canonical_isbns)`,
		targetTable, rawTable, predicate)
}

func kdcSummarySQL(rawTable, targetTable, predicate string) string {
	return fmt.Sprintf(`INSERT INTO %s
SELECT
    dataset_snapshot_date,
    CAST(kdc_group, 'FixedString(3)') AS kdc_code,
    multiIf(
        kdc_group = '000', '총류',
        kdc_group = '100', '철학',
        kdc_group = '200', '종교',
        kdc_group = '300', '사회과학',
        kdc_group = '400', '자연과학',
        kdc_group = '500', '기술과학',
        kdc_group = '600', '예술',
        kdc_group = '700', '언어',
        kdc_group = '800', '문학',
        kdc_group = '900', '역사',
        '미분류'
    ) AS kdc_label,
    uniqExactState(canonical_isbn),
    maxState(dataset_updated_at)
FROM
(
    SELECT
        raw.dataset_snapshot_date,
        raw.dataset_updated_at,
        arrayJoin(
            arrayDistinct(
                arrayMap(
                    i -> arrayElement(raw.canonical_isbns, i),
                    arrayFilter(
                        i -> arrayElement(raw.isbn_valid, i) = 1
                            AND notEmpty(arrayElement(raw.canonical_isbns, i)),
                        arrayEnumerate(raw.canonical_isbns)
                    )
                )
            )
        ) AS canonical_isbn,
        arrayJoin(
            arrayDistinct(
                arrayMap(
                    code -> concat(substring(code, 1, 1), '00'),
                    arrayFilter(code -> match(code, '^[0-9]'), raw.kdc_codes)
                )
            )
        ) AS kdc_group
    FROM %s AS raw
    WHERE %s
      AND raw.resource_type IN (
          'book', 'offline_material', 'online_material', 'audiovisual',
          'government_publication', 'serial', 'thesis'
      )
      AND length(raw.isbn_valid) = length(raw.canonical_isbns)
) AS expanded
GROUP BY dataset_snapshot_date, kdc_code, kdc_label`,
		targetTable, rawTable, predicate)
}

func isbnAliasSQL(rawTable, targetTable, predicate string) string {
	return fmt.Sprintf(`INSERT INTO %s
SELECT
    alias_isbn,
    canonical_isbn,
    multiIf(
        alias_isbn = canonical_isbn, 'canonical',
        alias_isbn = normalized_isbn13, 'isbn13',
        alias_isbn = normalized_isbn10, 'isbn10',
        'raw'
    ),
    'nlk_lod',
    uuid,
    version,
    toUInt8(1),
    dataset_updated_at,
    dataset_updated_at,
    'nlk_lod_bulk',
    now64(3, 'Asia/Seoul')
FROM
(
    SELECT
        raw.uuid,
        raw.version,
        raw.dataset_updated_at,
        arrayElement(raw.canonical_isbns, isbn_index) AS canonical_isbn,
        arrayElement(raw.isbn_raw, isbn_index) AS original_isbn,
        arrayElement(raw.isbn10, isbn_index) AS normalized_isbn10,
        arrayElement(raw.isbn13, isbn_index) AS normalized_isbn13
    FROM %s AS raw
    ARRAY JOIN arrayEnumerate(raw.canonical_isbns) AS isbn_index
    WHERE %s
      AND raw.resource_type IN (
          'book', 'offline_material', 'online_material', 'audiovisual',
          'government_publication', 'serial', 'thesis'
      )
      AND notEmpty(arrayElement(raw.canonical_isbns, isbn_index))
      AND arrayElement(raw.isbn_valid, isbn_index) = 1
      AND length(raw.isbn_raw) = length(raw.canonical_isbns)
      AND length(raw.isbn10) = length(raw.canonical_isbns)
      AND length(raw.isbn13) = length(raw.canonical_isbns)
      AND length(raw.isbn_valid) = length(raw.canonical_isbns)
) AS indexed
ARRAY JOIN arrayDistinct(
    arrayFilter(
        alias -> notEmpty(alias),
        [original_isbn, normalized_isbn10, normalized_isbn13, canonical_isbn]
    )
) AS alias_isbn
WHERE notEmpty(alias_isbn)`,
		targetTable, rawTable, predicate)
}
