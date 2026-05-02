/*
Verification queries for Data_Book_NAVER_* after schema creation, Kafka ingestion, and/or migration.
*/

SELECT
    cluster,
    shard_num,
    replica_num,
    host_name,
    is_active
FROM system.clusters
WHERE cluster = 'statground_cluster'
ORDER BY shard_num, replica_num, host_name;

SELECT
    database,
    table,
    engine,
    total_rows,
    total_bytes,
    comment
FROM system.tables
WHERE database IN ('Data_Book_NAVER_Log', 'Data_Book_NAVER_Raw', 'Data_Book_NAVER_Mart')
  AND table NOT LIKE '.inner%'
ORDER BY database, table;

SELECT
    hostName() AS host,
    database,
    table,
    sum(rows) AS active_rows,
    formatReadableSize(sum(bytes_on_disk)) AS active_size
FROM clusterAllReplicas('statground_cluster', system.parts)
WHERE active
  AND database IN ('Data_Book_NAVER_Log', 'Data_Book_NAVER_Raw', 'Data_Book_NAVER_Mart')
GROUP BY
    host,
    database,
    table
ORDER BY database, table, host;

SELECT
    count() AS raw_rows,
    uniqExact(isbn) AS uniq_isbn,
    min(created_at) AS min_created_at,
    max(updated_at) AS max_updated_at
FROM `Data_Book_NAVER_Raw`.naver_book_raw;

SELECT
    created_at,
    search_mode,
    search_query,
    search_sort,
    search_start,
    api_total,
    fetched_count,
    status,
    error
FROM `Data_Book_NAVER_Log`.naver_collect_log
ORDER BY created_at DESC
LIMIT 50;

SELECT
    period_start,
    provider,
    search_mode,
    countMerge(rows_state) AS rows,
    uniqCombined64Merge(isbn_uniq_state) AS uniq_isbn,
    uniqCombined64Merge(author_uniq_state) AS uniq_author,
    uniqCombined64Merge(publisher_uniq_state) AS uniq_publisher,
    minMerge(min_collected_at_state) AS min_collected_at,
    maxMerge(max_collected_at_state) AS max_collected_at
FROM `Data_Book_NAVER_Mart`.naver_book_stats_hourly
GROUP BY
    period_start,
    provider,
    search_mode
ORDER BY period_start DESC, rows DESC
LIMIT 100;

SELECT
    period_start,
    provider,
    search_mode,
    countMerge(rows_state) AS rows,
    uniqCombined64Merge(isbn_uniq_state) AS uniq_isbn,
    uniqCombined64Merge(author_uniq_state) AS uniq_author,
    uniqCombined64Merge(publisher_uniq_state) AS uniq_publisher
FROM `Data_Book_NAVER_Mart`.naver_book_stats_daily
GROUP BY
    period_start,
    provider,
    search_mode
ORDER BY period_start DESC, rows DESC
LIMIT 100;

/* Extended non-NAVER-only and cache/Shotalk tables */
SELECT
    'legacy_book_catalog' AS table_name,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(source_primary) AS source_primary_count,
    min(created_at) AS min_created_at,
    max(created_at) AS max_created_at
FROM `Data_Book_NAVER_Raw`.legacy_book_catalog;

SELECT
    source_primary,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid
FROM `Data_Book_NAVER_Raw`.legacy_book_catalog
GROUP BY source_primary
ORDER BY rows DESC
LIMIT 50;

SELECT
    'book_marketplace_url' AS table_name,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(uuid_book) AS uniq_book_uuid,
    min(created_at) AS min_created_at,
    max(created_at) AS max_created_at
FROM `Data_Book_NAVER_Raw`.book_marketplace_url;

SELECT
    type,
    active,
    count() AS rows,
    uniqExact(uuid_book) AS uniq_book_uuid
FROM `Data_Book_NAVER_Raw`.book_marketplace_url
GROUP BY
    type,
    active
ORDER BY rows DESC
LIMIT 50;

SELECT
    'aladin_publisher_cache' AS table_name,
    count() AS rows,
    uniqExact(publisher) AS uniq_publisher,
    max(detected_last_page) AS max_detected_last_page,
    max(collected_at) AS max_collected_at
FROM `Data_Book_NAVER_Log`.aladin_publisher_cache;

SELECT
    'shotalk_search_result' AS table_name,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(commission_link) AS uniq_commission_link,
    min(created_at) AS min_created_at,
    max(created_at) AS max_created_at
FROM `Data_Book_NAVER_Raw`.shotalk_search_result;

SELECT
    'book_kafka_parse_error' AS table_name,
    count() AS rows,
    min(created_at) AS min_created_at,
    max(created_at) AS max_created_at
FROM `Data_Book_NAVER_Log`.book_kafka_parse_error;

SELECT
    created_at,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    left(error, 500) AS error_sample,
    left(raw_message, 500) AS raw_message_sample
FROM `Data_Book_NAVER_Log`.book_kafka_parse_error
ORDER BY created_at DESC
LIMIT 50;
