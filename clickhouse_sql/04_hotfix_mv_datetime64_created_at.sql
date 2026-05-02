/*
Recreate Data_Book_NAVER Kafka/statistics materialized views for ClickHouse 26.1.2.11.

Use this only when MVs were created with an older definition or failed part-way.
Target tables are not dropped. Kafka consumption restarts when Kafka MVs are attached again.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

DROP TABLE IF EXISTS `Data_Book_NAVER_Log`.mv_kafka_book_events_to_book_events ON CLUSTER statground_cluster;
DROP TABLE IF EXISTS `Data_Book_NAVER_Log`.mv_kafka_book_events_to_parse_error ON CLUSTER statground_cluster;
DROP TABLE IF EXISTS `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_naver_book_raw ON CLUSTER statground_cluster;
DROP TABLE IF EXISTS `Data_Book_NAVER_Log`.mv_kafka_book_events_to_naver_collect_log ON CLUSTER statground_cluster;
DROP TABLE IF EXISTS `Data_Book_NAVER_Mart`.mv_naver_book_raw_to_stats_hourly ON CLUSTER statground_cluster;
DROP TABLE IF EXISTS `Data_Book_NAVER_Mart`.mv_naver_book_raw_to_stats_daily ON CLUSTER statground_cluster;
DROP TABLE IF EXISTS `Data_Book_NAVER_Log`.mv_kafka_book_events_to_aladin_publisher_cache ON CLUSTER statground_cluster;
DROP TABLE IF EXISTS `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_book_marketplace_url ON CLUSTER statground_cluster;
DROP TABLE IF EXISTS `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_shotalk_search_result ON CLUSTER statground_cluster;

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Log`.mv_kafka_book_events_to_book_events
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Log`.book_events
AS
SELECT
    ifNull(toUUIDOrNull(k.event_uuid), toUUID('00000000-0000-0000-0000-000000000000')) AS event_uuid,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    k.source AS source,
    k.host AS host,
    toUUIDOrNull(nullIf(k.uuid_user, '')) AS uuid_user,
    toIPv6OrDefault(k.ip) AS ip,
    k.url AS url,
    k.event_type AS event_type,
    k.payload AS payload,
    coalesce(
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS created_at,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Book_NAVER_Log`.kafka_book_events_queue AS k
WHERE length(ifNull(_error, '')) = 0;

ALTER TABLE `Data_Book_NAVER_Log`.mv_kafka_book_events_to_book_events
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from valid Kafka Engine book.events rows to Data_Book_NAVER_Log.book_events; malformed rows go to book_kafka_parse_error; OLAP 전용; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Log`.mv_kafka_book_events_to_parse_error
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Log`.book_kafka_parse_error
AS
SELECT
    ifNull(toUUIDOrNull(k.event_uuid), toUUID('00000000-0000-0000-0000-000000000000')) AS event_uuid,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    _error AS error,
    _raw_message AS raw_message,
    now64(3, 'Asia/Seoul') AS created_at,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Book_NAVER_Log`.kafka_book_events_queue AS k
WHERE length(ifNull(_error, '')) > 0;

ALTER TABLE `Data_Book_NAVER_Log`.mv_kafka_book_events_to_parse_error
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from malformed Kafka Engine book.events rows to Data_Book_NAVER_Log.book_kafka_parse_error; OLAP monitoring; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_naver_book_raw
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Raw`.naver_book_raw
AS
SELECT
    ifNull(toUUIDOrNull(JSONExtractString(k.payload, 'uuid')), ifNull(toUUIDOrNull(k.event_uuid), toUUID('00000000-0000-0000-0000-000000000000'))) AS uuid,
    'naver' AS provider,
    toUInt64OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'version'), ''), '0')) AS version,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'created_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS created_at,
    JSONExtractString(k.payload, 'created_log') AS created_log,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'updated_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS updated_at,
    JSONExtractString(k.payload, 'updated_log') AS updated_log,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'collected_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS collected_at,
    JSONExtractString(k.payload, 'title') AS title,
    JSONExtractString(k.payload, 'link') AS link,
    JSONExtractString(k.payload, 'image') AS image,
    JSONExtractString(k.payload, 'author') AS author,
    toUInt32OrNull(nullIf(nullIf(JSONExtractRaw(k.payload, 'discount'), ''), 'null')) AS discount,
    JSONExtractString(k.payload, 'publisher') AS publisher,
    JSONExtractString(k.payload, 'isbn') AS isbn,
    JSONExtractString(k.payload, 'description') AS description,
    JSONExtractString(k.payload, 'pubdate') AS pubdate,
    JSONExtractString(k.payload, 'search_mode') AS search_mode,
    JSONExtractString(k.payload, 'search_query') AS search_query,
    JSONExtractString(k.payload, 'search_sort') AS search_sort,
    toUInt16OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'search_start'), ''), '0')) AS search_start,
    toUInt16OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'search_display'), ''), '0')) AS search_display,
    toUInt32OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'api_total'), ''), '0')) AS api_total,
    if(notEmpty(JSONExtractString(k.payload, 'source')), JSONExtractString(k.payload, 'source'), k.source) AS source,
    ifNull(toUUIDOrNull(k.event_uuid), toUUID('00000000-0000-0000-0000-000000000000')) AS event_uuid,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    k.payload AS payload,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Book_NAVER_Log`.kafka_book_events_queue AS k
WHERE length(ifNull(_error, '')) = 0
  AND k.event_type = 'book.naver.raw.v1'
  AND notEmpty(JSONExtractString(k.payload, 'isbn'));

ALTER TABLE `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_naver_book_raw
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka book.naver.raw.v1 events to Data_Book_NAVER_Raw.naver_book_raw; uses toString(k.created_at) to support String/DateTime64 Kafka schemas; OLAP 전용; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Log`.mv_kafka_book_events_to_naver_collect_log
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Log`.naver_collect_log
AS
SELECT
    ifNull(toUUIDOrNull(JSONExtractString(k.payload, 'log_uuid')), ifNull(toUUIDOrNull(k.event_uuid), toUUID('00000000-0000-0000-0000-000000000000'))) AS log_uuid,
    ifNull(toUUIDOrNull(k.event_uuid), toUUID('00000000-0000-0000-0000-000000000000')) AS event_uuid,
    'naver' AS provider,
    k.source AS source,
    k.host AS host,
    toIPv6OrDefault(k.ip) AS ip,
    JSONExtractString(k.payload, 'search_mode') AS search_mode,
    JSONExtractString(k.payload, 'search_query') AS search_query,
    JSONExtractString(k.payload, 'search_sort') AS search_sort,
    toUInt16OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'search_start'), ''), '0')) AS search_start,
    toUInt16OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'search_display'), ''), '0')) AS search_display,
    toUInt32OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'api_total'), ''), '0')) AS api_total,
    toUInt16OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'fetched_count'), ''), '0')) AS fetched_count,
    JSONExtractString(k.payload, 'status') AS status,
    JSONExtractString(k.payload, 'error') AS error,
    JSONExtractString(k.payload, 'collect_log') AS collect_log,
    k.url AS url,
    k.payload AS payload,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'created_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS created_at,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Book_NAVER_Log`.kafka_book_events_queue AS k
WHERE length(ifNull(_error, '')) = 0
  AND k.event_type = 'book.naver.search_log.v1';

ALTER TABLE `Data_Book_NAVER_Log`.mv_kafka_book_events_to_naver_collect_log
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka book.naver.search_log.v1 events to Data_Book_NAVER_Log.naver_collect_log; uses toString(k.created_at) to support String/DateTime64 Kafka schemas; OLAP 전용; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Mart`.mv_naver_book_raw_to_stats_hourly
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Mart`.naver_book_stats_hourly_local
AS
SELECT
    toStartOfHour(collected_at) AS period_start,
    provider AS provider,
    search_mode AS search_mode,
    countState() AS rows_state,
    uniqCombined64State(isbn) AS isbn_uniq_state,
    uniqCombined64State(author) AS author_uniq_state,
    uniqCombined64State(publisher) AS publisher_uniq_state,
    minState(collected_at) AS min_collected_at_state,
    maxState(collected_at) AS max_collected_at_state
FROM `Data_Book_NAVER_Raw`.naver_book_raw_local
GROUP BY
    period_start,
    provider,
    search_mode;

ALTER TABLE `Data_Book_NAVER_Mart`.mv_naver_book_raw_to_stats_hourly
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view that stores NAVER Book hourly statistics inside ClickHouse; GitHub repo does not generate stats; OLAP 전용; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Mart`.mv_naver_book_raw_to_stats_daily
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Mart`.naver_book_stats_daily_local
AS
SELECT
    toStartOfDay(collected_at) AS period_start,
    provider AS provider,
    search_mode AS search_mode,
    countState() AS rows_state,
    uniqCombined64State(isbn) AS isbn_uniq_state,
    uniqCombined64State(author) AS author_uniq_state,
    uniqCombined64State(publisher) AS publisher_uniq_state,
    minState(collected_at) AS min_collected_at_state,
    maxState(collected_at) AS max_collected_at_state
FROM `Data_Book_NAVER_Raw`.naver_book_raw_local
GROUP BY
    period_start,
    provider,
    search_mode;

ALTER TABLE `Data_Book_NAVER_Mart`.mv_naver_book_raw_to_stats_daily
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view that stores NAVER Book daily statistics inside ClickHouse; GitHub repo does not generate stats; OLAP 전용; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Log`.mv_kafka_book_events_to_aladin_publisher_cache
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Log`.aladin_publisher_cache
AS
SELECT
    JSONExtractString(k.payload, 'publisher') AS publisher,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'collected_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS collected_at,
    toUInt32OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'detected_last_page'), ''), '0')) AS detected_last_page,
    ifNull(toUUIDOrNull(if(notEmpty(JSONExtractString(k.payload, 'run_uuid')), JSONExtractString(k.payload, 'run_uuid'), k.event_uuid)), toUUID('00000000-0000-0000-0000-000000000000')) AS run_uuid,
    if(notEmpty(JSONExtractString(k.payload, 'source')), JSONExtractString(k.payload, 'source'), 'aladin') AS source
FROM `Data_Book_NAVER_Log`.kafka_book_events_queue AS k
WHERE length(ifNull(_error, '')) = 0
  AND k.event_type = 'book.aladin.publisher_cache.v1'
  AND notEmpty(JSONExtractString(k.payload, 'publisher'));

ALTER TABLE `Data_Book_NAVER_Log`.mv_kafka_book_events_to_aladin_publisher_cache
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka book.aladin.publisher_cache.v1 events to Data_Book_NAVER_Log.aladin_publisher_cache; OLAP 전용; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_book_marketplace_url
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Raw`.book_marketplace_url
AS
SELECT
    ifNull(toUUIDOrNull(JSONExtractString(k.payload, 'uuid')), ifNull(toUUIDOrNull(k.event_uuid), toUUID('00000000-0000-0000-0000-000000000000'))) AS uuid,
    ifNull(toUUIDOrNull(JSONExtractString(k.payload, 'uuid_book')), toUUID('00000000-0000-0000-0000-000000000000')) AS uuid_book,
    ifNull(toUUIDOrNull(JSONExtractString(k.payload, 'uuid_marketplace')), toUUID('00000000-0000-0000-0000-000000000000')) AS uuid_marketplace,
    JSONExtractString(k.payload, 'url') AS url,
    toUInt8OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'active'), ''), '0')) AS active,
    toUUIDOrNull(nullIf(JSONExtractString(k.payload, 'uuid_affiliate'), '')) AS uuid_affiliate,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'created_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS created_at,
    JSONExtractString(k.payload, 'created_log') AS created_log,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'updated_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS updated_at,
    JSONExtractString(k.payload, 'updated_log') AS updated_log,
    JSONExtractString(k.payload, 'type') AS type,
    now64(3, 'Asia/Seoul') AS migrated_at,
    if(notEmpty(JSONExtractString(k.payload, 'source_table')), JSONExtractString(k.payload, 'source_table'), k.source) AS source_table
FROM `Data_Book_NAVER_Log`.kafka_book_events_queue AS k
WHERE length(ifNull(_error, '')) = 0
  AND k.event_type = 'book.marketplace.url.v1'
  AND notEmpty(JSONExtractString(k.payload, 'uuid'))
  AND notEmpty(JSONExtractString(k.payload, 'uuid_book'))
  AND notEmpty(JSONExtractString(k.payload, 'uuid_marketplace'));

ALTER TABLE `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_book_marketplace_url
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka book.marketplace.url.v1 events to Data_Book_NAVER_Raw.book_marketplace_url; provider-agnostic marketplace URL ingestion; OLAP 전용; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_shotalk_search_result
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Raw`.shotalk_search_result
AS
SELECT
    ifNull(toUUIDOrNull(if(notEmpty(JSONExtractString(k.payload, 'uuid')), JSONExtractString(k.payload, 'uuid'), k.event_uuid)), toUUID('00000000-0000-0000-0000-000000000000')) AS uuid,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'created_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(toString(k.created_at), 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS created_at,
    JSONExtractString(k.payload, 'q') AS q,
    toUInt16OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'req_limit'), ''), '0')) AS req_limit,
    toUInt32OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'req_page'), ''), '0')) AS req_page,
    toUInt16OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'item_rank'), ''), '0')) AS item_rank,
    JSONExtractString(k.payload, 'title') AS title,
    toUInt32OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'price'), ''), '0')) AS price,
    JSONExtractString(k.payload, 'photo_url') AS photo_url,
    JSONExtractString(k.payload, 'cp_code') AS cp_code,
    JSONExtractString(k.payload, 'cp_name') AS cp_name,
    JSONExtractString(k.payload, 'cp_icon_url') AS cp_icon_url,
    JSONExtractString(k.payload, 'commission_link') AS commission_link,
    if(
        toUInt64OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'version'), ''), '0')) = 0,
        toUInt64(toUnixTimestamp64Milli(now64(3, 'Asia/Seoul'))),
        toUInt64OrZero(ifNull(nullIf(JSONExtractRaw(k.payload, 'version'), ''), '0'))
    ) AS version
FROM `Data_Book_NAVER_Log`.kafka_book_events_queue AS k
WHERE length(ifNull(_error, '')) = 0
  AND k.event_type = 'book.shotalk.search_result.v1'
  AND notEmpty(JSONExtractString(k.payload, 'commission_link'));

ALTER TABLE `Data_Book_NAVER_Raw`.mv_kafka_book_events_to_shotalk_search_result
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka book.shotalk.search_result.v1 events to Data_Book_NAVER_Raw.shotalk_search_result; OLAP 전용; SSOT 아님';
