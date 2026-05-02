/*
Optional transition copy from previously-created Data_Prediction_Book_* databases to Data_Book_NAVER_*.

Run order when v4 Data_Prediction_Book_* already contains data:
1. 10_disable_old_data_prediction_book_kafka_objects.sql
2. 01_create_data_book_naver_on_cluster.sql
3. This script
4. 03_verify_data_book_naver.sql

Notes:
- This script does NOT copy Data_Prediction_Book_Mart directly.
- Data_Book_NAVER_Mart is populated by the naver_book_raw materialized views when new raw rows are copied.
- GLOBAL NOT IN is used to avoid double-distributed subquery issues on a cluster.
- Re-running this script is intended to be idempotent for tables that have UUID/event keys.
*/

SELECT
    'old Data_Prediction_Book_Log.book_events before copy' AS target,
    count() AS rows
FROM `Data_Prediction_Book_Log`.book_events;

SELECT
    'new Data_Book_NAVER_Log.book_events before copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Log`.book_events;

INSERT INTO `Data_Book_NAVER_Log`.book_events
SELECT *
FROM `Data_Prediction_Book_Log`.book_events
WHERE event_uuid GLOBAL NOT IN
(
    SELECT event_uuid
    FROM `Data_Book_NAVER_Log`.book_events
);

SELECT
    'new Data_Book_NAVER_Log.book_events after copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Log`.book_events;

SELECT
    'old Data_Prediction_Book_Raw.naver_book_raw before copy' AS target,
    count() AS rows
FROM `Data_Prediction_Book_Raw`.naver_book_raw;

SELECT
    'new Data_Book_NAVER_Raw.naver_book_raw before copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.naver_book_raw;

INSERT INTO `Data_Book_NAVER_Raw`.naver_book_raw
SELECT *
FROM `Data_Prediction_Book_Raw`.naver_book_raw
WHERE uuid GLOBAL NOT IN
(
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.naver_book_raw
);

SELECT
    'new Data_Book_NAVER_Raw.naver_book_raw after copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.naver_book_raw;

SELECT
    'old Data_Prediction_Book_Log.naver_collect_log before copy' AS target,
    count() AS rows
FROM `Data_Prediction_Book_Log`.naver_collect_log;

SELECT
    'new Data_Book_NAVER_Log.naver_collect_log before copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Log`.naver_collect_log;

INSERT INTO `Data_Book_NAVER_Log`.naver_collect_log
SELECT *
FROM `Data_Prediction_Book_Log`.naver_collect_log
WHERE event_uuid GLOBAL NOT IN
(
    SELECT event_uuid
    FROM `Data_Book_NAVER_Log`.naver_collect_log
);

SELECT
    'new Data_Book_NAVER_Log.naver_collect_log after copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Log`.naver_collect_log;

SELECT
    'old Data_Prediction_Book_Raw.naver_commission_link_shotalk before copy' AS target,
    count() AS rows
FROM `Data_Prediction_Book_Raw`.naver_commission_link_shotalk;

SELECT
    'new Data_Book_NAVER_Raw.naver_commission_link_shotalk before copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.naver_commission_link_shotalk;

INSERT INTO `Data_Book_NAVER_Raw`.naver_commission_link_shotalk
SELECT *
FROM `Data_Prediction_Book_Raw`.naver_commission_link_shotalk
WHERE uuid GLOBAL NOT IN
(
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.naver_commission_link_shotalk
);

SELECT
    'new Data_Book_NAVER_Raw.naver_commission_link_shotalk after copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.naver_commission_link_shotalk;

SELECT
    'old Data_Prediction_Book_Raw.legacy_book_catalog before copy' AS target,
    count() AS rows
FROM `Data_Prediction_Book_Raw`.legacy_book_catalog;

SELECT
    'new Data_Book_NAVER_Raw.legacy_book_catalog before copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.legacy_book_catalog;

INSERT INTO `Data_Book_NAVER_Raw`.legacy_book_catalog
SELECT *
FROM `Data_Prediction_Book_Raw`.legacy_book_catalog
WHERE uuid GLOBAL NOT IN
(
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.legacy_book_catalog
);

SELECT
    'new Data_Book_NAVER_Raw.legacy_book_catalog after copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.legacy_book_catalog;

SELECT
    'old Data_Prediction_Book_Raw.book_marketplace_url before copy' AS target,
    count() AS rows
FROM `Data_Prediction_Book_Raw`.book_marketplace_url;

SELECT
    'new Data_Book_NAVER_Raw.book_marketplace_url before copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.book_marketplace_url;

INSERT INTO `Data_Book_NAVER_Raw`.book_marketplace_url
SELECT *
FROM `Data_Prediction_Book_Raw`.book_marketplace_url
WHERE uuid GLOBAL NOT IN
(
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.book_marketplace_url
);

SELECT
    'new Data_Book_NAVER_Raw.book_marketplace_url after copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.book_marketplace_url;

SELECT
    'old Data_Prediction_Book_Log.aladin_publisher_cache before copy' AS target,
    count() AS rows
FROM `Data_Prediction_Book_Log`.aladin_publisher_cache;

SELECT
    'new Data_Book_NAVER_Log.aladin_publisher_cache before copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Log`.aladin_publisher_cache;

INSERT INTO `Data_Book_NAVER_Log`.aladin_publisher_cache
SELECT *
FROM `Data_Prediction_Book_Log`.aladin_publisher_cache
WHERE (publisher, collected_at, detected_last_page, run_uuid, source) GLOBAL NOT IN
(
    SELECT
        publisher,
        collected_at,
        detected_last_page,
        run_uuid,
        source
    FROM `Data_Book_NAVER_Log`.aladin_publisher_cache
);

SELECT
    'new Data_Book_NAVER_Log.aladin_publisher_cache after copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Log`.aladin_publisher_cache;

SELECT
    'old Data_Prediction_Book_Raw.shotalk_search_result before copy' AS target,
    count() AS rows
FROM `Data_Prediction_Book_Raw`.shotalk_search_result;

SELECT
    'new Data_Book_NAVER_Raw.shotalk_search_result before copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.shotalk_search_result;

INSERT INTO `Data_Book_NAVER_Raw`.shotalk_search_result
SELECT *
FROM `Data_Prediction_Book_Raw`.shotalk_search_result
WHERE uuid GLOBAL NOT IN
(
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.shotalk_search_result
);

SELECT
    'new Data_Book_NAVER_Raw.shotalk_search_result after copy' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.shotalk_search_result;

SELECT
    'Data_Book_NAVER_Mart.naver_book_stats_daily after raw copy' AS target,
    toDate(period_start) AS day,
    provider,
    search_mode,
    countMerge(rows_state) AS rows,
    uniqCombined64Merge(isbn_uniq_state) AS uniq_isbn
FROM `Data_Book_NAVER_Mart`.naver_book_stats_daily
GROUP BY
    day,
    provider,
    search_mode
ORDER BY day DESC, rows DESC
LIMIT 20;
