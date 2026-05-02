/*
Remote migration template for non-NAVER-only / cache / Shotalk old tables.

Edit every __OLD_*__ placeholder before execution.
ClickHouse remote() uses the old server native TCP endpoint.
*/

SELECT
    'old remote statground_book.backup_v_book_list_all' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'backup_v_book_list_all', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__');

SELECT
    'new Data_Book_NAVER_Raw.legacy_book_catalog before' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid
FROM `Data_Book_NAVER_Raw`.legacy_book_catalog;

INSERT INTO `Data_Book_NAVER_Raw`.legacy_book_catalog
(
    rnum,
    uuid,
    dedupe_key,
    isbn13,
    isbn10,
    oclc,
    lccn,
    olid,
    google_id,
    naver_id,
    title,
    subtitle,
    authors,
    publisher,
    published_date,
    publish_year,
    language,
    description,
    subjects,
    categories,
    page_count,
    cover_url,
    info_url,
    source_primary,
    source_list,
    raw_json,
    raw_json_hash,
    last_query,
    last_query_at,
    active,
    created_at,
    updated_at,
    last_seen_at,
    created_log,
    updated_log,
    table_of_contents,
    price,
    migrated_at,
    source_table
)
SELECT
    rnum,
    uuid,
    dedupe_key,
    isbn13,
    isbn10,
    oclc,
    lccn,
    olid,
    google_id,
    naver_id,
    title,
    subtitle,
    authors,
    publisher,
    published_date,
    publish_year,
    language,
    description,
    subjects,
    categories,
    page_count,
    cover_url,
    info_url,
    source_primary,
    source_list,
    toString(raw_json) AS raw_json,
    raw_json_hash,
    last_query,
    last_query_at,
    active,
    created_at,
    updated_at,
    last_seen_at,
    toString(created_log) AS created_log,
    toString(updated_log) AS updated_log,
    table_of_contents,
    price,
    now64(3, 'Asia/Seoul') AS migrated_at,
    'remote.statground_book.backup_v_book_list_all' AS source_table
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'backup_v_book_list_all', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__')
WHERE uuid NOT IN (
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.legacy_book_catalog
);

SELECT
    'new Data_Book_NAVER_Raw.legacy_book_catalog after' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(source_primary) AS source_primary_count
FROM `Data_Book_NAVER_Raw`.legacy_book_catalog;

SELECT
    'old remote statground_book.backup_marketplace_url' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(uuid_book) AS uniq_book_uuid
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'backup_marketplace_url', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__');

SELECT
    'new Data_Book_NAVER_Raw.book_marketplace_url before' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid
FROM `Data_Book_NAVER_Raw`.book_marketplace_url;

INSERT INTO `Data_Book_NAVER_Raw`.book_marketplace_url
(
    uuid,
    uuid_book,
    uuid_marketplace,
    url,
    active,
    uuid_affiliate,
    created_at,
    created_log,
    updated_at,
    updated_log,
    type,
    migrated_at,
    source_table
)
SELECT
    uuid,
    uuid_book,
    uuid_marketplace,
    url,
    active,
    uuid_affiliate,
    created_at,
    toString(created_log) AS created_log,
    updated_at,
    toString(updated_log) AS updated_log,
    type,
    now64(3, 'Asia/Seoul') AS migrated_at,
    'remote.statground_book.backup_marketplace_url' AS source_table
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'backup_marketplace_url', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__')
WHERE uuid NOT IN (
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.book_marketplace_url
);

SELECT
    'new Data_Book_NAVER_Raw.book_marketplace_url after' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(uuid_book) AS uniq_book_uuid
FROM `Data_Book_NAVER_Raw`.book_marketplace_url;

SELECT
    'old remote statground_book.raw_aladin_publisher_cache' AS target,
    count() AS rows,
    uniqExact(publisher) AS uniq_publisher,
    max(detected_last_page) AS max_detected_last_page
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'raw_aladin_publisher_cache', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__');

SELECT
    'new Data_Book_NAVER_Log.aladin_publisher_cache before' AS target,
    count() AS rows,
    uniqExact(publisher) AS uniq_publisher,
    max(detected_last_page) AS max_detected_last_page
FROM `Data_Book_NAVER_Log`.aladin_publisher_cache;

INSERT INTO `Data_Book_NAVER_Log`.aladin_publisher_cache
(
    publisher,
    collected_at,
    detected_last_page,
    run_uuid,
    source
)
SELECT
    publisher,
    collected_at,
    detected_last_page,
    run_uuid,
    source
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'raw_aladin_publisher_cache', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__')
WHERE (publisher, collected_at, detected_last_page, run_uuid) NOT IN (
    SELECT
        publisher,
        collected_at,
        detected_last_page,
        run_uuid
    FROM `Data_Book_NAVER_Log`.aladin_publisher_cache
);

SELECT
    'new Data_Book_NAVER_Log.aladin_publisher_cache after' AS target,
    count() AS rows,
    uniqExact(publisher) AS uniq_publisher,
    max(detected_last_page) AS max_detected_last_page
FROM `Data_Book_NAVER_Log`.aladin_publisher_cache;

SELECT
    'old remote log.log_shotalk_search' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(commission_link) AS uniq_commission_link
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'log', 'log_shotalk_search', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__');

SELECT
    'new Data_Book_NAVER_Raw.shotalk_search_result before' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(commission_link) AS uniq_commission_link
FROM `Data_Book_NAVER_Raw`.shotalk_search_result;

INSERT INTO `Data_Book_NAVER_Raw`.shotalk_search_result
(
    uuid,
    created_at,
    q,
    req_limit,
    req_page,
    item_rank,
    title,
    price,
    photo_url,
    cp_code,
    cp_name,
    cp_icon_url,
    commission_link,
    version
)
SELECT
    uuid,
    created_at,
    q,
    req_limit,
    req_page,
    item_rank,
    title,
    price,
    photo_url,
    cp_code,
    cp_name,
    cp_icon_url,
    commission_link,
    version
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'log', 'log_shotalk_search', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__')
WHERE uuid NOT IN (
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.shotalk_search_result
);

SELECT
    'new Data_Book_NAVER_Raw.shotalk_search_result after' AS target,
    count() AS rows,
    uniqExact(uuid) AS uniq_uuid,
    uniqExact(commission_link) AS uniq_commission_link
FROM `Data_Book_NAVER_Raw`.shotalk_search_result;
