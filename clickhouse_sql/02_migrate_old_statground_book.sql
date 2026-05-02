/*
Migration from old ClickHouse database statground_book to new Data_Book_NAVER_* schema.

Run after:
  01_create_data_book_naver_on_cluster.sql

Included:
  - statground_book.raw_naver -> Data_Book_NAVER_Raw.naver_book_raw
  - statground_book.naver_commision_link_shotalk -> Data_Book_NAVER_Raw.naver_commission_link_shotalk

Separate extended migration:
  - statground_book.backup_v_book_list_all -> Data_Book_NAVER_Raw.legacy_book_catalog
  - statground_book.backup_marketplace_url -> Data_Book_NAVER_Raw.book_marketplace_url
  - statground_book.raw_aladin_publisher_cache -> Data_Book_NAVER_Log.aladin_publisher_cache
  - log.log_shotalk_search -> Data_Book_NAVER_Raw.shotalk_search_result
  Run 05_migrate_old_book_extended_local.sql or 05b_migrate_old_book_extended_remote_template.sql after this script.

Idempotence:
  - raw_naver migration skips UUIDs already present in Data_Book_NAVER_Raw.naver_book_raw.
  - Shotalk migration skips UUIDs already present in Data_Book_NAVER_Raw.naver_commission_link_shotalk.
  - Do not repeatedly run full INSERT without the NOT IN guard because mart AggregateFunction tables count inserted rows.
*/

SELECT
    'old raw_naver' AS target,
    count() AS rows,
    uniqExact(isbn) AS uniq_isbn
FROM `statground_book`.raw_naver;

SELECT
    'new naver_book_raw before' AS target,
    count() AS rows,
    uniqExact(isbn) AS uniq_isbn
FROM `Data_Book_NAVER_Raw`.naver_book_raw;

INSERT INTO `Data_Book_NAVER_Raw`.naver_book_raw
(
    uuid,
    provider,
    version,
    created_at,
    created_log,
    updated_at,
    updated_log,
    collected_at,
    title,
    link,
    image,
    author,
    discount,
    publisher,
    isbn,
    description,
    pubdate,
    search_mode,
    search_query,
    search_sort,
    search_start,
    search_display,
    api_total,
    source,
    event_uuid,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    payload,
    ingested_at
)
SELECT
    uuid AS uuid,
    'naver' AS provider,
    toUInt64(version) AS version,
    created_at AS created_at,
    created_log AS created_log,
    updated_at AS updated_at,
    updated_log AS updated_log,
    updated_at AS collected_at,
    title AS title,
    link AS link,
    image AS image,
    author AS author,
    discount AS discount,
    publisher AS publisher,
    isbn AS isbn,
    description AS description,
    pubdate AS pubdate,
    'migration_old_raw_naver' AS search_mode,
    '' AS search_query,
    '' AS search_sort,
    toUInt16(0) AS search_start,
    toUInt16(0) AS search_display,
    toUInt32(0) AS api_total,
    'clickhouse_migration' AS source,
    uuid AS event_uuid,
    'migration.statground_book.raw_naver' AS kafka_topic,
    toUInt32(0) AS kafka_partition,
    toUInt64(0) AS kafka_offset,
    toJSONString(map(
        'uuid', toString(uuid),
        'version', toString(version),
        'created_at', formatDateTime(created_at, '%Y-%m-%d %H:%i:%S.%f', 'Asia/Seoul'),
        'created_log', created_log,
        'updated_at', formatDateTime(updated_at, '%Y-%m-%d %H:%i:%S.%f', 'Asia/Seoul'),
        'updated_log', updated_log,
        'title', title,
        'link', link,
        'image', image,
        'author', author,
        'discount', ifNull(toString(discount), ''),
        'publisher', publisher,
        'isbn', isbn,
        'description', description,
        'pubdate', pubdate,
        'source', 'clickhouse_migration',
        'search_mode', 'migration_old_raw_naver'
    )) AS payload,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `statground_book`.raw_naver FINAL
WHERE notEmpty(isbn)
  AND uuid NOT IN (
      SELECT uuid
      FROM `Data_Book_NAVER_Raw`.naver_book_raw
  );

SELECT
    'new naver_book_raw after' AS target,
    count() AS rows,
    uniqExact(isbn) AS uniq_isbn
FROM `Data_Book_NAVER_Raw`.naver_book_raw;

SELECT
    'old naver_commision_link_shotalk' AS target,
    count() AS rows
FROM `statground_book`.naver_commision_link_shotalk;

SELECT
    'new naver_commission_link_shotalk before' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.naver_commission_link_shotalk;

INSERT INTO `Data_Book_NAVER_Raw`.naver_commission_link_shotalk
(
    uuid,
    created_at,
    updated_at,
    isbn,
    marketplace_name,
    marketplace_url,
    url_sha256,
    api_success,
    api_message,
    data_status,
    cp_code,
    cp_name,
    cp_icon,
    commission_per,
    product_name,
    product_price_raw,
    product_price_int,
    product_img,
    commission_link,
    error
)
SELECT
    uuid,
    created_at,
    updated_at,
    isbn,
    marketplace_name,
    marketplace_url,
    url_sha256,
    api_success,
    api_message,
    data_status,
    cp_code,
    cp_name,
    cp_icon,
    commission_per,
    product_name,
    product_price_raw,
    product_price_int,
    product_img,
    commission_link,
    error
FROM `statground_book`.naver_commision_link_shotalk FINAL
WHERE uuid NOT IN (
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.naver_commission_link_shotalk
);

SELECT
    'new naver_commission_link_shotalk after' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.naver_commission_link_shotalk;

/*
Optional post-migration check:
The statistics materialized views should have received inserted raw rows if 01_create_data_book_naver_on_cluster.sql was executed before this migration.
*/
SELECT
    period_start,
    provider,
    search_mode,
    countMerge(rows_state) AS rows,
    uniqCombined64Merge(isbn_uniq_state) AS uniq_isbn,
    uniqCombined64Merge(author_uniq_state) AS uniq_author,
    uniqCombined64Merge(publisher_uniq_state) AS uniq_publisher
FROM `Data_Book_NAVER_Mart`.naver_book_stats_daily
WHERE search_mode = 'migration_old_raw_naver'
GROUP BY
    period_start,
    provider,
    search_mode
ORDER BY period_start DESC
LIMIT 30;
