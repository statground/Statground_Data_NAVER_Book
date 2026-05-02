/*
Remote migration template for the old NAVER/Shotalk tables.

Use this when the old ClickHouse is a different DBeaver connection/server.
Edit every __OLD_*__ placeholder before execution.

Important:
- ClickHouse remote() uses the old server native TCP endpoint, not necessarily the HTTP/JDBC endpoint.
- If the old external port is HTTP only, export from old and import into new instead of this template.
*/

SELECT
    'old remote statground_book.raw_naver' AS target,
    count() AS rows,
    uniqExact(isbn) AS uniq_isbn
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'raw_naver', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__');

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
    'migration_old_raw_naver_remote' AS search_mode,
    '' AS search_query,
    '' AS search_sort,
    toUInt16(0) AS search_start,
    toUInt16(0) AS search_display,
    toUInt32(0) AS api_total,
    'clickhouse_remote_migration' AS source,
    uuid AS event_uuid,
    'remote.statground_book.raw_naver' AS kafka_topic,
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
        'source', 'clickhouse_remote_migration',
        'search_mode', 'migration_old_raw_naver_remote'
    )) AS payload,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'raw_naver', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__')
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
    'old remote statground_book.naver_commision_link_shotalk' AS target,
    count() AS rows
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'naver_commision_link_shotalk', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__');

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
FROM remote('__OLD_CLICKHOUSE_NATIVE_HOST_PORT__', 'statground_book', 'naver_commision_link_shotalk', '__OLD_CLICKHOUSE_USER__', '__OLD_CLICKHOUSE_PASSWORD__')
WHERE uuid NOT IN (
    SELECT uuid
    FROM `Data_Book_NAVER_Raw`.naver_commission_link_shotalk
);

SELECT
    'new naver_commission_link_shotalk after' AS target,
    count() AS rows
FROM `Data_Book_NAVER_Raw`.naver_commission_link_shotalk;
