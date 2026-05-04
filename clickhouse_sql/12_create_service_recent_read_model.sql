/*
Add/fix Data_Book_NAVER_Service serving read models for statground_go book workbench.

Use this on existing clusters where Data_Book_NAVER_Raw already exists.
The 30-day recent table keeps the workbench recent-card query time-first and small,
while naver_book_latest remains the full ISBN-level latest read model for search/detail.
*/

CREATE DATABASE IF NOT EXISTS `Data_Book_NAVER_Service`
ON CLUSTER statground_cluster
COMMENT 'Data_Book_NAVER_Service; NAVER book application serving tables/read models; OLAP 전용; SSOT 아님; Asia/Seoul 기준';

CREATE DATABASE IF NOT EXISTS `Data_Book_NAVER_Service`
COMMENT 'Data_Book_NAVER_Service entrypoint database; NAVER Distributed serving read models; OLAP 전용; SSOT 아님; Asia/Seoul 기준';

CREATE TABLE IF NOT EXISTS `Data_Book_NAVER_Service`.naver_book_latest_local
ON CLUSTER statground_cluster
(
    isbn String COMMENT 'NAVER Book ISBN field; serving key for detail lookup',
    uuid UUID COMMENT 'Latest NAVER book row UUID',
    provider LowCardinality(String) DEFAULT 'naver' COMMENT 'Book data provider',
    version UInt64 COMMENT 'ReplacingMergeTree version from raw row; larger value wins',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'First-seen timestamp from latest selected raw row',
    updated_at DateTime64(3, 'Asia/Seoul') COMMENT 'Latest update timestamp from raw row',
    title String COMMENT 'NAVER Book title',
    link String COMMENT 'NAVER Book detail URL',
    image String COMMENT 'NAVER Book thumbnail image URL',
    author String COMMENT 'NAVER Book author field',
    discount Nullable(UInt32) COMMENT 'NAVER Book discount price',
    publisher String COMMENT 'NAVER Book publisher',
    description String COMMENT 'NAVER Book description',
    pubdate String COMMENT 'NAVER Book publication date string',
    source LowCardinality(String) COMMENT 'Producer source from raw row',
    ingested_at DateTime64(3, 'Asia/Seoul') COMMENT 'Serving row ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_Book_NAVER_Service/naver_book_latest_local', '{replica}', version)
ORDER BY isbn
SETTINGS index_granularity = 8192
COMMENT 'Latest NAVER Book serving local replicated table keyed by ISBN; maintained from Data_Book_NAVER_Raw.naver_book_raw_local; OLAP read model';

CREATE TABLE IF NOT EXISTS `Data_Book_NAVER_Service`.naver_book_latest
ON CLUSTER statground_cluster
(
    isbn String COMMENT 'NAVER Book ISBN field; serving key for detail lookup',
    uuid UUID COMMENT 'Latest NAVER book row UUID',
    provider LowCardinality(String) DEFAULT 'naver' COMMENT 'Book data provider',
    version UInt64 COMMENT 'ReplacingMergeTree version from raw row; larger value wins',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'First-seen timestamp from latest selected raw row',
    updated_at DateTime64(3, 'Asia/Seoul') COMMENT 'Latest update timestamp from raw row',
    title String COMMENT 'NAVER Book title',
    link String COMMENT 'NAVER Book detail URL',
    image String COMMENT 'NAVER Book thumbnail image URL',
    author String COMMENT 'NAVER Book author field',
    discount Nullable(UInt32) COMMENT 'NAVER Book discount price',
    publisher String COMMENT 'NAVER Book publisher',
    description String COMMENT 'NAVER Book description',
    pubdate String COMMENT 'NAVER Book publication date string',
    source LowCardinality(String) COMMENT 'Producer source from raw row',
    ingested_at DateTime64(3, 'Asia/Seoul') COMMENT 'Serving row ingestion timestamp'
)
ENGINE = Distributed('statground_cluster', 'Data_Book_NAVER_Service', 'naver_book_latest_local', cityHash64(isbn))
COMMENT 'Latest NAVER Book serving distributed table; query this table from applications instead of aggregating raw rows';

CREATE TABLE IF NOT EXISTS `Data_Book_NAVER_Service`.naver_book_recent_local
ON CLUSTER statground_cluster
(
    isbn String COMMENT 'NAVER Book ISBN field; repeated ISBNs are allowed across collection times',
    uuid UUID COMMENT 'NAVER book row UUID from raw row',
    provider LowCardinality(String) DEFAULT 'naver' COMMENT 'Book data provider',
    version UInt64 COMMENT 'Raw row version from updated_at Unix milliseconds; larger value wins for identical updated_at/isbn rows',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'First-seen timestamp from raw row',
    updated_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collection/update timestamp; ORDER BY leading column for recent-list reads',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Actual NAVER API collection timestamp',
    title String COMMENT 'NAVER Book title',
    link String COMMENT 'NAVER Book detail URL',
    image String COMMENT 'NAVER Book thumbnail image URL',
    author String COMMENT 'NAVER Book author field',
    discount Nullable(UInt32) COMMENT 'NAVER Book discount price',
    publisher String COMMENT 'NAVER Book publisher',
    pubdate String COMMENT 'NAVER Book publication date string',
    search_mode LowCardinality(String) COMMENT 'Crawler search mode dimension',
    search_query String COMMENT 'Search query submitted to NAVER API',
    search_sort LowCardinality(String) COMMENT 'NAVER API sort parameter',
    source LowCardinality(String) COMMENT 'Producer source from raw row',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'Serving row ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_Book_NAVER_Service/naver_book_recent_local', '{replica}', version)
PARTITION BY toYYYYMM(updated_at)
ORDER BY (updated_at, isbn)
TTL updated_at + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192
COMMENT 'Small time-first NAVER Book recent collection serving table for workbench cards; stores slim 30-day rows and avoids scanning all latest ISBNs';

CREATE TABLE IF NOT EXISTS `Data_Book_NAVER_Service`.naver_book_recent
ON CLUSTER statground_cluster
(
    isbn String COMMENT 'NAVER Book ISBN field; repeated ISBNs are allowed across collection times',
    uuid UUID COMMENT 'NAVER book row UUID from raw row',
    provider LowCardinality(String) DEFAULT 'naver' COMMENT 'Book data provider',
    version UInt64 COMMENT 'Raw row version from updated_at Unix milliseconds; larger value wins for identical updated_at/isbn rows',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'First-seen timestamp from raw row',
    updated_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collection/update timestamp; distributed read field',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Actual NAVER API collection timestamp',
    title String COMMENT 'NAVER Book title',
    link String COMMENT 'NAVER Book detail URL',
    image String COMMENT 'NAVER Book thumbnail image URL',
    author String COMMENT 'NAVER Book author field',
    discount Nullable(UInt32) COMMENT 'NAVER Book discount price',
    publisher String COMMENT 'NAVER Book publisher',
    pubdate String COMMENT 'NAVER Book publication date string',
    search_mode LowCardinality(String) COMMENT 'Crawler search mode dimension',
    search_query String COMMENT 'Search query submitted to NAVER API',
    search_sort LowCardinality(String) COMMENT 'NAVER API sort parameter',
    source LowCardinality(String) COMMENT 'Producer source from raw row',
    ingested_at DateTime64(3, 'Asia/Seoul') COMMENT 'Serving row ingestion timestamp'
)
ENGINE = Distributed('statground_cluster', 'Data_Book_NAVER_Service', 'naver_book_recent_local', cityHash64(isbn))
COMMENT 'Recent NAVER Book collection distributed serving table for application cards; use this for recent-list endpoints instead of naver_book_latest';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Service`.mv_naver_book_raw_to_latest_local
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Service`.naver_book_latest_local
AS
SELECT
    isbn,
    uuid,
    provider,
    version,
    created_at,
    updated_at,
    title,
    link,
    image,
    author,
    discount,
    publisher,
    description,
    pubdate,
    source,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Book_NAVER_Raw`.naver_book_raw_local
WHERE notEmpty(isbn);

ALTER TABLE `Data_Book_NAVER_Service`.mv_naver_book_raw_to_latest_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view maintaining latest NAVER Book serving local table from raw local inserts; ReplacingMergeTree(version) resolves repeated ISBN rows';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Book_NAVER_Service`.mv_naver_book_raw_to_recent_local
ON CLUSTER statground_cluster
TO `Data_Book_NAVER_Service`.naver_book_recent_local
AS
SELECT
    isbn,
    uuid,
    provider,
    version,
    created_at,
    updated_at,
    collected_at,
    title,
    link,
    image,
    author,
    discount,
    publisher,
    pubdate,
    search_mode,
    search_query,
    search_sort,
    source,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Book_NAVER_Raw`.naver_book_raw_local
WHERE notEmpty(isbn);

ALTER TABLE `Data_Book_NAVER_Service`.mv_naver_book_raw_to_recent_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view maintaining slim time-first recent collection rows for application workbench cards; 30-day TTL keeps this read model small';

INSERT INTO `Data_Book_NAVER_Service`.naver_book_recent
(
    isbn,
    uuid,
    provider,
    version,
    created_at,
    updated_at,
    collected_at,
    title,
    link,
    image,
    author,
    discount,
    publisher,
    pubdate,
    search_mode,
    search_query,
    search_sort,
    source,
    ingested_at
)
SELECT
    isbn,
    uuid,
    provider,
    version,
    created_at,
    updated_at,
    collected_at,
    title,
    link,
    image,
    author,
    discount,
    publisher,
    pubdate,
    search_mode,
    search_query,
    search_sort,
    source,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Book_NAVER_Raw`.naver_book_raw
WHERE notEmpty(isbn)
  AND updated_at >= now64(3, 'Asia/Seoul') - INTERVAL 30 DAY
  AND (updated_at, isbn, version) GLOBAL NOT IN
  (
      SELECT updated_at, isbn, version
      FROM `Data_Book_NAVER_Service`.naver_book_recent
      WHERE updated_at >= now64(3, 'Asia/Seoul') - INTERVAL 30 DAY
  );

GRANT SELECT ON `Data_Book_NAVER_Service`.naver_book_latest TO statground_ch_app;
GRANT SELECT ON `Data_Book_NAVER_Service`.naver_book_recent TO statground_ch_app;

GRANT SELECT ON `Data_Book_NAVER_Service`.naver_book_latest TO trino_ch_statground;
GRANT SELECT ON `Data_Book_NAVER_Service`.naver_book_recent TO trino_ch_statground;
