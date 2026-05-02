/*
Optional mart rebuild from Data_Book_NAVER_Raw.naver_book_raw.

Use only if:
- raw rows were inserted before the mart materialized views existed, or
- you intentionally want to rebuild the hourly/daily aggregate states from current raw rows.

This script truncates mart local aggregate tables and recalculates from the distributed raw table.
It does not change raw/log tables.
*/

TRUNCATE TABLE `Data_Book_NAVER_Mart`.naver_book_stats_hourly_local
ON CLUSTER statground_cluster;

TRUNCATE TABLE `Data_Book_NAVER_Mart`.naver_book_stats_daily_local
ON CLUSTER statground_cluster;

INSERT INTO `Data_Book_NAVER_Mart`.naver_book_stats_hourly
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
FROM `Data_Book_NAVER_Raw`.naver_book_raw
GROUP BY
    period_start,
    provider,
    search_mode;

INSERT INTO `Data_Book_NAVER_Mart`.naver_book_stats_daily
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
FROM `Data_Book_NAVER_Raw`.naver_book_raw
GROUP BY
    period_start,
    provider,
    search_mode;

SELECT
    'rebuilt hourly mart' AS target,
    count() AS aggregate_rows
FROM `Data_Book_NAVER_Mart`.naver_book_stats_hourly;

SELECT
    'rebuilt daily mart' AS target,
    count() AS aggregate_rows
FROM `Data_Book_NAVER_Mart`.naver_book_stats_daily;
