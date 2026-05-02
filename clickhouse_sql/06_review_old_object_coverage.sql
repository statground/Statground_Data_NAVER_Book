/*
Coverage review for old book-related objects known from the uploaded files and follow-up DDL.
Run against the OLD ClickHouse connection to confirm which source tables exist before migration.
*/

SELECT
    database,
    name AS table,
    engine,
    total_rows,
    comment
FROM system.tables
WHERE (database = 'statground_book' AND name IN (
        'raw_naver',
        'naver_commision_link_shotalk',
        'backup_v_book_list_all',
        'backup_marketplace_url',
        'raw_aladin_publisher_cache'
    ))
   OR (database = 'log' AND name = 'log_shotalk_search')
ORDER BY database, name;

/*
Expected target mapping:
- statground_book.raw_naver                    -> Data_Book_NAVER_Raw.naver_book_raw
- statground_book.naver_commision_link_shotalk -> Data_Book_NAVER_Raw.naver_commission_link_shotalk
- statground_book.backup_v_book_list_all       -> Data_Book_NAVER_Raw.legacy_book_catalog
- statground_book.backup_marketplace_url       -> Data_Book_NAVER_Raw.book_marketplace_url
- statground_book.raw_aladin_publisher_cache   -> Data_Book_NAVER_Log.aladin_publisher_cache
- log.log_shotalk_search                       -> Data_Book_NAVER_Raw.shotalk_search_result
*/
