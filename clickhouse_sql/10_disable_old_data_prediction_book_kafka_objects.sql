/*
Optional transition helper for environments where v4 Data_Prediction_Book_* objects were already created.

Purpose:
- Stop the old Data_Prediction_Book_* Kafka consumers before switching production ingestion to Data_Book_NAVER_*.
- Keeps old target data tables intact for verification or copy.

Why not only RENAME DATABASE?
- ClickHouse supports RENAME DATABASE for Atomic databases, but this Statground schema contains Distributed engine arguments and explicit ReplicatedMergeTree Keeper paths with the old database name embedded as strings.
- For long-term maintainability, create Data_Book_NAVER_* with the corrected DDL and copy data instead of relying on a metadata rename.
*/

DROP TABLE IF EXISTS `Data_Prediction_Book_Log`.mv_kafka_book_events_to_book_events
ON CLUSTER statground_cluster;

DROP TABLE IF EXISTS `Data_Prediction_Book_Raw`.mv_kafka_book_events_to_naver_book_raw
ON CLUSTER statground_cluster;

DROP TABLE IF EXISTS `Data_Prediction_Book_Log`.mv_kafka_book_events_to_naver_collect_log
ON CLUSTER statground_cluster;

DROP TABLE IF EXISTS `Data_Prediction_Book_Log`.mv_kafka_book_events_to_aladin_publisher_cache
ON CLUSTER statground_cluster;

DROP TABLE IF EXISTS `Data_Prediction_Book_Raw`.mv_kafka_book_events_to_book_marketplace_url
ON CLUSTER statground_cluster;

DROP TABLE IF EXISTS `Data_Prediction_Book_Raw`.mv_kafka_book_events_to_shotalk_search_result
ON CLUSTER statground_cluster;

DROP TABLE IF EXISTS `Data_Prediction_Book_Log`.kafka_book_events_queue
ON CLUSTER statground_cluster;
