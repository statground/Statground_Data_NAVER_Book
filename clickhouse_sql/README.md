# Statground Data_Book_NAVER ClickHouse SQL

Execution target: ClickHouse 26.1.2.11 / DBeaver `migration_admin`.
Cluster: `statground_cluster`.

## Naming decision

The correct namespace for this repository is provider-scoped book data:

- `Data_Book_NAVER_Log`
- `Data_Book_NAVER_Raw`
- `Data_Book_NAVER_Mart`
- `Data_Book_NAVER_Service`

This is **not** a prediction-domain schema. It is a NAVER book provider schema under the `Data_Book` domain.

The Kafka topic remains `book.events` because the current Kafka platform already creates `book.events` and `book.dlq.events`, while auto-create topics are disabled. Provider separation is handled by ClickHouse DB namespace and event type filters such as `book.naver.raw.v1`.

The ClickHouse consumer group is:

```text
clickhouse_data_book_naver_events_v1
```

## Standard fresh execution order

1. `01_create_data_book_naver_on_cluster.sql`
   - Creates `Data_Book_NAVER_Log`, `Data_Book_NAVER_Raw`, `Data_Book_NAVER_Mart`, `Data_Book_NAVER_Service` on `statground_cluster`.
   - Creates Replicated local tables, Distributed entry tables, Kafka queue, Kafka parse-error tables, and Materialized Views.
   - Includes NAVER raw/log tables plus support/legacy tables currently needed by this NAVER book repository:
     - `Data_Book_NAVER_Raw.legacy_book_catalog`
     - `Data_Book_NAVER_Raw.book_marketplace_url`
     - `Data_Book_NAVER_Log.aladin_publisher_cache`
     - `Data_Book_NAVER_Log.book_kafka_parse_error`
     - `Data_Book_NAVER_Raw.shotalk_search_result`
   - GitHub Actions only publishes Kafka events; statistics are stored in ClickHouse mart tables.
   - Application serving tables are kept under `Data_Book_NAVER_Service`:
     - `Data_Book_NAVER_Service.naver_book_latest`
     - `Data_Book_NAVER_Service.naver_book_recent`

2. `02_migrate_old_statground_book.sql`
   - Local migration for:
     - `statground_book.raw_naver -> Data_Book_NAVER_Raw.naver_book_raw`
     - `statground_book.naver_commision_link_shotalk -> Data_Book_NAVER_Raw.naver_commission_link_shotalk`
   - Use this if the old `statground_book` database is visible from the same ClickHouse connection.

3. `02b_migrate_old_statground_book_remote_template.sql`
   - Remote migration template for the two tables above.
   - Use this if the old ClickHouse is a separate server/connection.
   - Edit `__OLD_CLICKHOUSE_NATIVE_HOST_PORT__`, `__OLD_CLICKHOUSE_USER__`, and `__OLD_CLICKHOUSE_PASSWORD__` before execution.

4. `05_migrate_old_book_extended_local.sql`
   - Local migration for:
     - `statground_book.backup_v_book_list_all -> Data_Book_NAVER_Raw.legacy_book_catalog`
     - `statground_book.backup_marketplace_url -> Data_Book_NAVER_Raw.book_marketplace_url`
     - `statground_book.raw_aladin_publisher_cache -> Data_Book_NAVER_Log.aladin_publisher_cache`
     - `log.log_shotalk_search -> Data_Book_NAVER_Raw.shotalk_search_result`

5. `05b_migrate_old_book_extended_remote_template.sql`
   - Remote migration template for the four extended tables above.
   - Edit the same `__OLD_*__` placeholders before execution.

6. `03_verify_data_book_naver.sql`
   - Basic row-count, cluster, mart, legacy, cache, Shotalk, and Kafka parse-error verification queries.

7. `06_review_old_object_coverage.sql`
   - Run on the OLD ClickHouse connection to confirm old source object coverage before migration.

## If v3 `Data_Book_*` was already created

Do not rely on a simple database rename for the production schema. This schema embeds DB names inside `Distributed(...)` engine arguments and explicit `Replicated*MergeTree` Keeper paths, so a simple rename leaves long-term maintenance risks.

Recommended order:

1. `07_disable_old_data_book_kafka_objects.sql`
   - Stops old `Data_Book_*` Kafka consumers by dropping only the old Kafka queue and Kafka-fed MVs.
   - Keeps old target tables intact.

2. `01_create_data_book_naver_on_cluster.sql`
   - Creates the corrected NAVER DB/table/MV layout.

3. `08_copy_data_book_to_data_book_naver_if_v3_exists.sql`
   - Copies existing v3 `Data_Book_*` data into `Data_Book_NAVER_*`.
   - Does not copy old mart tables directly; mart rows are generated from copied `naver_book_raw` rows.

4. `03_verify_data_book_naver.sql`

5. Optional: `09_rebuild_data_book_naver_mart_from_raw.sql`
   - Use only when raw rows existed before mart MVs were created or when you intentionally want a mart rebuild.

6. Optional: `12_create_service_recent_read_model.sql`
   - Use this on existing clusters where `Data_Book_NAVER_Service.naver_book_latest` exists but the recent-list read model is missing.
   - Adds `Data_Book_NAVER_Service.naver_book_recent` with a 30-day TTL and grants app/Trino read access.

## If v4 `Data_Prediction_Book_*` was already created

Use the v4 cleanup/copy scripts:

1. `10_disable_old_data_prediction_book_kafka_objects.sql`
2. `01_create_data_book_naver_on_cluster.sql`
3. `11_copy_data_prediction_book_to_data_book_naver_if_v4_exists.sql`
4. `03_verify_data_book_naver.sql`

## Notes

- AliSQL remains the platform SSOT. These ClickHouse schemas are OLAP-only and not SSOT.
- UUID policy: producers use UUID v7; ClickHouse stores UUID columns as `UUID`.
- Timezone policy: `DateTime64(3, 'Asia/Seoul')`.
- `ORDER BY` is treated as the primary index.
- Log/cache tables are time-first. `shotalk_search_result` deliberately keeps `ORDER BY commission_link` because the old table is a ReplacingMergeTree latest-result table with `commission_link` as the logical unique key.
- JSON-like legacy columns are stored as serialized `String` in the new extended tables to keep remote migration and Kafka ingestion stable across old ClickHouse `String`/`JSON` type differences.
- The support/legacy tables are currently placed under `Data_Book_NAVER_*` because this package is for the NAVER book repository. If other providers later share those tables, split them into a common namespace such as `Data_Book_Common_Raw`.
- `kafka_handle_error_mode='stream'` is enabled, and malformed Kafka rows are stored in `Data_Book_NAVER_Log.book_kafka_parse_error`.
- All CREATE/ALTER objects include COMMENTs according to the Statground DDL policy.
