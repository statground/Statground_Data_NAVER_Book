#!/usr/bin/env python3
"""Random OPTIMIZE for ReplacingMergeTree tables (ClickHouse).

Purpose:
  - Trigger background merges/dedup proactively for statground_book.raw_naver (ReplacingMergeTree).
  - To avoid full-table FINAL on every run, we pick ONE recent monthly partition randomly.

Env:
  CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE
  OPTIMIZE_TABLE (optional, default: {CH_DATABASE}.raw_naver)
  OPTIMIZE_RECENT_PARTITIONS (optional, default: 6)
  OPTIMIZE_FINAL (optional, default: 1)
  OPTIMIZE_ENABLED (optional, default: 1)
"""

import os
import random
import sys
import clickhouse_connect

def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def main() -> int:
    if env_int("OPTIMIZE_ENABLED", 1) != 1:
        print("[OPTIMIZE] skipped (OPTIMIZE_ENABLED!=1)")
        return 0

    host = os.environ.get("CH_HOST") or os.environ.get("CLICKHOUSE_HOST")
    port = env_int("CH_PORT", env_int("CLICKHOUSE_PORT", 8123))
    user = os.environ.get("CH_USER") or os.environ.get("CLICKHOUSE_USER") or "default"
    password = os.environ.get("CH_PASSWORD") or os.environ.get("CLICKHOUSE_PASSWORD") or ""
    db = os.environ.get("CH_DATABASE") or os.environ.get("CLICKHOUSE_DATABASE") or ""

    if not host:
        print("[OPTIMIZE] missing CH_HOST (or CLICKHOUSE_HOST). skip.")
        return 0

    table = os.environ.get("OPTIMIZE_TABLE")
    if not table:
        if db:
            table = f"{db}.raw_naver"
        else:
            # last resort
            table = "raw_naver"

    recent_n = env_int("OPTIMIZE_RECENT_PARTITIONS", 6)
    do_final = env_int("OPTIMIZE_FINAL", 1) == 1

    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=db if db else None,
    )

    # Find recent partitions (monthly) based on created_at
    # NOTE: raw_naver is large; keep this query tiny.
    q = f"""SELECT DISTINCT toYYYYMM(created_at) AS p
            FROM {table}
            WHERE created_at >= now() - INTERVAL 365 DAY
            ORDER BY p DESC
            LIMIT {recent_n}"""
    parts = [row[0] for row in client.query(q).result_rows]
    if not parts:
        print(f"[OPTIMIZE] no partitions found for table={table}. skip.")
        return 0

    p = random.choice(parts)
    stmt = f"OPTIMIZE TABLE {table} PARTITION {p}" + (" FINAL" if do_final else "")

    print(f"[OPTIMIZE] partitions(recent={recent_n})={parts} -> chosen={p}")
    print(f"[OPTIMIZE] running: {stmt}")
    client.command(stmt)
    print("[OPTIMIZE] done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
