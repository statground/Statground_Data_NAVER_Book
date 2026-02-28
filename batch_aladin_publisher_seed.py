import os
import json
import random
import time
import re
from datetime import datetime

import requests
import pandas as pd
import pytz
import clickhouse_connect
import uuid6
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

KST = pytz.timezone("Asia/Seoul")

NAVER_URL = "https://openapi.naver.com/v1/search/book.json"
RAW_NAVER_TABLE = os.getenv("RAW_NAVER_TABLE") or "raw_naver"

# ---------------------------
# env helpers
# ---------------------------
def _require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

# ---- Required env (GitHub Actions) ----
CH_HOST = _require_env("CH_HOST")
CH_PORT = int(_require_env("CH_PORT"))
CH_USER = _require_env("CH_USER")
CH_PASSWORD = _require_env("CH_PASSWORD")
CH_DATABASE = _require_env("CH_DATABASE")

try:
    NAVER_API_KEYS = json.loads(_require_env("NAVER_API_KEYS"))
except Exception as e:
    raise RuntimeError(
        "NAVER_API_KEYS must be JSON like "
        '[{"client_id":"...","client_secret":"..."}, ...]'
    ) from e

# ---------------------------
# Controls
# ---------------------------
# Aladin
ALADIN_URL = (os.getenv("ALADIN_PUBLISHER_LIST_URL") or "https://www.aladin.co.kr/aladdin/PublisherList.aspx").strip()
ALADIN_MAX_WORKERS = int(os.getenv("ALADIN_MAX_WORKERS") or "6")
ALADIN_SLEEP_MIN = float(os.getenv("ALADIN_SLEEP_MIN") or "0.05")
ALADIN_SLEEP_MAX = float(os.getenv("ALADIN_SLEEP_MAX") or "0.20")

# Cache table (ClickHouse)
ALADIN_CACHE_TABLE = (os.getenv("ALADIN_CACHE_TABLE") or "raw_aladin_publisher_cache").strip()

# Publisher sampling
PUBLISHER_SAMPLE_N = int(os.getenv("PUBLISHER_SAMPLE_N") or "100")

# Naver
DISPLAY = int(os.getenv("NAVER_DISPLAY") or "100")  # max 100
NAVER_MAX_WORKERS = int(os.getenv("NAVER_MAX_WORKERS") or "10")
NAVER_SLEEP_MIN = float(os.getenv("NAVER_SLEEP_MIN") or "0.05")
NAVER_SLEEP_MAX = float(os.getenv("NAVER_SLEEP_MAX") or "0.20")

# If 2 -> both sim/date; if 1 -> date only (권장: date만이 중복 적고 누적 수집 유리)
REQS_PER_TERM = int(os.getenv("REQS_PER_TERM") or "1")


# ---------------------------
# ClickHouse client
# ---------------------------
def make_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
    )

base_client = make_ch_client()


def get_table_columns(ch_client, database: str, table: str) -> set[str]:
    q = f"""
        SELECT name
        FROM system.columns
        WHERE database = '{database}'
          AND table = '{table}'
    """
    df = ch_client.query_df(q)
    if df is None or df.empty or "name" not in df.columns:
        return set()
    return set(df["name"].astype(str).tolist())


RAW_NAVER_COLUMNS = get_table_columns(base_client, CH_DATABASE, RAW_NAVER_TABLE)
if not RAW_NAVER_COLUMNS:
    raise RuntimeError(f"Cannot read columns for {CH_DATABASE}.{RAW_NAVER_TABLE}")


def filter_row_by_existing_columns(row: dict) -> dict:
    return {k: v for k, v in row.items() if k in RAW_NAVER_COLUMNS}


# ---------------------------
# NAVER API helpers
# ---------------------------
def pick_api_headers() -> dict:
    api = random.choice(NAVER_API_KEYS)
    return {
        "X-Naver-Client-Id": api["client_id"],
        "X-Naver-Client-Secret": api["client_secret"],
    }


def fetch_naver_page(query: str, sort: str, start: int, display: int = 100) -> tuple[int, list[dict]]:
    """
    return (total, items)
    """
    headers = pick_api_headers()
    params = {"query": query, "display": display, "start": start, "sort": sort}
    r = requests.get(NAVER_URL, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        return 0, []
    data = r.json()
    total = int(data.get("total") or 0)
    items = data.get("items", []) or []
    return total, items


def build_existing_map(ch_client, isbns: list[str]) -> dict:
    isbns = [i for i in isbns if isinstance(i, str) and i.strip()]
    if not isbns:
        return {}

    safe_isbns = [i.replace("'", "") for i in isbns]
    quoted = ",".join([f"'{i}'" for i in safe_isbns])

    has_version = "version" in RAW_NAVER_COLUMNS
    order_expr = "version DESC" if has_version else "created_at DESC"

    q = f"""
        SELECT isbn, uuid, created_at, created_log
        FROM {RAW_NAVER_TABLE}
        WHERE isbn IN ({quoted})
        ORDER BY {order_expr}
        LIMIT 1 BY isbn
    """

    try:
        df = ch_client.query_df(q)
    except Exception:
        return {}

    if df is None or df.empty:
        return {}

    needed = {"isbn", "uuid", "created_at", "created_log"}
    if not needed.issubset(set(df.columns)):
        return {}

    result = {}
    for _, row in df.iterrows():
        result[str(row["isbn"])] = {
            "uuid": str(row["uuid"]),
            "created_at": row["created_at"],
            "created_log": row["created_log"],
        }
    return result


def upsert_items_to_raw_naver(ch_client, items: list[dict], now: datetime, version: int, updated_log: str):
    batch_isbns = [it.get("isbn") for it in items if it.get("isbn")]
    existing_map = build_existing_map(ch_client, batch_isbns)

    rows = []
    for it in items:
        isbn = it.get("isbn")
        if not isbn:
            continue

        existing = existing_map.get(isbn)
        if existing:
            book_uuid = existing["uuid"]
            created_at_value = existing["created_at"]
            created_log_value = existing["created_log"]
        else:
            book_uuid = str(uuid6.uuid7())
            created_at_value = now
            created_log_value = "github_actions_auto"

        row = {
            "uuid": book_uuid,
            "version": version,
            "created_at": created_at_value,
            "created_log": created_log_value,
            "updated_at": now,
            "updated_log": updated_log,
            "title": it.get("title") or "",
            "link": it.get("link") or "",
            "image": it.get("image") or "",
            "author": it.get("author") or "",
            "discount": int(it["discount"]) if it.get("discount") else None,
            "publisher": it.get("publisher") or "",
            "isbn": isbn,
            "description": it.get("description") or "",
            "pubdate": it.get("pubdate") or "",
        }
        rows.append(filter_row_by_existing_columns(row))

    if rows:
        ch_client.insert_df(RAW_NAVER_TABLE, pd.DataFrame(rows))


def collect_all_books_for_publisher(publisher_name: str):
    """
    출판사명으로 네이버 Book API를 페이지네이션(start=1..1000)까지 최대한 수집.
    """
    time.sleep(random.uniform(NAVER_SLEEP_MIN, NAVER_SLEEP_MAX))

    # thread-safe: client per worker
    ch_client = make_ch_client()

    # 권장: date 위주 수집(중복이 적고 최신부터 쌓임)
    sorts = ["date"] if REQS_PER_TERM <= 1 else ["sim", "date"]

    for sort in sorts:
        now = datetime.now(KST)
        version = int(now.timestamp())

        start = 1
        total = None
        fetched = 0

        while True:
            # 네이버 API 제한: start <= 1000
            if start > 1000:
                break

            t, items = fetch_naver_page(publisher_name, sort, start=start, display=DISPLAY)

            if total is None:
                total = t

            if not items:
                break

            updated_log = f"aladin_publisher_seed|publisher={publisher_name}|sort={sort}|start={start}"
            upsert_items_to_raw_naver(ch_client, items, now=now, version=version, updated_log=updated_log)

            fetched += len(items)
            start += DISPLAY

            # total이 0이 아니면 종료 조건 명확히 가능
            if total and start > total:
                break

            # polite
            time.sleep(random.uniform(NAVER_SLEEP_MIN, NAVER_SLEEP_MAX))

        print(f"[NAVER] publisher='{publisher_name}' sort={sort} total={total or 0} fetched={fetched}")


# ---------------------------
# Aladin crawler (dynamic last page)
# ---------------------------
def _aladin_headers(base_url: str) -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Referer": base_url,
        "Origin": "https://www.aladin.co.kr",
    }


def parse_cnt(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    cnt_input = soup.select_one('form[name="PageNav"] input[name="cnt"]')
    if cnt_input and cnt_input.has_attr("value"):
        return str(cnt_input["value"]).strip()
    return None


def parse_last_page(html: str) -> int | None:
    soup = BeautifulSoup(html, "lxml")

    # 1) "끝" 링크 우선
    for a in soup.select("a"):
        if a.get_text(strip=True) == "끝":
            href = a.get("href", "")
            m = re.search(r"Page_Set\('(\d+)'\)", href)
            if m:
                return int(m.group(1))

    # 2) fallback: Page_Set 중 최대값
    max_n = None
    for a in soup.select("a[href*='Page_Set']"):
        href = a.get("href", "")
        m = re.search(r"Page_Set\('(\d+)'\)", href)
        if m:
            n = int(m.group(1))
            max_n = n if max_n is None else max(max_n, n)
    return max_n


def extract_publishers_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    pubs = []
    for td in soup.select("td.c2b_center"):
        name = td.get_text(strip=True)
        if name:
            pubs.append(name)
    return pubs


def aladin_fetch_page(page: int, cnt: str, base_url: str) -> tuple[int, list[str]]:
    time.sleep(random.uniform(ALADIN_SLEEP_MIN, ALADIN_SLEEP_MAX))
    s = requests.Session()
    s.headers.update(_aladin_headers(base_url))
    r = s.post(base_url, data={"page": str(page), "cnt": str(cnt)}, timeout=30)
    r.raise_for_status()
    pubs = extract_publishers_from_html(r.text)
    return page, pubs


def detect_cnt_and_last_page(base_url: str) -> tuple[str, int]:
    s = requests.Session()
    s.headers.update(_aladin_headers(base_url))
    r = s.get(base_url, timeout=30)
    r.raise_for_status()

    cnt = parse_cnt(r.text) or "27942"
    last_page = parse_last_page(r.text)

    if not last_page:
        raise RuntimeError("Failed to detect Aladin last page (pagination structure changed or blocked).")

    return cnt, last_page


# ---------------------------
# ClickHouse cache for aladin publishers
# ---------------------------
def ensure_aladin_cache_table(ch_client):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {ALADIN_CACHE_TABLE}
    (
      publisher String COMMENT '알라딘 출판사명(원문)',
      collected_at DateTime64(3, 'Asia/Seoul') COMMENT '수집 시각 (Asia/Seoul)',
      detected_last_page UInt32 COMMENT '수집 당시 알라딘 최대 페이지',
      run_uuid UUID COMMENT '배치 실행 UUID v7 (OLAP 전용, SSOT 아님)',
      source LowCardinality(String) COMMENT '수집 출처 (aladin)'
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(collected_at)
    ORDER BY (collected_at, detected_last_page, publisher)
    COMMENT '알라딘 출판사 목록 캐시/로그 (OLAP 전용, SSOT 아님). 배치 효율 목적 캐시';
    """
    ch_client.command(ddl)


def load_cached_publishers_if_fresh(ch_client, current_last_page: int) -> list[str]:
    """
    캐시가 존재하고, 캐시의 detected_last_page가 current_last_page와 같으면 재사용.
    """
    q_lp = f"""
      SELECT max(detected_last_page) AS last_page
      FROM {ALADIN_CACHE_TABLE}
    """
    try:
        df_lp = ch_client.query_df(q_lp)
        cached_lp = int(df_lp["last_page"].iloc[0]) if df_lp is not None and not df_lp.empty else 0
    except Exception:
        return []

    if cached_lp != int(current_last_page):
        return []

    q = f"""
      SELECT DISTINCT publisher
      FROM {ALADIN_CACHE_TABLE}
      WHERE detected_last_page = {int(current_last_page)}
    """
    try:
        df = ch_client.query_df(q)
        if df is None or df.empty:
            return []
        pubs = [str(x).strip() for x in df["publisher"].tolist() if isinstance(x, str) and x.strip()]
        # dedupe(안전)
        seen = set()
        uniq = []
        for p in pubs:
            if p not in seen:
                seen.add(p)
                uniq.append(p)
        return uniq
    except Exception:
        return []


def save_publishers_cache(ch_client, publishers: list[str], detected_last_page: int, run_uuid: str):
    now = datetime.now(KST)
    rows = []
    for p in publishers:
        rows.append({
            "publisher": p,
            "collected_at": now,
            "detected_last_page": int(detected_last_page),
            "run_uuid": run_uuid,
            "source": "aladin",
        })
    ch_client.insert_df(ALADIN_CACHE_TABLE, pd.DataFrame(rows))


def crawl_aladin_publishers_dynamic(base_url: str, max_workers: int) -> tuple[list[str], int]:
    cnt, last_page = detect_cnt_and_last_page(base_url)
    print(f"[ALADIN] detected cnt={cnt}, last_page={last_page}")

    results: dict[int, list[str]] = {}
    pages = list(range(1, last_page + 1))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(aladin_fetch_page, p, cnt, base_url) for p in pages]
        for fut in as_completed(futures):
            page, pubs = fut.result()
            results[page] = pubs
            if page % 50 == 0:
                print(f"[ALADIN] fetched page={page}/{last_page}")

    all_pubs: list[str] = []
    for p in range(1, last_page + 1):
        all_pubs.extend(results.get(p, []))

    seen = set()
    uniq = []
    for p in all_pubs:
        p = str(p).strip()
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)

    print(f"[ALADIN] pages={last_page} raw={len(all_pubs)} unique={len(uniq)}")
    return uniq, last_page


# ---------------------------
# run
# ---------------------------
def run():
    run_uuid = str(uuid6.uuid7())
    ensure_aladin_cache_table(base_client)

    # 1) 먼저 last_page만 탐지해서, 캐시 재사용 여부를 결정
    _, current_last_page = detect_cnt_and_last_page(ALADIN_URL)
    cached = load_cached_publishers_if_fresh(base_client, current_last_page)

    if cached:
        publishers = cached
        print(f"[CACHE] use cached publishers for last_page={current_last_page}: {len(publishers)}")
    else:
        publishers, detected_last_page = crawl_aladin_publishers_dynamic(ALADIN_URL, ALADIN_MAX_WORKERS)
        save_publishers_cache(base_client, publishers, detected_last_page=detected_last_page, run_uuid=run_uuid)
        print(f"[CACHE] saved publishers: {len(publishers)} (last_page={detected_last_page})")

    if not publishers:
        raise RuntimeError("No publishers collected from Aladin.")

    # 2) sample random publishers
    sample_n = min(PUBLISHER_SAMPLE_N, len(publishers))
    sampled = random.sample(publishers, k=sample_n)
    print(f"[SAMPLE] picked {len(sampled)} publishers")

    # 3) 네이버: 출판사별로 페이지네이션 끝까지 수집
    ok = 0
    with ThreadPoolExecutor(max_workers=NAVER_MAX_WORKERS) as ex:
        futures = [ex.submit(collect_all_books_for_publisher, p) for p in sampled]
        for fut in as_completed(futures):
            fut.result()
            ok += 1
            if ok % 10 == 0:
                print(f"[DONE] completed publishers: {ok}/{len(sampled)}")

    print(f"[FINISH] completed publishers: {ok}/{len(sampled)}")


if __name__ == "__main__":
    run()