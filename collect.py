import os
import json
import random
import requests
import clickhouse_connect
import uuid6
import pandas as pd
import pytz
import re
from datetime import datetime

from konlpy.tag import Okt
import nltk
from nltk import pos_tag, word_tokenize

# ✅ NLTK 리소스 (GitHub Actions 환경 대응)
nltk.download("punkt")
nltk.download("punkt_tab")
nltk.download("averaged_perceptron_tagger")
nltk.download("averaged_perceptron_tagger_eng")  # ✅ 추가: LookupError 해결

KST = pytz.timezone("Asia/Seoul")
NAVER_URL = "https://openapi.naver.com/v1/search/book.json"

CH_HOST = os.environ["CH_HOST"]
CH_PORT = int(os.environ["CH_PORT"])
CH_USER = os.environ["CH_USER"]
CH_PASSWORD = os.environ["CH_PASSWORD"]
CH_DATABASE = os.environ["CH_DATABASE"]
NAVER_API_KEYS = json.loads(os.environ["NAVER_API_KEYS"])

TABLE_NAME = "raw_naver"

# -----------------------------
# Batch controls (GitHub Actions)
# -----------------------------
# COLLECT_MODE: keyword | author | publisher | mixed(legacy)
COLLECT_MODE = (os.getenv("COLLECT_MODE") or "mixed").strip().lower()

# How many unique terms to collect per run
BATCH_SIZE = int(os.getenv("BATCH_SIZE") or "500")

# How many raw rows to sample from ClickHouse to build term candidates
SAMPLE_ROWS = int(os.getenv("SAMPLE_ROWS") or "8000")

# Naver API request params
DISPLAY = int(os.getenv("NAVER_DISPLAY") or "100")  # max 100

# One term = one request (default). If set to 2, we do both sort=sim/date.
REQS_PER_TERM = int(os.getenv("REQS_PER_TERM") or "1")

client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE
)

okt = Okt()


def get_table_columns(database: str, table: str) -> set[str]:
    q = f"""
        SELECT name
        FROM system.columns
        WHERE database = '{database}'
          AND table = '{table}'
    """
    df = client.query_df(q)
    if df is None or df.empty or "name" not in df.columns:
        return set()
    return set(df["name"].astype(str).tolist())


TABLE_COLUMNS = get_table_columns(CH_DATABASE, TABLE_NAME)


def filter_row_by_existing_columns(row: dict) -> dict:
    return {k: v for k, v in row.items() if k in TABLE_COLUMNS}


def pick_api_headers() -> dict:
    api = random.choice(NAVER_API_KEYS)
    return {
        "X-Naver-Client-Id": api["client_id"],
        "X-Naver-Client-Secret": api["client_secret"],
    }


def fetch_items(keyword: str, sort: str, start: int, display: int = 100):
    headers = pick_api_headers()
    params = {"query": keyword, "display": display, "start": start, "sort": sort}
    r = requests.get(NAVER_URL, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        return []
    data = r.json()
    return data.get("items", []) or []


def sanitize_keyword(keyword) -> str:
    """✅ None/공백 방지 + fallback"""
    fallback = ["통계", "데이터", "Statistics", "Data"]
    if keyword is None:
        return random.choice(fallback)
    if not isinstance(keyword, str):
        keyword = str(keyword)
    keyword = keyword.strip()
    if not keyword:
        return random.choice(fallback)
    return keyword


def generate_keyword() -> str:
    fallback = ["통계", "데이터", "Statistics", "Data"]

    try:
        df = client.query_df(
            "SELECT title, author, publisher FROM raw_naver ORDER BY rand() LIMIT 100"
        )
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        return random.choice(fallback)

    source = random.choice(["title", "author", "publisher"])

    # author (형태소 분석 X)
    if source == "author":
        candidates = []
        if "author" in df.columns:
            for v in df["author"].tolist():
                if isinstance(v, str) and v.strip():
                    parts = [p.strip() for p in v.split("^") if p and p.strip()]
                    candidates.extend(parts)
        return sanitize_keyword(random.choice(candidates)) if candidates else random.choice(fallback)

    # publisher (형태소 분석 X)
    if source == "publisher":
        candidates = []
        if "publisher" in df.columns:
            for v in df["publisher"].tolist():
                if isinstance(v, str) and v.strip():
                    candidates.append(v.strip())
        return sanitize_keyword(random.choice(candidates)) if candidates else random.choice(fallback)

    # title → 형태소 분석
    titles = []
    if "title" in df.columns:
        for v in df["title"].tolist():
            if isinstance(v, str) and v.strip():
                titles.append(v.strip())

    if not titles:
        return random.choice(fallback)

    keywords = []
    for title in titles:
        if re.search("[가-힣]", title):
            nouns = okt.nouns(title)
            keywords.extend([n for n in nouns if len(n) >= 2])
        else:
            tokens = word_tokenize(title)
            # ✅ pos_tag에서 tagger_eng 필요 (위에서 다운로드)
            tagged = pos_tag(tokens)
            keywords.extend([w for w, t in tagged if t.startswith("NN") and len(w) >= 4])

    return sanitize_keyword(random.choice(keywords)) if keywords else random.choice(fallback)


def _sample_df_for_terms(sample_rows: int) -> pd.DataFrame:
    """Random sample from raw_naver to build term pools."""
    try:
        return client.query_df(
            f"SELECT title, author, publisher FROM {TABLE_NAME} ORDER BY rand() LIMIT {int(sample_rows)}"
        )
    except Exception:
        return pd.DataFrame()


def _extract_keywords_from_titles(titles: list[str]) -> set[str]:
    out: set[str] = set()
    for title in titles:
        if not isinstance(title, str):
            continue
        t = title.strip()
        if not t:
            continue

        if re.search("[가-힣]", t):
            nouns = okt.nouns(t)
            for n in nouns:
                n = n.strip()
                if len(n) >= 2:
                    out.add(n)
        else:
            tokens = word_tokenize(t)
            tagged = pos_tag(tokens)
            for w, tag in tagged:
                w = str(w).strip()
                if tag.startswith("NN") and len(w) >= 4:
                    out.add(w)
    return out


def _extract_authors(author_values: list[str]) -> set[str]:
    out: set[str] = set()
    for v in author_values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if not v:
            continue
        # author in DB can be "A^B^C" (multiple authors)
        for p in v.split("^"):
            p = p.strip()
            if len(p) >= 2:
                out.add(p)
    return out


def _extract_publishers(publisher_values: list[str]) -> set[str]:
    out: set[str] = set()
    for v in publisher_values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if len(v) >= 2:
            out.add(v)
    return out


def pick_unique_terms(mode: str, batch_size: int, sample_rows: int) -> list[str]:
    """Build a pool from existing raw_naver and sample unique terms."""
    fallback = ["통계", "데이터", "Statistics", "Data"]

    df = _sample_df_for_terms(sample_rows)
    if df is None or df.empty:
        return random.sample(fallback, k=min(len(fallback), batch_size))

    mode = (mode or "").strip().lower()

    if mode == "author":
        pool = _extract_authors(df.get("author", pd.Series([])).tolist())
    elif mode == "publisher":
        pool = _extract_publishers(df.get("publisher", pd.Series([])).tolist())
    else:
        # keyword
        pool = _extract_keywords_from_titles(df.get("title", pd.Series([])).tolist())

    if not pool:
        pool = set(fallback)

    pool_list = list(pool)
    random.shuffle(pool_list)
    picked = pool_list[: min(batch_size, len(pool_list))]
    return [sanitize_keyword(x) for x in picked]


def build_existing_map(isbns):
    isbns = [i for i in isbns if isinstance(i, str) and i.strip()]
    if not isbns:
        return {}

    safe_isbns = [i.replace("'", "") for i in isbns]
    quoted = ",".join([f"'{i}'" for i in safe_isbns])

    has_version = "version" in TABLE_COLUMNS
    order_expr = "version DESC" if has_version else "created_at DESC"

    q = f"""
        SELECT isbn, uuid, created_at, created_log
        FROM {TABLE_NAME}
        WHERE isbn IN ({quoted})
        ORDER BY {order_expr}
        LIMIT 1 BY isbn
    """

    try:
        df = client.query_df(q)
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


def _collect_for_term(term: str, mode: str):
    # Request strategy
    sorts = [random.choice(["sim", "date"])] if REQS_PER_TERM <= 1 else ["sim", "date"]
    for sort in sorts:
        items = fetch_items(term, sort, start=1, display=DISPLAY)
        if not items:
            continue

        now = datetime.now(KST)
        version = int(now.timestamp())

        batch_isbns = [it.get("isbn") for it in items if it.get("isbn")]
        existing_map = build_existing_map(batch_isbns)

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
                "updated_log": f"auto_upsert|mode={mode}|term={term}|sort={sort}",
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
            df_ins = pd.DataFrame(rows)
            client.insert_df(TABLE_NAME, df_ins)


def collect():
    mode = COLLECT_MODE

    # legacy behavior
    if mode == "mixed":
        term = sanitize_keyword(generate_keyword())
        _collect_for_term(term, mode)
        return

    # new behavior
    terms = pick_unique_terms(mode=mode, batch_size=BATCH_SIZE, sample_rows=SAMPLE_ROWS)
    random.shuffle(terms)
    for term in terms:
        _collect_for_term(term, mode)


if __name__ == "__main__":
    collect()
