import hashlib
import json
import os
import random
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import clickhouse_connect
import nltk
import pandas as pd
import pytz
import requests
import uuid6
from konlpy.tag import Okt
from nltk import pos_tag, word_tokenize

# ✅ NLTK 리소스 (GitHub Actions 환경 대응)
nltk.download("punkt")
nltk.download("punkt_tab")
nltk.download("averaged_perceptron_tagger")
nltk.download("averaged_perceptron_tagger_eng")

KST = pytz.timezone("Asia/Seoul")
NAVER_URL = "https://openapi.naver.com/v1/search/book.json"


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            f"Set it in GitHub Actions Secrets (repo Settings → Secrets and variables → Actions)."
        )
    return v



def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "t", "y", "yes", "on"}


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
        "NAVER_API_KEYS must be a valid JSON string like "
        '[{"client_id":"...","client_secret":"..."}, ...]'
    ) from e

TABLE_NAME = "raw_naver"

# -----------------------------
# Batch controls (GitHub Actions)
# -----------------------------
# COLLECT_MODE:
# - keyword | author | publisher | mixed(legacy)
# - unicode_random | unicode_letters | unicode_char | char | character
COLLECT_MODE = (os.getenv("COLLECT_MODE") or "mixed").strip().lower()

# How many unique terms to collect per run
BATCH_SIZE = int(os.getenv("BATCH_SIZE") or "1000")

# How many raw rows to sample from ClickHouse to build term candidates
SAMPLE_ROWS = int(os.getenv("SAMPLE_ROWS") or "8000")

# Naver API request params
DISPLAY = max(1, min(100, int(os.getenv("NAVER_DISPLAY") or "100")))  # max 100
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT") or "20")

# One term = one request (legacy default). If >= 2, do both sort=sim/date.
REQS_PER_TERM = int(os.getenv("REQS_PER_TERM") or "1")

# If true, page through all starts until NAVER_MAX_START(<=1000)
PAGINATE_ALL = _bool_env("PAGINATE_ALL", default=False)
NAVER_MAX_START = max(1, min(1000, int(os.getenv("NAVER_MAX_START") or "1000")))

# Optional explicit sort modes. Example: sim,date
NAVER_SORTS = [
    s.strip().lower()
    for s in (os.getenv("NAVER_SORTS") or "").split(",")
    if s.strip().lower() in {"sim", "date"}
]

# Threading controls
MAX_WORKERS = max(1, int(os.getenv("MAX_WORKERS") or "8"))

# Unicode random term controls
UNICODE_RANDOM_SEED = int(os.getenv("UNICODE_RANDOM_SEED") or "20260311")
UNICODE_BATCH_ANCHOR = os.getenv("UNICODE_BATCH_ANCHOR") or "2026-03-11T00:00:00+09:00"
UNICODE_SINGLE_RATIO = float(os.getenv("UNICODE_SINGLE_RATIO") or "0.5")

UNICODE_EXCLUDED_RANGES = [
    (0x2100, 0x214F),   # Letterlike Symbols
    (0xFB00, 0xFB4F),   # Alphabetic Presentation Forms
    (0xFB50, 0xFDFF),   # Arabic Presentation Forms-A
    (0xFE70, 0xFEFF),   # Arabic Presentation Forms-B
    (0xF900, 0xFAFF),   # CJK Compatibility Ideographs
    (0x2F800, 0x2FA1F), # CJK Compatibility Ideographs Supplement
    (0x1D400, 0x1D7FF), # Mathematical Alphanumeric Symbols
]



def make_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )


# Base client (single-thread use) for metadata/sample queries
base_client = make_ch_client()

# Thread-local ClickHouse client / HTTP session (one per worker thread)
try:
    import threading as _threading

    _tls = _threading.local()
except Exception:
    _tls = None



def get_thread_client():
    if _tls is None:
        return make_ch_client()
    if not hasattr(_tls, "client"):
        _tls.client = make_ch_client()
    return _tls.client



def get_thread_session():
    if _tls is None:
        return requests.Session()
    if not hasattr(_tls, "session"):
        _tls.session = requests.Session()
    return _tls.session


okt = Okt()


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


TABLE_COLUMNS = get_table_columns(base_client, CH_DATABASE, TABLE_NAME)



def filter_row_by_existing_columns(row: dict) -> dict:
    return {k: v for k, v in row.items() if k in TABLE_COLUMNS}



def pick_api_headers() -> dict:
    api = random.choice(NAVER_API_KEYS)
    return {
        "X-Naver-Client-Id": api["client_id"],
        "X-Naver-Client-Secret": api["client_secret"],
    }



def fetch_page(keyword: str, sort: str, start: int, display: int = 100) -> dict:
    headers = pick_api_headers()
    params = {"query": keyword, "display": display, "start": start, "sort": sort}

    try:
        r = get_thread_session().get(
            NAVER_URL,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
    except Exception:
        return {"items": [], "total": 0}

    if r.status_code != 200:
        return {"items": [], "total": 0}

    try:
        data = r.json()
    except Exception:
        return {"items": [], "total": 0}

    return {
        "items": data.get("items", []) or [],
        "total": int(data.get("total") or 0),
    }



def sanitize_keyword(keyword) -> str:
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
        df = base_client.query_df(
            f"SELECT title, author, publisher FROM {TABLE_NAME} ORDER BY rand() LIMIT 100"
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
            tagged = pos_tag(tokens)
            keywords.extend([w for w, t in tagged if t.startswith("NN") and len(w) >= 4])

    return sanitize_keyword(random.choice(keywords)) if keywords else random.choice(fallback)



def _sample_df_for_terms(sample_rows: int) -> pd.DataFrame:
    """Random sample from raw_naver to build term pools."""
    try:
        return base_client.query_df(
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


UNICODE_SCRIPT_PROPS = None
UNICODE_LETTER_POOL = None
UNICODE_SCRIPT_POOLS = None
UNICODE_SCRIPT_CACHE: dict[int, str] = {}



def _import_regex_core():
    try:
        import regex._regex_core as regex_core  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "regex package is required for unicode_random mode. Install `regex` in GitHub Actions."
        ) from e
    return regex_core



def _get_unicode_script_props():
    global UNICODE_SCRIPT_PROPS
    if UNICODE_SCRIPT_PROPS is not None:
        return UNICODE_SCRIPT_PROPS

    regex_core = _import_regex_core()
    script_names = list(regex_core.PROPERTY_NAMES[85][1].values())
    UNICODE_SCRIPT_PROPS = [
        (name, regex_core.lookup_property("SCRIPT", name, True))
        for name in script_names
        if name not in {"UNKNOWN", "COMMON", "INHERITED"}
    ]
    return UNICODE_SCRIPT_PROPS



def _is_excluded_unicode_letter(cp: int) -> bool:
    for start, end in UNICODE_EXCLUDED_RANGES:
        if start <= cp <= end:
            return True
    return False



def _detect_unicode_script(cp: int) -> str:
    cached = UNICODE_SCRIPT_CACHE.get(cp)
    if cached is not None:
        return cached

    for name, prop in _get_unicode_script_props():
        if prop.matches(cp):
            UNICODE_SCRIPT_CACHE[cp] = name
            return name

    UNICODE_SCRIPT_CACHE[cp] = "UNKNOWN"
    return "UNKNOWN"



def _build_unicode_term_inventory() -> tuple[list[str], dict[str, list[str]]]:
    global UNICODE_LETTER_POOL, UNICODE_SCRIPT_POOLS
    if UNICODE_LETTER_POOL is not None and UNICODE_SCRIPT_POOLS is not None:
        return UNICODE_LETTER_POOL, UNICODE_SCRIPT_POOLS

    letters: list[str] = []
    script_pools: dict[str, list[str]] = {}

    for cp in range(0x110000):
        if _is_excluded_unicode_letter(cp):
            continue

        ch = chr(cp)
        if not unicodedata.category(ch).startswith("L"):
            continue

        script = _detect_unicode_script(cp)
        if script in {"UNKNOWN", "COMMON", "INHERITED"}:
            continue

        letters.append(ch)
        script_pools.setdefault(script, []).append(ch)

    UNICODE_LETTER_POOL = letters
    UNICODE_SCRIPT_POOLS = script_pools
    return letters, script_pools



def _parse_anchor_hour() -> datetime:
    raw = UNICODE_BATCH_ANCHOR.strip()
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = KST.localize(dt)
    else:
        dt = dt.astimezone(KST)
    return dt.replace(minute=0, second=0, microsecond=0)



def _current_hour_index() -> int:
    now_hour = datetime.now(KST).replace(minute=0, second=0, microsecond=0)
    anchor = _parse_anchor_hour()
    diff_h = int((now_hour - anchor).total_seconds() // 3600)
    return max(0, diff_h)



def _take_cyclic(values: list[str], start: int, count: int) -> list[str]:
    if not values or count <= 0:
        return []
    n = len(values)
    return [values[(start + i) % n] for i in range(count)]



def _pick_unicode_random_terms(batch_size: int) -> list[str]:
    letters, script_pools = _build_unicode_term_inventory()
    if not letters:
        return ["통계"]

    single_ratio = min(1.0, max(0.0, UNICODE_SINGLE_RATIO))
    single_target = int(round(batch_size * single_ratio))
    if batch_size > 0:
        single_target = max(1, min(batch_size, single_target))

    base_rng = random.Random(UNICODE_RANDOM_SEED)
    shuffled_letters = letters.copy()
    base_rng.shuffle(shuffled_letters)

    hour_index = _current_hour_index()
    single_start = (hour_index * max(1, single_target)) % len(shuffled_letters)
    single_terms = _take_cyclic(shuffled_letters, single_start, single_target)

    terms = list(single_terms)
    seen = set(terms)

    eligible_scripts = [script for script, chars in script_pools.items() if len(chars) >= 2]
    seed_material = f"{UNICODE_RANDOM_SEED}:{hour_index}:{batch_size}".encode("utf-8")
    seeded_rng = random.Random(int(hashlib.sha256(seed_material).hexdigest()[:16], 16))

    attempts = 0
    max_attempts = max(10000, batch_size * 200)
    while len(terms) < batch_size and eligible_scripts and attempts < max_attempts:
        attempts += 1
        script = seeded_rng.choice(eligible_scripts)
        chars = script_pools[script]
        term = seeded_rng.choice(chars) + seeded_rng.choice(chars)
        if term in seen:
            continue
        seen.add(term)
        terms.append(term)

    # Fallback: if 2-char generation somehow 부족하면 1-char로 채움
    if len(terms) < batch_size:
        fill_start = (single_start + len(single_terms)) % len(shuffled_letters)
        for ch in _take_cyclic(shuffled_letters, fill_start, batch_size * 2):
            if ch in seen:
                continue
            seen.add(ch)
            terms.append(ch)
            if len(terms) >= batch_size:
                break

    seeded_rng.shuffle(terms)
    return [sanitize_keyword(x) for x in terms[:batch_size]]



def pick_unique_terms(mode: str, batch_size: int, sample_rows: int) -> list[str]:
    """Build a pool and sample unique terms."""
    fallback = ["통계", "데이터", "Statistics", "Data"]
    mode = (mode or "").strip().lower()

    if mode in {"unicode_random", "unicode_letters", "unicode_letter", "unicode_char", "char", "character"}:
        return _pick_unicode_random_terms(batch_size=batch_size)

    df = _sample_df_for_terms(sample_rows)
    if df is None or df.empty:
        return random.sample(fallback, k=min(len(fallback), batch_size))

    if mode == "author":
        pool = _extract_authors(df.get("author", pd.Series(dtype="object")).tolist())
    elif mode == "publisher":
        pool = _extract_publishers(df.get("publisher", pd.Series(dtype="object")).tolist())
    else:
        pool = _extract_keywords_from_titles(df.get("title", pd.Series(dtype="object")).tolist())

    if not pool:
        pool = set(fallback)

    pool_list = list(pool)
    random.shuffle(pool_list)
    picked = pool_list[: min(batch_size, len(pool_list))]
    return [sanitize_keyword(x) for x in picked]



def build_existing_map(ch_client, isbns: list[str]) -> dict[str, dict]:
    isbns = [i for i in isbns if isinstance(i, str) and i.strip()]
    if not isbns:
        return {}

    safe_isbns = [i.replace("'", "") for i in isbns]
    quoted = ",".join([f"'{i}'" for i in safe_isbns])

    has_version = "version" in TABLE_COLUMNS
    order_expr = "version DESC" if has_version else "updated_at DESC"

    q = f"""
        SELECT isbn, uuid, created_at, created_log
        FROM {TABLE_NAME}
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

    result: dict[str, dict] = {}
    for _, row in df.iterrows():
        result[str(row["isbn"])] = {
            "uuid": str(row["uuid"]),
            "created_at": row["created_at"],
            "created_log": row["created_log"],
        }
    return result



def _effective_sorts() -> list[str]:
    if NAVER_SORTS:
        return NAVER_SORTS
    if REQS_PER_TERM <= 1:
        return [random.choice(["sim", "date"])]
    return ["sim", "date"]



def _term_debug(term: str) -> str:
    try:
        return term.encode("unicode_escape").decode("ascii")
    except Exception:
        return repr(term)



def _collect_for_term(term: str, mode: str) -> dict:
    ch_client = get_thread_client()
    total_requests = 0
    total_insert_rows = 0
    term_existing_cache: dict[str, dict] = {}

    for sort in _effective_sorts():
        start = 1
        seen_page_signatures: set[tuple[str, ...]] = set()

        while start <= NAVER_MAX_START:
            page = fetch_page(term, sort, start=start, display=DISPLAY)
            items = page.get("items", []) or []
            total = int(page.get("total") or 0)
            total_requests += 1

            if not items:
                break

            page_isbns = [str(it.get("isbn")) for it in items if it.get("isbn")]
            page_signature = tuple(sorted(set(page_isbns)))

            if page_signature and page_signature in seen_page_signatures:
                break
            if page_signature:
                seen_page_signatures.add(page_signature)

            missing = [isbn for isbn in page_isbns if isbn not in term_existing_cache]
            if missing:
                term_existing_cache.update(build_existing_map(ch_client, missing))

            now = datetime.now(KST)
            version = int(now.timestamp())
            rows = []
            page_seen_isbns: set[str] = set()

            for it in items:
                isbn = it.get("isbn")
                if not isbn:
                    continue
                isbn = str(isbn)
                if isbn in page_seen_isbns:
                    continue
                page_seen_isbns.add(isbn)

                existing = term_existing_cache.get(isbn)
                if existing:
                    book_uuid = existing["uuid"]
                    created_at_value = existing["created_at"]
                    created_log_value = existing["created_log"]
                else:
                    book_uuid = str(uuid6.uuid7())
                    created_at_value = now
                    created_log_value = "github_actions_auto"
                    term_existing_cache[isbn] = {
                        "uuid": book_uuid,
                        "created_at": created_at_value,
                        "created_log": created_log_value,
                    }

                row = {
                    "uuid": book_uuid,
                    "version": version,
                    "created_at": created_at_value,
                    "created_log": created_log_value,
                    "updated_at": now,
                    "updated_log": f"auto_upsert|mode={mode}|term={term}|sort={sort}|start={start}",
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
                ch_client.insert_df(TABLE_NAME, df_ins)
                total_insert_rows += len(rows)

            if not PAGINATE_ALL:
                break
            if len(items) < DISPLAY:
                break
            if not page_signature:
                break

            effective_total = min(1000, total) if total > 0 else 1000
            next_start = start + DISPLAY
            if next_start > effective_total:
                break

            start = next_start

    return {
        "term": term,
        "term_debug": _term_debug(term),
        "requests": total_requests,
        "rows": total_insert_rows,
    }



def collect():
    mode = COLLECT_MODE

    # legacy behavior
    if mode == "mixed":
        term = sanitize_keyword(generate_keyword())
        result = _collect_for_term(term, mode)
        print(f"[mixed] term={result['term_debug']} requests={result['requests']} rows={result['rows']}")
        return

    terms = pick_unique_terms(mode=mode, batch_size=BATCH_SIZE, sample_rows=SAMPLE_ROWS)
    if not terms:
        print("No terms selected; nothing to collect.")
        return

    random.shuffle(terms)
    worker_count = min(MAX_WORKERS, max(1, len(terms)))
    print(
        f"[collect] mode={mode} terms={len(terms)} workers={worker_count} "
        f"paginate_all={int(PAGINATE_ALL)} max_start={NAVER_MAX_START} display={DISPLAY}"
    )

    total_requests = 0
    total_rows = 0
    with ThreadPoolExecutor(max_workers=worker_count) as ex:
        future_map = {ex.submit(_collect_for_term, term, mode): term for term in terms}
        for future in as_completed(future_map):
            term = future_map[future]
            try:
                result = future.result()
            except Exception as e:
                print(f"[error] term={_term_debug(term)} error={e}")
                continue

            total_requests += int(result.get("requests") or 0)
            total_rows += int(result.get("rows") or 0)
            print(
                f"[done] term={result['term_debug']} requests={result['requests']} rows={result['rows']}"
            )

    print(f"[summary] mode={mode} terms={len(terms)} requests={total_requests} rows={total_rows}")


if __name__ == "__main__":
    collect()
