#!/usr/bin/env bash
set -euo pipefail

ROOT_REPO="${BOOK_CDN_ROOT_REPO:-statground/Statground_CDN_Book}"
ROOT_DIR="${BOOK_CDN_ROOT_DIR:-Statground_CDN_Book}"
SHARD_REPO_PREFIX="${BOOK_CDN_SHARD_REPO_PREFIX:-statground/Statground_CDN_Book_Detail_}"
SHARD_DIR_TEMPLATE="${BOOK_CDN_SHARD_DIR_TEMPLATE:-Statground_CDN_Book_Detail_%s}"
BRANCH="${BOOK_CDN_BRANCH:-main}"
LANGUAGE="${BOOK_CDN_LANGUAGE:-ko}"
REGISTRY_PATH="${BOOK_CDN_REGISTRY_PATH:-books/${LANGUAGE}/registry.json}"
REPORT_PATH="${BOOK_CDN_REPORT_PATH:-book_cdn_export_report.json}"
PREFIXES="${BOOK_CDN_HASH_PREFIXES:-0123456789abcdef}"
EXPORT_OUTPUT_ROOT="${BOOK_CDN_OUTPUT_ROOT:-.book_cdn_export}"

if [[ -z "${STATGROUND_CDN_BOOK_ADMIN_TOKEN:-}" ]]; then
  echo "STATGROUND_CDN_BOOK_ADMIN_TOKEN secret is required"
  exit 1
fi
if [[ -z "${STATGROUND_BOOK_CONTENT_KEY:-}" ]]; then
  echo "STATGROUND_BOOK_CONTENT_KEY secret is required"
  exit 1
fi
if [[ -z "${CH_HOST:-${CLICKHOUSE_HOST:-}}" ]] || [[ -z "${CH_USER:-${CLICKHOUSE_USER:-}}" ]] || [[ -z "${CH_PASSWORD:-${CLICKHOUSE_PASSWORD:-}}" ]]; then
  echo "ClickHouse secrets are required for book CDN export"
  exit 1
fi

normalize_prefixes() {
  python3 - "$1" "${2:-}" <<'PY'
import json
import re
import sys

raw = sys.argv[1].strip().lower()
registry_path = sys.argv[2].strip() if len(sys.argv) > 2 else ""
if raw in ("registry", "existing"):
    tokens = []
    try:
        with open(registry_path, encoding="utf-8") as f:
            registry = json.load(f)
        for shard in registry.get("shards", []):
            prefix = str(shard.get("prefix", "")).strip().lower()
            if prefix:
                tokens.append(prefix)
    except Exception:
        tokens = []
    if not tokens:
        tokens = list("0123456789abcdef")
elif raw in ("", "all", "*"):
    tokens = list("0123456789abcdef")
elif re.search(r"[,\s]", raw):
    tokens = [token for token in re.split(r"[,\s]+", raw) if token]
else:
    tokens = list(raw)

out = []
for token in tokens:
    if re.fullmatch(r"[0-9a-f]{1,8}", token) and token not in out:
        out.append(token)
if not out:
    out = ["0"]
print(",".join(out))
PY
}

token_remote() {
  printf 'https://x-access-token:%s@github.com/%s.git' "$STATGROUND_CDN_BOOK_ADMIN_TOKEN" "$1"
}

prepare_repo() {
  local repo="$1"
  local dir="$2"
  if ! python scripts/ensure_cdn_book_repo.py --repo "$repo" --description "Statground encrypted NAVER Book CDN"; then
    echo "::error::Unable to create or access ${repo}. Pre-create it as a public repository or update STATGROUND_CDN_BOOK_ADMIN_TOKEN with statground org repository creation and contents write permissions."
    return 1
  fi
  if [[ -d "$dir/.git" ]]; then
    git -C "$dir" remote set-url origin "$(token_remote "$repo")"
    git -C "$dir" fetch origin "$BRANCH" || true
    return 0
  fi
  if git ls-remote --exit-code --heads "$(token_remote "$repo")" "$BRANCH" >/dev/null 2>&1; then
    git clone --branch "$BRANCH" "$(token_remote "$repo")" "$dir"
    return 0
  fi
  mkdir -p "$dir"
  git -C "$dir" init -b "$BRANCH"
  git -C "$dir" remote add origin "$(token_remote "$repo")"
}

commit_repo_path() {
  local dir="$1"
  local message="$2"
  local pathspec="$3"
  git -C "$dir" config user.name "statground-book-cdn-bot"
  git -C "$dir" config user.email "actions@users.noreply.github.com"
  git -C "$dir" add "$pathspec"
  if git -C "$dir" diff --cached --quiet; then
    if git -C "$dir" rev-parse --verify HEAD >/dev/null 2>&1; then
      git -C "$dir" rev-parse HEAD
    else
      printf ''
    fi
    return 0
  fi
  git -C "$dir" commit -m "$message"
  git -C "$dir" push origin "HEAD:${BRANCH}"
  git -C "$dir" rev-parse HEAD
}

prepare_repo "$ROOT_REPO" "$ROOT_DIR"
PREFIXES="$(normalize_prefixes "$PREFIXES" "${ROOT_DIR}/${REGISTRY_PATH}")"
IFS=',' read -r -a PREFIX_LIST <<< "$PREFIXES"

rm -rf "$EXPORT_OUTPUT_ROOT"
mkdir -p "$EXPORT_OUTPUT_ROOT"

export BOOK_CDN_LANGUAGE="$LANGUAGE"
export BOOK_CDN_REPORT_PATH="$REPORT_PATH"
export BOOK_CDN_SHARD_DIR_TEMPLATE="$SHARD_DIR_TEMPLATE"
export BOOK_CDN_SHARD_REPO_PREFIX="$SHARD_REPO_PREFIX"
export BOOK_CDN_HASH_PREFIXES="$PREFIXES"
export BOOK_CDN_OUTPUT_ROOT="$EXPORT_OUTPUT_ROOT"

go run -mod=mod ./cmd/export_book_cdn

commits=""
for prefix in "${PREFIX_LIST[@]}"; do
  export_dir="${EXPORT_OUTPUT_ROOT}/$(printf "$SHARD_DIR_TEMPLATE" "$prefix")"
  if [[ ! -d "${export_dir}/books" ]]; then
    continue
  fi
  repo="${SHARD_REPO_PREFIX}${prefix}"
  dir="$(printf "$SHARD_DIR_TEMPLATE" "$prefix")"
  prepare_repo "$repo" "$dir"
  mkdir -p "${dir}/books"
  cp -a "${export_dir}/books/." "${dir}/books/"
  sha="$(commit_repo_path "$dir" "Refresh encrypted NAVER Book CDN shard ${prefix}" books)"
  if [[ -n "$sha" ]]; then
    if [[ -n "$commits" ]]; then
      commits+=","
    fi
    commits+="${prefix}=${sha}"
  fi
done

if [[ -z "$commits" ]]; then
  echo "No shard commits are available; skipping registry update"
  exit 0
fi

export BOOK_CDN_REGISTRY_ROOT="$ROOT_DIR"
export BOOK_CDN_SHARD_COMMITS="$commits"
go run -mod=mod ./cmd/update_book_cdn_registry

root_sha="$(commit_repo_path "$ROOT_DIR" "Refresh encrypted NAVER Book CDN registry" "$REGISTRY_PATH")"
if [[ -z "$root_sha" ]]; then
  echo "Root registry commit is unavailable"
  exit 1
fi

export BOOK_CDN_ROOT_COMMIT_SHA="$root_sha"
export BOOK_CDN_ROOT_REPO="$ROOT_REPO"
export BOOK_CDN_REGISTRY_FILE="${ROOT_DIR}/${REGISTRY_PATH}"
export BOOK_CDN_REGISTRY_PATH="$REGISTRY_PATH"
go run -mod=mod ./cmd/record_book_cdn_release

registry_url="https://cdn.jsdelivr.net/gh/${ROOT_REPO}@${root_sha}/${REGISTRY_PATH}"
for attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do
  status="$(curl -sS -o /dev/null -w '%{http_code}' -I "$registry_url" || true)"
  if [[ "$status" == "200" ]]; then
    echo "Book CDN registry visible: ${registry_url}"
    exit 0
  fi
  echo "Book CDN registry not visible yet: HTTP ${status} attempt ${attempt}/12"
  sleep 10
done
echo "Book CDN registry verification failed: ${registry_url}"
exit 1
