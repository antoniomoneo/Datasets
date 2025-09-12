#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

mkdir -p "decide-madrid"
tmpfile="$(mktemp)"
url="https://decide.madrid.es/system/api/proposals.csv"

echo "[fetch] Downloading proposals.csv..."
if ! curl -sSLo "$tmpfile" \
  --retry 5 --retry-delay 5 --retry-all-errors \
  -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36" \
  -H "Accept: text/csv, */*;q=0.1" \
  -H "Referer: https://decide.madrid.es/" \
  "$url"; then
  echo "[fetch] ERROR: curl failed" >&2
  exit 1
fi

dest="decide-madrid/proposals_latest.csv"
if [ ! -s "$tmpfile" ]; then
  echo "[fetch] ERROR: downloaded file is empty" >&2
  exit 1
fi

changed=1
if [ -f "$dest" ] && cmp -s "$tmpfile" "$dest"; then
  echo "[fetch] No changes detected"
  changed=0
else
  mv "$tmpfile" "$dest"
  echo "[fetch] Updated $dest"
fi

if [ "$changed" -eq 1 ]; then
  echo "[summary] Generating metrics..."
  python3 scripts/decide_madrid_summary.py \
    --in "$dest" \
    --compare-git \
    --out-json decide-madrid/proposals_summary.json \
    --out-md decide-madrid/proposals_summary.md

  echo "[git] Committing changes..."
  git add decide-madrid/proposals_latest.csv decide-madrid/proposals_summary.json decide-madrid/proposals_summary.md
  if ! git diff --cached --quiet; then
    git commit -m "chore(decide-madrid): update proposals (latest)"
    git push
    echo "[git] Changes pushed"
  else
    echo "[git] Nothing to commit after generation"
  fi
fi

echo "[done]"

