#!/usr/bin/env bash
set -euo pipefail

# Allow overriding the source URL via environment variable (e.g., proxy)
URL="${DECIDE_MADRID_URL:-https://decide.madrid.es/system/api/proposals.csv}"
OUTDIR="decide-madrid"
mkdir -p "$OUTDIR"

LATEST="$OUTDIR/proposals_latest.csv"
TMPFILE="$(mktemp)"
HDRS="$(mktemp)"
COOKIES="$(mktemp)"

echo "Descargando CSV desde: $URL"

# Prime cookies by visiting site root (helps bypass some CDNs)
BASE_ORIGIN="$(echo "$URL" | sed -E 's#(https?://[^/]+).*#\1/#')"
curl -sSL --fail --compressed --http1.1 \
  --retry 2 --retry-delay 2 \
  -c "$COOKIES" -b "$COOKIES" \
  -D /dev/null \
  -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15" \
  -H "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" \
  -H "Accept-Language: es-ES,es;q=0.9,en;q=0.8" \
  -H "Connection: keep-alive" \
  -H "Referer: $BASE_ORIGIN" \
  "$BASE_ORIGIN" || true

# Download CSV with cookies and stricter headers
curl -sSL --fail --compressed --http1.1 \
  --retry 3 --retry-delay 2 --retry-all-errors \
  -c "$COOKIES" -b "$COOKIES" \
  -D "$HDRS" -o "$TMPFILE" \
  -H "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36" \
  -H "Accept: text/csv, text/plain;q=0.9, */*;q=0.1" \
  -H "Accept-Language: es-ES,es;q=0.9,en;q=0.8" \
  -H "Connection: keep-alive" \
  -H "Sec-Fetch-Site: same-origin" \
  -H "Sec-Fetch-Mode: navigate" \
  -H "Sec-Fetch-Dest: document" \
  -H "Upgrade-Insecure-Requests: 1" \
  -H "Referer: $BASE_ORIGIN" \
  "$URL"

if [[ ! -s "$TMPFILE" ]]; then
  echo "El archivo descargado está vacío. Abortando sin cambios."
  exit 0
fi

# Validación de tipo de contenido y evitar HTML de error
ctype=$(grep -i '^content-type:' "$HDRS" | tail -1 | tr -d '\r' | awk -F': ' '{print tolower($2)}') || true
if [[ -n "$ctype" ]] && ! echo "$ctype" | grep -Eq 'text/csv|application/csv|text/plain'; then
  echo "Tipo de contenido inesperado: $ctype" >&2
  exit 1
fi
if head -c 20 "$TMPFILE" | grep -qi '<html'; then
  echo "Servidor devolvió HTML (posible 403). Abortando sin cambios." >&2
  exit 1
fi

# Apply filter (created_at >= 2024-01-01) and drop description
TMPFILT="$(mktemp)"
python3 scripts/decide_madrid_filter.py \
  --in "$TMPFILE" \
  --out "$TMPFILT" \
  --drop-column description \
  --from-date 2024-01-01

# Check if filtered output changed vs latest
CHANGED=1
if [[ -f "$LATEST" ]] && diff -q "$TMPFILT" "$LATEST" >/dev/null 2>&1; then
  echo "Sin cambios respecto a proposals_latest.csv. No se hace commit."
  CHANGED=0
else
  mv "$TMPFILT" "$LATEST"
  echo "Actualizado $LATEST"
fi

if [[ "$CHANGED" -eq 1 ]]; then
  echo "Generando resumen..."
  python3 scripts/decide_madrid_summary.py \
    --in "$LATEST" \
    --compare-git \
    --out-json "$OUTDIR/proposals_summary.json" \
    --out-md "$OUTDIR/proposals_summary.md"
fi

# Prepara commit si está en un repo git
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git add "$LATEST" "$OUTDIR/proposals_summary.json" "$OUTDIR/proposals_summary.md"
  if [[ "$CHANGED" -eq 1 ]]; then
    ROWS=$(wc -l < "$LATEST" | tr -d ' ')
    git -c user.name="github-actions[bot]" \
        -c user.email="41898282+github-actions[bot]@users.noreply.github.com" \
        commit -m "chore(decide-madrid): update filtered proposals (>=2024, no description) (${ROWS} rows)" || true
  else
    echo "No hay cambios para commitear."
  fi
else
  echo "No estás en un repo git: archivo guardado en $LATEST."
fi

echo "Hecho."
