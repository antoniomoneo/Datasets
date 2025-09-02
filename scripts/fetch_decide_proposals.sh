#!/usr/bin/env bash
set -euo pipefail

URL="https://decide.madrid.es/system/api/proposals.csv"
OUTDIR="decide-madrid"
mkdir -p "$OUTDIR"

LATEST="$OUTDIR/proposals_latest.csv"
TMPFILE="$(mktemp)"
HDRS="$(mktemp)"

echo "Descargando CSV desde: $URL"

try_download() {
  local ua="$1"
  curl -sSL --fail --compressed --http1.1 \
    --retry 3 --retry-delay 2 --retry-all-errors \
    -D "$HDRS" -o "$TMPFILE" \
    -H "User-Agent: $ua" \
    -H "Accept: text/csv, text/plain;q=0.9, */*;q=0.1" \
    -H "Accept-Language: es-ES,es;q=0.9,en;q=0.8" \
    -H "Connection: keep-alive" \
    -H "Referer: https://decide.madrid.es/" \
    "$URL"
}

# Primer intento con Safari macOS, segundo con Chrome
if ! try_download "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"; then
  echo "Primer intento falló; probando con Chrome UA" >&2
  try_download "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
fi

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

# ¿Cambió el contenido con respecto al último?
if [[ -f "$LATEST" ]] && diff -q "$TMPFILE" "$LATEST" >/dev/null 2>&1; then
  echo "Sin cambios respecto a proposals_latest.csv. No se hace commit."
  rm -f "$TMPFILE"
  exit 0
fi

# Sustituye el último
mv "$TMPFILE" "$LATEST"
echo "Actualizado $LATEST"

# Generar resumen si cambia
echo "Generando resumen..."
python3 scripts/decide_madrid_summary.py \
  --in "$LATEST" \
  --compare-git \
  --out-json "$OUTDIR/proposals_summary.json" \
  --out-md "$OUTDIR/proposals_summary.md"

# Prepara commit si está en un repo git
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git add "$LATEST" "$OUTDIR/proposals_summary.json" "$OUTDIR/proposals_summary.md"
  ROWS=$(wc -l < "$LATEST" | tr -d ' ')
  git -c user.name="github-actions[bot]" \
      -c user.email="41898282+github-actions[bot]@users.noreply.github.com" \
      commit -m "chore(decide-madrid): update proposals (latest, ${ROWS} rows)" || {
        echo "No hay cambios para commitear."
        exit 0
      }
else
  echo "No estás en un repo git: archivo guardado en $LATEST."
fi

echo "Hecho."

