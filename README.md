# Datasets
Catálogo de datasets y automatizaciones.

## Flujos automatizados

- `fetch-calair.yml`: descarga CalAIR tiempo real y escribe `data/calair` (JSON, CSV ancho y `latest.flat.csv`).
  - Comienza a las 23:00 Europe/Madrid y repite cada 15 min hasta ~01:45.
  - Si el fichero del día sale vacío, reintenta y aplica fallback usando el último `latest.flat.csv` no vacío.

- `fetch_decide_proposals.yml`: descarga CSV de propuestas de Decide Madrid, aplica filtro y genera resumen.
  - Usa `scripts/decide_madrid_filter.py` y `scripts/decide_madrid_summary.py`.
  - Puerta de tiempo a las 23:59 Europe/Madrid.

- `update-tangible-climate-calendar.yml`: descarga un ICS público y actualiza `tangible-climate-calendar/calendar.csv` de forma horaria.
