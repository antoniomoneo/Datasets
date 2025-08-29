# Datasets
Dataset catalogue

## Flujos automatizados

- `fetch_calair_ayer.yml`: descarga diariamente los datos de CalAIR del día
  anterior ejecutando `fetch_calair_fin_dia.py`. Se programa a las **01:30 UTC**
  (≈03:30 Madrid) una vez cerrado el día y **antes** del flujo de subida a GCP.
- `calair_fin_dia_to_gcs.yml`: publica el último `.flat.csv` en GCS a las
  **02:15 UTC** (≈04:15 Madrid).
