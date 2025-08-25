#!/usr/bin/env python3
from __future__ import annotations
import json, csv, sys, re
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from typing import Dict, List, Tuple, Any

API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"

# Opcional: catálogo de estaciones online (déjalo vacío si no tienes uno)
STATIONS_URL = ""  # por ejemplo: "https://.../estaciones.json" o ".../estaciones.csv"

# Fichero local opcional con el mapeo de estaciones
LOCAL_STATIONS_CSV = Path("data/meta/stations.csv")

# ---------- utilidades de red ----------
def http_get_json(url: str, timeout: int = 90) -> Any:
    req = Request(url, headers={"User-Agent": "github-action-calair/1.4"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw), raw

def http_get_bytes(url: str, timeout: int = 90) -> bytes:
    req = Request(url, headers={"User-Agent": "github-action-calair/1.4"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()

# ---------- extracción de filas ----------
def extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload if (not payload or isinstance(payload[0], dict)) else []
    if isinstance(payload, dict):
        for key in ("data", "result", "results", "items", "rows"):
            val = payload.get(key)
            if isinstance(val, list) and (not val or isinstance(val[0], dict)):
                return val or []
        for v in payload.values():
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return v or []
    return []

def safe_fieldnames(rows: List[Dict[str, Any]]) -> List[str]:
    if not rows: return []
    keys = list(rows[0].keys())
    seen = set(keys)
    for r in rows[1:]:
        for k in r.keys():
            if k not in seen:
                seen.add(k); keys.append(k)
    return keys

# ---------- inferencia de tipos para CSV (fila 2) ----------
def infer_value_type(v) -> str:
    if v is None: return "null"
    if isinstance(v, bool): return "boolean"
    if isinstance(v, (int, float)): return "number"
    return "string"

def infer_column_types(rows: List[Dict[str, Any]], fieldnames: List[str]) -> List[str]:
    types = []
    for col in fieldnames:
        col_type = None
        for r in rows:
            v = r.get(col, None)
            t = infer_value_type(v)
            if t == "string":
                col_type = "string"; break
            if t == "number":
                if col_type != "string": col_type = "number"
            elif t == "boolean":
                if col_type is None: col_type = "boolean"
        if col_type is None: col_type = "null"
        types.append(col_type)
    return types

# ---------- escritura CSV ----------
def write_csv_with_types(path: Path, rows: List[Dict[str, Any]]):
    fieldnames = safe_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        if not fieldnames:
            f.write("")
            return
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        types = infer_column_types(rows, fieldnames)
        f.write(",".join(types) + "\n")
        if rows:
            w.writerows(rows)

def write_csv_plain(path: Path, rows: List[Dict[str, Any]]):
    fieldnames = safe_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        if not fieldnames:
            f.write("")
            return
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        if rows:
            w.writerows(rows)

# ---------- aplanado (flatten) ----------
def flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    flat = {}
    for k, v in d.items():
        nk = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flat.update(flatten_dict(v, nk, sep))
        elif isinstance(v, list):
            # indexamos listas: campo.0, campo.1, ...
            for i, item in enumerate(v):
                ik = f"{nk}{sep}{i}"
                if isinstance(item, dict):
                    flat.update(flatten_dict(item, ik, sep))
                else:
                    flat[ik] = item
        else:
            flat[nk] = v
    return flat

def flatten_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [flatten_dict(r) for r in rows]

# ---------- mapeo estaciones ----------
STATION_CODE_KEYS = [
    "estacion", "station", "idEstacion", "idestacion", "cod_estacion", "codigo_estacion", "code"
]
STATION_NAME_KEYS = [
    "nombre", "name", "station_name", "nombre_estacion", "denominacion", "label"
]

def detect_station_code_key(sample: Dict[str, Any]) -> str | None:
    for k in STATION_CODE_KEYS:
        if k in sample: return k
    # fallback: heurística por regex
    for k in sample.keys():
        if re.search(r"(estaci[oó]n|station).*id|cod", k, flags=re.I):
            return k
    return None

def detect_station_name_key(sample: Dict[str, Any]) -> str | None:
    for k in STATION_NAME_KEYS:
        if k in sample: return k
    for k in sample.keys():
        if re.search(r"(nombre|name)", k, flags=re.I):
            return k
    return None

def load_stations_from_local() -> Dict[str, str]:
    if not LOCAL_STATIONS_CSV.exists(): return {}
    mp = {}
    with LOCAL_STATIONS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # columnas esperadas: station_code, station_name (flexible)
        # detectamos las columnas
        headers = reader.fieldnames or []
        code_col = None
        name_col = None
        for h in headers:
            hl = h.lower()
            if code_col is None and re.search(r"(station_)?code|cod(_|)estaci", hl):
                code_col = h
            if name_col is None and re.search(r"(station_)?name|nombre", hl):
                name_col = h
        if code_col is None or name_col is None:
            # intento por nombres exactos
            if "station_code" in headers: code_col = "station_code"
            if "station_name" in headers: name_col = "station_name"
        if code_col is None or name_col is None:
            print("⚠️ stations.csv no tiene columnas reconocibles (se esperan station_code/station_name).")
            return {}
        for row in reader:
            code = str(row.get(code_col, "")).strip()
            name = str(row.get(name_col, "")).strip()
            if code:
                mp[code] = name
    print(f"🔎 Mapeo estaciones (local): {len(mp)} entradas.")
    return mp

def load_stations_from_url(url: str) -> Dict[str, str]:
    if not url: return {}
    try:
        if url.lower().endswith(".json"):
            payload, _ = http_get_json(url)
            rows = extract_rows(payload)
        else:
            raw = http_get_bytes(url)
            text = raw.decode("utf-8", errors="replace")
            rdr = csv.DictReader(text.splitlines())
            rows = list(rdr)
        if not rows:
            return {}
        # detectar columnas code y name
        code_key = detect_station_code_key(rows[0]) or "estacion"
        name_key = detect_station_name_key(rows[0]) or "nombre"
        mp = {}
        for r in rows:
            code = str(r.get(code_key, "")).strip()
            name = str(r.get(name_key, "")).strip()
            if code:
                mp[code] = name
        print(f"🔎 Mapeo estaciones (URL): {len(mp)} entradas.")
        return mp
    except Exception as e:
        print(f"⚠️ No se pudo cargar STATIONS_URL: {e}")
        return {}

def enrich_with_station_names(rows: List[Dict[str, Any]], station_map: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str | None]:
    if not rows: return rows, None
    sample = rows[0]
    code_key = detect_station_code_key(sample)
    if not code_key:
        # nada que mapear
        return rows, None
    for r in rows:
        code = str(r.get(code_key, "")).strip()
        r["station_name"] = station_map.get(code, "")
    return rows, code_key

# ---------- histórico ----------
def append_history(history_csv: Path, rows: List[Dict[str, Any]]):
    if not rows:
        print("ℹ️ No se añaden filas a history.csv (0 filas).")
        return
    fieldnames = safe_fieldnames(rows)
    new_file = not history_csv.exists()
    with history_csv.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file: w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"📚 history.csv: {'creado' if new_file else 'actualizado'} (+{len(rows)} filas).")

def append_history_flat(history_flat_csv: Path, rows_flat: List[Dict[str, Any]]):
    if not rows_flat:
        print("ℹ️ No se añaden filas a history_flat.csv (0 filas).")
        return
    fieldnames = safe_fieldnames(rows_flat)
    new_file = not history_flat_csv.exists()
    with history_flat_csv.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file: w.writeheader()
        for r in rows_flat:
            w.writerow(r)
    print(f"📚 history_flat.csv: {'creado' if new_file else 'actualizado'} (+{len(rows_flat)} filas).")

# ---------- main ----------
def main() -> int:
    # Timestamp UTC
    now_utc = datetime.now(timezone.utc)
    dt = now_utc.strftime("%Y-%m-%d")
    ts = now_utc.strftime("%Y-%m-%dT%H-%M-%SZ")

    # Directorios/paths
    day_dir = Path("data/calair") / dt
    day_dir.mkdir(parents=True, exist_ok=True)
    hist_csv = Path("data/calair/history.csv")
    hist_flat_csv = Path("data/calair/history_flat.csv")

    stamped_json = day_dir / f"calair_tiemporeal_{ts}.json"
    latest_json  = day_dir / "latest.json"
    stamped_csv  = day_dir / f"calair_tiemporeal_{ts}.csv"
    latest_csv   = day_dir / "latest.csv"
    stamped_flat_csv = day_dir / f"calair_tiemporeal_{ts}.flat.csv"
    latest_flat_csv  = day_dir / "latest.flat.csv"

    # Fetch payload
    try:
        payload, raw = http_get_json(API_URL)
        print(f"✅ Fetch OK: {len(raw)} bytes. Tipo raíz: {type(payload).__name__}")
    except Exception as e:
        err = {"error": str(e), "when": ts, "url": API_URL}
        with latest_json.open("w", encoding="utf-8") as f: json.dump(err, f, ensure_ascii=False, indent=2)
        with stamped_json.open("w", encoding="utf-8") as f: json.dump(err, f, ensure_ascii=False, indent=2)
        write_csv_with_types(latest_csv, [])
        write_csv_with_types(stamped_csv, [])
        write_csv_with_types(latest_flat_csv, [])
        write_csv_with_types(stamped_flat_csv, [])
        print(f"❌ Error fetch: {e}")
        return 1

    # Guardar JSON
    with stamped_json.open("w", encoding="utf-8") as f: json.dump(payload, f, ensure_ascii=False, indent=2)
    with latest_json.open("w", encoding="utf-8") as f: json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON: {stamped_json.name}, {latest_json.name}")

    # Extraer filas
    rows = extract_rows(payload)
    print(f"🧮 Filas detectadas: {len(rows)}")

    # Cargar mapeo de estaciones
    stations_map = load_stations_from_url(STATIONS_URL)
    if not stations_map:
        stations_map = load_stations_from_local()

    # Enriquecer con station_name (si hay code)
    rows, code_key = enrich_with_station_names(rows, stations_map)
    if code_key:
        print(f"🧷 Unión por código de estación '{code_key}' → campo añadido: station_name")
    else:
        print("ℹ️ No se detectó columna de código de estación para la unión (station_name vacío).")

    # CSV diarios (cabecera + fila de tipos)
    write_csv_with_types(stamped_csv, rows)
    write_csv_with_types(latest_csv, rows)
    print(f"💾 CSV: {stamped_csv.name}, {latest_csv.name}")

    # Flatten
    rows_flat = flatten_rows(rows)
    write_csv_with_types(stamped_flat_csv, rows_flat)
    write_csv_with_types(latest_flat_csv, rows_flat)
    print(f"💾 CSV flatten: {stamped_flat_csv.name}, {latest_flat_csv.name}")

    # Históricos (planos, sin fila de tipos)
    append_history(hist_csv, rows)
    append_history_flat(hist_flat_csv, rows_flat)

    return 0

if __name__ == "__main__":
    sys.exit(main())
