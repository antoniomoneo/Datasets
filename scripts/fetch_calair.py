#!/usr/bin/env python3
from __future__ import annotations
import json, csv, sys, re, math
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from typing import Dict, List, Tuple, Any

# ========= Config =========
API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"

# Catálogo local de estaciones (ya en tu repo)
LOCAL_STATIONS_CSV = Path("datasets/meta/informacion_estaciones_red_calidad_aire.csv")
LOCAL_STATIONS_GEO = Path("datasets/meta/informacion_estaciones_red_calidad_aire.geo")  # opcional (GeoJSON)

# ========= Utilidades red =========
def http_get_json(url: str, timeout: int = 90) -> tuple[Any, bytes]:
    req = Request(url, headers={"User-Agent": "github-action-calair/1.7"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw), raw

# ========= Payload dinámico =========
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
    keys, seen = [], set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k); keys.append(k)
    return keys

# ========= Tipado CSV (2ª fila) =========
def infer_value_type(v) -> str:
    if v is None: return "null"
    if isinstance(v, bool): return "boolean"
    if isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return "number"
    return "string"

def infer_column_types(rows: List[Dict[str, Any]], fieldnames: List[str]) -> List[str]:
    types = []
    for col in fieldnames:
        col_type = None
        for r in rows:
            t = infer_value_type(r.get(col, None))
            if t == "string": col_type = "string"; break
            if t == "number" and col_type != "string": col_type = "number"
            if t == "boolean" and col_type is None: col_type = "boolean"
        types.append(col_type or "null")
    return types

def write_csv_with_types(path: Path, rows: List[Dict[str, Any]]):
    fns = safe_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        if not fns:
            f.write("")
            return
        w = csv.DictWriter(f, fieldnames=fns)
        w.writeheader()
        f.write(",".join(infer_column_types(rows, fns)) + "\n")
        if rows: w.writerows(rows)

def write_csv_plain(path: Path, rows: List[Dict[str, Any]]):
    fns = safe_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        if not fns:
            f.write("")
            return
        w = csv.DictWriter(f, fieldnames=fns)
        w.writeheader()
        if rows: w.writerows(rows)

# ========= Flatten genérico (por si hay dict/list) =========
def flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    flat = {}
    for k, v in d.items():
        nk = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flat.update(flatten_dict(v, nk, sep))
        elif isinstance(v, list):
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

# ========= Claves estación =========
STATION_CODE_KEYS = [
    "estacion","station","idEstacion","idestacion","cod_estacion","codigo_estacion","code",
    "estacion_codigo","codEstacion","cod_est","cod_estac"
]
STATION_NAME_KEYS = ["nombre","name","station_name","nombre_estacion","denominacion","label","estacion_nombre"]

def detect_station_code_key(sample: Dict[str, Any]) -> str | None:
    for k in STATION_CODE_KEYS:
        if k in sample: return k
    for k in sample.keys():
        if re.search(r"(estaci[oó]n|station).*(id|cod|c[oó]digo)", k, flags=re.I):
            return k
    return None

def detect_station_name_key(sample: Dict[str, Any]) -> str | None:
    for k in STATION_NAME_KEYS:
        if k in sample: return k
    for k in sample.keys():
        if re.search(r"(nombre|name)", k, flags=re.I): return k
    return None

def normalize_station_code(x: Any) -> str:
    if x is None: return ""
    s = str(x).strip()
    return re.sub(r"\s+", "", s)

# ========= Catálogo estaciones (CSV y GeoJSON) =========
def load_stations_csv(p: Path) -> Dict[str, Dict[str, Any]]:
    if not p.exists(): return {}
    with p.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        rows = list(rdr)
    if not rows: return {}
    sample = rows[0]
    code_key = detect_station_code_key(sample) or "estacion_codigo"
    name_key = detect_station_name_key(sample)

    mapping: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        code = normalize_station_code(r.get(code_key, ""))
        if not code: continue
        meta = {f"station_{k}": v for k, v in r.items()}
        meta["station_code_norm"] = code
        if name_key and f"station_{name_key}" in meta:
            meta["station_name"] = meta[f"station_{name_key}"]
        # Normaliza lat/lon si existen
        lat = None; lon = None
        for k, v in r.items():
            kl = k.lower()
            if lat is None and re.search(r"(lat|y|coord.*y)", kl):
                try: lat = float(str(v).replace(",", "."))
                except: pass
            if lon is None and re.search(r"(lon|lng|x|coord.*x)", kl):
                try: lon = float(str(v).replace(",", "."))
                except: pass
        if isinstance(lat, (int,float)): meta["lat"] = lat
        if isinstance(lon, (int,float)): meta["lng"] = lon
        mapping[code] = meta
    print(f"🔎 Estaciones CSV: {len(mapping)} filas ({p})")
    return mapping

def load_stations_geo(p: Path) -> Dict[str, Dict[str, Any]]:
    if not p.exists(): return {}
    try:
        gj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    feats = gj.get("features") or []
    mapping: Dict[str, Dict[str, Any]] = {}
    for ft in feats:
        props = ft.get("properties") or {}
        geom = ft.get("geometry") or {}
        code_key = detect_station_code_key(props) or "estacion_codigo"
        code = normalize_station_code(props.get(code_key, ""))
        if not code: continue
        meta = {f"station_{k}": v for k, v in props.items()}
        meta["station_code_norm"] = code
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            try:
                lon, lat = float(coords[0]), float(coords[1])
                meta["lng"] = lon; meta["lat"] = lat
            except: pass
        name_key = detect_station_name_key(props)
        if name_key and f"station_{name_key}" in meta:
            meta["station_name"] = meta[f"station_{name_key}"]
        mapping[code] = meta
    print(f"🗺️  Estaciones GeoJSON: {len(mapping)} features ({p})")
    return mapping

def merge_station_maps(a: Dict[str, Dict[str, Any]], b: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out = dict(a)
    for code, meta_b in b.items():
        meta = out.get(code, {})
        for k, v in meta_b.items():
            if k not in meta or meta.get(k) in (None, "", "NaN"):
                meta[k] = v
        out[code] = meta
    return out

def load_station_catalog() -> Dict[str, Dict[str, Any]]:
    csv_map = load_stations_csv(LOCAL_STATIONS_CSV)
    geo_map = load_stations_geo(LOCAL_STATIONS_GEO) if LOCAL_STATIONS_GEO.exists() else {}
    return merge_station_maps(csv_map, geo_map) if geo_map else csv_map

# ========= Normalización Hxx y unpivot =========
def normalize_numeric_hours(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convierte H01..H24 a float (o None)."""
    hour_cols = [f"H{str(i).zfill(2)}" for i in range(1, 25)]
    for r in rows:
        for h in hour_cols:
            if h in r:
                val = r[h]
                if val in ("", None, "NaN"):
                    r[h] = None
                else:
                    try:
                        v = float(str(val).replace(",", "."))
                        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                            v = None
                        r[h] = v
                    except Exception:
                        r[h] = None
    return rows

def unpivot_hours_to_long(rows: List[Dict[str, Any]], drop_empty: bool = True) -> List[Dict[str, Any]]:
    """
    Convierte columnas H01..H24 y V01..V24 a formato largo:
    - Hora (1..24)
    - Valor (de Hxx)
    - Validacion (de Vxx)
    Mantiene el resto de columnas (incluidas station_* y lat/lng).
    """
    long_rows = []
    hour_cols = [f"{i:02d}" for i in range(1, 25)]
    for r in rows:
        base = {k: v for k, v in r.items() if not re.match(r"^[HV]\d{2}$", k)}
        for hh in hour_cols:
            h_key = f"H{hh}"
            v_key = f"V{hh}"
            val = r.get(h_key, None)        # ya numérico por normalize_numeric_hours
            val_valid = r.get(v_key, None)  # lo dejamos tal cual venga
            if drop_empty and (val is None and (v_key not in r or r.get(v_key) in ("", None))):
                continue
            new_row = dict(base)
            new_row["Hora"] = int(hh)       # 1..24
            new_row["Valor"] = val
            new_row["Validacion"] = val_valid
            long_rows.append(new_row)
    return long_rows

# ========= Históricos =========
def append_history(history_csv: Path, rows: List[Dict[str, Any]]):
    if not rows:
        print("ℹ️ No se añaden filas a history.csv (0 filas).")
        return
    fns = safe_fieldnames(rows)
    new_file = not history_csv.exists()
    with history_csv.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fns)
        if new_file: w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"📚 history.csv: {'creado' if new_file else 'actualizado'} (+{len(rows)} filas).")

def append_history_flat(history_flat_csv: Path, rows_flat: List[Dict[str, Any]]):
    if not rows_flat:
        print("ℹ️ No se añaden filas a history_flat.csv (0 filas).")
        return
    fns = safe_fieldnames(rows_flat)
    new_file = not history_flat_csv.exists()
    with history_flat_csv.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fns)
        if new_file: w.writeheader()
        for r in rows_flat:
            w.writerow(r)
    print(f"📚 history_flat.csv: {'creado' if new_file else 'actualizado'} (+{len(rows_flat)} filas).")

# ========= Main =========
def main() -> int:
    now_utc = datetime.now(timezone.utc)
    dt = now_utc.strftime("%Y-%m-%d")
    ts = now_utc.strftime("%Y-%m-%dT%H-%M-%SZ")

    # Directorios/paths salida
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

    # 1) Descarga datos tiempo real
    try:
        payload, raw = http_get_json(API_URL)
        print(f"✅ Fetch tiempo real OK: {len(raw)} bytes")
    except Exception as e:
        err = {"error": str(e), "when": ts, "url": API_URL}
        stamped_json.write_text(json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8")
        latest_json.write_text(json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8")
        # Creamos CSV vacíos (diagnóstico)
        for p in (stamped_csv, latest_csv, stamped_flat_csv, latest_flat_csv):
            write_csv_with_types(p, [])
        print(f"❌ Error fetch: {e}")
        return 1

    # 2) Guarda JSON crudo (siempre)
    stamped_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # 3) Extrae filas
    rows = extract_rows(payload)
    print(f"🧮 Filas detectadas: {len(rows)}")

    # 4) Catálogo de estaciones (local)
    station_map = load_station_catalog()

    # 5) Enriquecer con metadatos de estación (station_*, lat, lng)
    rows = rows or []
    if rows:
        sample_key = detect_station_code_key(rows[0])
    else:
        sample_key = None

    def enrich_with_station_meta(rows_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows_in: return rows_in
        code_key = detect_station_code_key(rows_in[0])
        if not code_key: 
            print("ℹ️ No se detectó clave de estación en datos.")
            return rows_in
        for r in rows_in:
            code = normalize_station_code(r.get(code_key, ""))
            meta = station_map.get(code, {})
            for k, v in meta.items():
                if k not in r:
                    r[k] = v
            if "station_name" not in r:
                nm_key = detect_station_name_key(r)
                if nm_key: r["station_name"] = r.get(nm_key, "")
        print(f"🧷 Unión por '{code_key}'. Añadidos 'station_*', 'lat', 'lng' si existen.")
        return rows_in

    rows = enrich_with_station_meta(rows)

    # 6) Normalizar horas a numéricas
    rows = normalize_numeric_hours(rows)

    # 7) CSV anchos (con 2ª fila de tipos)
    write_csv_with_types(stamped_csv, rows)
    write_csv_with_types(latest_csv, rows)
    print(f"💾 CSV ancho: {stamped_csv.name}, {latest_csv.name}")

    # 8) Versión larga: Hora / Valor / Validacion
    rows_flat = unpivot_hours_to_long(rows, drop_empty=True)
    write_csv_with_types(stamped_flat_csv, rows_flat)
    write_csv_with_types(latest_flat_csv, rows_flat)
    print(f"💾 CSV largo: {stamped_flat_csv.name}, {latest_flat_csv.name}")

    # 9) Históricos (planos, sin fila de tipos)
    append_history(hist_csv, rows)
    append_history_flat(hist_flat_csv, rows_flat)

    return 0

if __name__ == "__main__":
    sys.exit(main())
