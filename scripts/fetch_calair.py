#!/usr/bin/env python3
from __future__ import annotations
import json, csv, sys, re, math, os
from datetime import datetime, timezone
import time
from pathlib import Path
from urllib.request import Request, urlopen
from typing import Dict, List, Tuple, Any

# ========= Config =========
API_URL = (
    "https://datos.madrid.es/egob/CalidadDelAire/"
    "listcalair_tiemporeal_ult?formato=json"
)

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

def filter_latest_day(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filtra las filas para quedarse solo con el día más reciente."""
    if not rows:
        return rows
    def ymd(r: Dict[str, Any]) -> tuple[int, int, int]:
        try:
            return int(r.get("ANO", 0)), int(r.get("MES", 0)), int(r.get("DIA", 0))
        except Exception:
            return (0, 0, 0)
    last = max(ymd(r) for r in rows)
    return [r for r in rows if ymd(r) == last]

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

def _search_last_nonempty_latest_flat(base: Path) -> Path | None:
    """Busca el último `latest.flat.csv` no vacío en subcarpetas fechadas.
    Devuelve el path si lo encuentra, si no None.
    """
    if not base.exists():
        return None
    candidates: list[tuple[str, Path]] = []
    for child in base.iterdir():
        if not child.is_dir():
            continue
        # Directorios tipo YYYY-MM-DD
        name = child.name
        try:
            datetime.strptime(name, "%Y-%m-%d")
        except ValueError:
            continue
        p = child / "latest.flat.csv"
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            candidates.append((name, p))
    if not candidates:
        return None
    # Ordenar por fecha descendente
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates[0][1]


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

    # 1) Descarga datos tiempo real con reintentos si el CSV plano queda vacío
    max_retries = int((os.getenv("CALAIR_MAX_RETRIES") or "2").strip() or 2)
    wait_seconds = int((os.getenv("CALAIR_WAIT_SECONDS") or "180").strip() or 180)
    attempt = 0
    last_err: Exception | None = None
    payload = None
    rows: List[Dict[str, Any]] = []
    while attempt <= max_retries:
        try:
            payload, raw = http_get_json(API_URL)
            print(f"✅ Fetch tiempo real OK: {len(raw)} bytes (attempt {attempt+1}/{max_retries+1})")
        except Exception as e:
            last_err = e
            print(f"❌ Error fetch (attempt {attempt+1}/{max_retries+1}): {e}")
            payload = None

        if payload is not None:
            # 2) Guarda JSON crudo (siempre que haya respuesta)
            stamped_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            # 3) Extrae filas
            rows = extract_rows(payload)
            print(f"🧮 Filas detectadas: {len(rows)}")

            # Catálogo y procesamiento normal solo si hay contenido útil
            if rows:
                break

        attempt += 1
        if attempt <= max_retries:
            print(f"⏳ latest.flat.csv vacío o sin filas; reintento en {wait_seconds}s...")
            time.sleep(wait_seconds)

    if payload is None and last_err is not None:
        # Fallo duro de red en todos los intentos: dejamos diagnóstico mínimo y salimos
        err = {"error": str(last_err), "when": ts, "url": API_URL}
        stamped_json.write_text(json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8")
        latest_json.write_text(json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8")
        for p in (stamped_csv, latest_csv, stamped_flat_csv, latest_flat_csv):
            write_csv_plain(p, [])
        print("❌ Abortado tras reintentos por error de red.")
        return 1

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

    # 7) Quedarse solo con el último día disponible
    rows = filter_latest_day(rows)

    # 8) CSV anchos
    write_csv_plain(stamped_csv, rows)
    write_csv_plain(latest_csv, rows)
    print(f"💾 CSV ancho: {stamped_csv.name}, {latest_csv.name}")

    # 9) Versión larga: Hora / Valor / Validacion
    rows_flat = unpivot_hours_to_long(rows, drop_empty=True)
    if not rows_flat:
        # Fallback: usar el último latest.flat.csv no vacío de días anteriores
        cand = _search_last_nonempty_latest_flat(Path("data/calair"))
        if cand:
            print(f"🔁 latest.flat.csv vacío. Usando fallback: {cand}")
            # Copiamos contenido de cand a stamped_flat, latest_flat y raíz
            data = cand.read_text(encoding="utf-8")
            stamped_flat_csv.write_text(data, encoding="utf-8")
            latest_flat_csv.write_text(data, encoding="utf-8")
            root_latest_flat = Path("data/calair/latest.flat.csv")
            root_latest_flat.write_text(data, encoding="utf-8")
            # history_flat no se actualiza en este caso (evitar duplicados falsos)
            print("✅ Fallback aplicado y copias actualizadas.")
            return 0
        else:
            print("⚠️ latest.flat.csv vacío y sin fallback disponible.")
            # Escribimos explícitamente vacíos como diagnóstico
            write_csv_plain(stamped_flat_csv, [])
            write_csv_plain(latest_flat_csv, [])
            root_latest_flat = Path("data/calair/latest.flat.csv")
            write_csv_plain(root_latest_flat, [])
            return 0

    write_csv_plain(stamped_flat_csv, rows_flat)
    write_csv_plain(latest_flat_csv, rows_flat)
    print(f"💾 CSV largo: {stamped_flat_csv.name}, {latest_flat_csv.name}")

    # 8bis) Copia a nivel raíz para pisar la del día anterior
    root_latest_flat = Path("data/calair/latest.flat.csv")
    write_csv_plain(root_latest_flat, rows_flat)
    print(f"💾 Copia actualizada: {root_latest_flat}")

    # 9) Históricos (planos, sin fila de tipos)
    append_history(hist_csv, rows)
    append_history_flat(hist_flat_csv, rows_flat)

    return 0

if __name__ == "__main__":
    sys.exit(main())
