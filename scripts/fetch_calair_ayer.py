#!/usr/bin/env python3
from __future__ import annotations
import json, csv, sys, re, math, time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import ssl
from zoneinfo import ZoneInfo
import certifi

API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_historico.json"

# Fecha de AYER en Europa/Madrid
YESTERDAY = (datetime.now(ZoneInfo("Europe/Madrid")) - timedelta(days=1)).strftime("%Y-%m-%d")
API_PAYLOAD = {"where": {"fecha": YESTERDAY}, "pageSize": 5000}

def http_post_json(url: str, payload: dict, timeout: int = 90, retries: int = 5, backoff: float = 1.5):
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=data,
        headers={
            "User-Agent": "github-action-calair/2.1",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    # SSL context con certifi
    ctx = ssl.create_default_context(cafile=certifi.where())

    last_err = None
    for i in range(retries):
        try:
            with urlopen(req, timeout=timeout, context=ctx) as resp:
                raw = resp.read()
            return json.loads(raw), raw
        except (HTTPError, URLError, ssl.SSLError) as e:
            last_err = e
            # backoff exponencial con jitter pequeño
            sleep_s = (backoff ** i) + 0.1
            print(f"⚠️  Intento {i+1}/{retries} fallido: {e}. Reintentando en {sleep_s:.1f}s...", flush=True)
            time.sleep(sleep_s)
    raise last_err

def extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload if (not payload or isinstance(payload[0], dict)) else []
    if isinstance(payload, dict):
        for key in ("data","result","results","items","rows"):
            val = payload.get(key)
            if isinstance(val, list) and (not val or isinstance(val[0], dict)):
                return val or []
        for v in payload.values():
            if isinstance(v, list) and (not v or isinstance(v[0], dict))):
                return v or []
    return []

def safe_fieldnames(rows: List[Dict[str, Any]]) -> List[str]:
    if not rows: return []
    out, seen = [], set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k); out.append(k)
    return out

def infer_value_type(v) -> str:
    if v is None: return "null"
    if isinstance(v, bool): return "boolean"
    if isinstance(v, (int, float)):
        try:
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return "null"
            return "number"
        except Exception:
            return "string"
    return "string"

def infer_column_types(rows: List[Dict[str, Any]], fns: List[str]) -> List[str]:
    types = []
    for col in fns:
        col_type = None
        for r in rows:
            t = infer_value_type(r.get(col))
            if t == "string": col_type = "string"; break
            if t == "number" and col_type != "string": col_type = "number"
            if t == "boolean" and col_type is None: col_type = "boolean"
        types.append(col_type or "null")
    return types

def write_csv_with_types(path: Path, rows: List[Dict[str, Any]]):
    fns = safe_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        if not fns:
            f.write("")  # vacío para señalizar ejecución sin datos
            return
        w = csv.DictWriter(f, fieldnames=fns)
        w.writeheader()
        f.write(",".join(infer_column_types(rows, fns)) + "\n")
        if rows: w.writerows(rows)

def normalize_numeric_hours(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
    long_rows = []
    hour_cols = [f"{i:02d}" for i in range(1, 25)]
    for r in rows:
        base = {k: v for k, v in r.items() if not re.match(r"^[HV]\d{2}$", k)}
        for hh in hour_cols:
            h_key, v_key = f"H{hh}", f"V{hh}"
            val, val_valid = r.get(h_key), r.get(v_key)
            if drop_empty and (val is None and (v_key not in r or val_valid in ("", None))):
                continue
            new_row = dict(base)
            new_row["Hora"] = int(hh)
            new_row["Valor"] = val
            new_row["Validacion"] = val_valid
            long_rows.append(new_row)
    return long_rows

def main() -> int:
    # Fecha (ayer) y rutas
    dt = YESTERDAY
    ts = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%Y-%m-%dT%H-%M-%S%z")

    day_dir = Path("data/calair") / dt
    day_dir.mkdir(parents=True, exist_ok=True)
    stamped_json = day_dir / f"calair_historico_{ts}.json"
    stamped_csv  = day_dir / f"calair_historico_{ts}.csv"
    stamped_flat_csv = day_dir / f"calair_historico_{ts}.flat.csv"

    try:
        payload, raw = http_post_json(API_URL, API_PAYLOAD)
        print(f"✅ Fetch histórico OK ({dt}): {len(raw)} bytes")
    except Exception as e:
        err = {"error": str(e), "when": ts, "url": API_URL, "payload": API_PAYLOAD}
        stamped_json.write_text(json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"❌ Error fetch: {e}")
        return 1

    stamped_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    rows = extract_rows(payload)
    print(f"🧮 Filas detectadas: {len(rows)}")
    rows = normalize_numeric_hours(rows)

    write_csv_with_types(stamped_csv, rows)
    rows_flat = unpivot_hours_to_long(rows, drop_empty=True)
    write_csv_with_types(stamped_flat_csv, rows_flat)

    print(f"💾 CSVs guardados: {stamped_csv.name}, {stamped_flat_csv.name}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
