#!/usr/bin/env python3
from __future__ import annotations
import json, csv, sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"

def fetch(url: str) -> tuple[dict | list, bytes]:
    req = Request(url, headers={"User-Agent": "github-action-calair/1.3"})
    with urlopen(req, timeout=90) as resp:
        raw = resp.read()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"No se pudo parsear JSON: {e}")
        return data, raw

def extract_rows(payload: dict | list) -> list[dict]:
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

def safe_fieldnames(rows: list[dict]) -> list[str]:
    if not rows: return []
    keys = list(rows[0].keys())
    seen = set(keys)
    for r in rows[1:]:
        for k in r.keys():
            if k not in seen:
                seen.add(k); keys.append(k)
    return keys

# -------- Tipado sencillo --------
def infer_value_type(v) -> str:
    if v is None: return "null"
    if isinstance(v, bool): return "boolean"
    if isinstance(v, (int, float)): return "number"
    # todo lo demás como string (incluye fechas ISO)
    return "string"

def infer_column_types(rows: list[dict], fieldnames: list[str]) -> list[str]:
    types = []
    for col in fieldnames:
        col_type = None  # priorizamos: boolean/number/string; null no decide tipo
        for r in rows:
            v = r.get(col, None)
            t = infer_value_type(v)
            if t == "string":
                col_type = "string"; break  # string domina
            if t == "number":
                if col_type != "string":
                    col_type = "number"
            elif t == "boolean":
                if col_type is None:
                    col_type = "boolean"
        if col_type is None:
            col_type = "null"
        types.append(col_type)
    return types

def write_json(path: Path, obj):
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_csv_with_types(path: Path, rows: list[dict]):
    """CSV con fila 1=cabeceras, fila 2=tipos, resto=datos."""
    fieldnames = safe_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        if not fieldnames:
            # CSV vacío si no hay datos ni claves
            f.write("")
            return
        w = csv.DictWriter(f, fieldnames=fieldnames)
        # cabeceras
        w.writeheader()
        # fila de tipos
        types = infer_column_types(rows, fieldnames)
        f.write(",".join(types) + "\n")
        # datos
        if rows:
            w.writerows(rows)

def write_csv_plain(path: Path, rows: list[dict]):
    """CSV plano (sin fila de tipos). Útil para history.csv."""
    fieldnames = safe_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        if not fieldnames:
            f.write("")
            return
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        if rows:
            w.writerows(rows)

def append_history(history_csv: Path, rows: list[dict]):
    if not rows:
        print("ℹ️ No se añaden filas a history.csv (0 filas).")
        return
    fieldnames = safe_fieldnames(rows)
    new_file = not history_csv.exists()
    with history_csv.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"📚 history.csv: {'creado' if new_file else 'actualizado'} (+{len(rows)} filas).")

def main() -> int:
    # Timestamp UTC
    now_utc = datetime.now(timezone.utc)
    dt = now_utc.strftime("%Y-%m-%d")
    ts = now_utc.strftime("%Y-%m-%dT%H-%M-%SZ")

    # Directorios destino
    day_dir = Path("data/calair") / dt
    day_dir.mkdir(parents=True, exist_ok=True)
    hist_csv = Path("data/calair/history.csv")

    # Paths
    stamped_json = day_dir / f"calair_tiemporeal_{ts}.json"
    latest_json  = day_dir / "latest.json"
    stamped_csv  = day_dir / f"calair_tiemporeal_{ts}.csv"
    latest_csv   = day_dir / "latest.csv"

    # Fetch
    try:
        payload, raw = fetch(API_URL)
        print(f"✅ Fetch OK: {len(raw)} bytes. Tipo raíz: {type(payload).__name__}")
    except Exception as e:
        err = {"error": str(e), "when": ts, "url": API_URL}
        write_json(latest_json, err)
        write_json(stamped_json, err)
        write_csv_with_types(latest_csv, [])
        write_csv_with_types(stamped_csv, [])
        print(f"❌ Error fetch: {e}")
        return 1

    # JSON siempre
    write_json(stamped_json, payload)
    write_json(latest_json, payload)
    print(f"💾 JSON: {stamped_json.name}, {latest_json.name}")

    # CSV con tipos (stamped & latest)
    rows = extract_rows(payload)
    print(f"🧮 Filas detectadas: {len(rows)}")
    write_csv_with_types(stamped_csv, rows)
    write_csv_with_types(latest_csv, rows)
    print(f"💾 CSV: {stamped_csv.name}, {latest_csv.name}")

    # Histórico plano (sin fila de tipos)
    append_history(hist_csv, rows)

    return 0

if __name__ == "__main__":
    sys.exit(main())
