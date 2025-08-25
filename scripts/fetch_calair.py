#!/usr/bin/env python3
from __future__ import annotations
import json, csv, sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"

def fetch(url: str) -> tuple[dict | list, bytes]:
    req = Request(url, headers={"User-Agent": "github-action-calair/1.2"})
    with urlopen(req, timeout=90) as resp:
        raw = resp.read()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"No se pudo parsear JSON: {e}")
        return data, raw

def extract_rows(payload: dict | list) -> list[dict]:
    """
    Intenta encontrar filas tipo list[dict] en payload.
    - Si payload ya es una lista de dicts, úsala.
    - Si es dict, busca la primera clave cuyo valor sea list[dict].
    """
    if isinstance(payload, list):
        return payload if (payload and isinstance(payload[0], dict)) else []

    if isinstance(payload, dict):
        # 1) campos comunes
        for key in ("data", "result", "results", "items", "rows"):
            val = payload.get(key)
            if isinstance(val, list) and (not val or isinstance(val[0], dict)):
                return val or []
        # 2) fallback: primera lista de dicts que encontremos
        for v in payload.values():
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return v or []
    return []

def safe_fieldnames(rows: list[dict]) -> list[str]:
    if rows:
        # Unión ordenada de claves (primera fila como base)
        keys = list(rows[0].keys())
        seen = set(keys)
        for r in rows[1:]:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
        return keys
    return []

def write_json(path: Path, obj):
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_csv(path: Path, rows: list[dict]):
    fieldnames = safe_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        if fieldnames:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        else:
            # CSV vacío (sin cabecera) si no hay datos ni claves
            f.write("")

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
        # Incluso si falla el fetch, escribimos artefactos mínimos para diagnosticar
        err = {"error": str(e), "when": ts, "url": API_URL}
        write_json(latest_json, err)
        write_json(stamped_json, err)
        write_csv(latest_csv, [])
        write_csv(stamped_csv, [])
        print(f"❌ Error fetch: {e}")
        # Devolvemos código de error para que falle el job (visible en Actions)
        return 1

    # Guardar JSON crudo (siempre)
    write_json(stamped_json, payload)
    write_json(latest_json, payload)
    print(f"💾 JSON: {stamped_json.name}, {latest_json.name}")

    # Extraer filas y guardar CSVs del día (siempre)
    rows = extract_rows(payload)
    print(f"🧮 Filas detectadas: {len(rows)}")
    write_csv(stamped_csv, rows)
    write_csv(latest_csv, rows)
    print(f"💾 CSV: {stamped_csv.name}, {latest_csv.name}")

    # Acumular histórico
    append_history(hist_csv, rows)

    return 0

if __name__ == "__main__":
    sys.exit(main())
