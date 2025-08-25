#!/usr/bin/env python3
from __future__ import annotations
import json
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"

def fetch(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "github-action-calair/1.0"})
    try:
        with urlopen(req, timeout=60) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            raw = resp.read()
            return json.loads(raw)
    except HTTPError as e:
        raise RuntimeError(f"HTTPError: {e.code} {e.reason}") from e
    except URLError as e:
        raise RuntimeError(f"URLError: {e.reason}") from e

def save_csv(data: dict, path: Path):
    # Se asume que la API devuelve {"data": [ {...}, {...}, ... ]}
    rows = data.get("data")
    if not rows:
        print("⚠️ No se encontraron filas en la respuesta")
        return
    
    # Tomamos las claves del primer registro como cabecera
    fieldnames = list(rows[0].keys())
    
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main() -> int:
    data = fetch(API_URL)

    # Marca de tiempo en UTC para trazabilidad
    now_utc = datetime.now(timezone.utc)
    dt = now_utc.strftime("%Y-%m-%d")
    ts = now_utc.strftime("%Y-%m-%dT%H-%M-%SZ")

    # Carpeta destino por fecha
    out_dir = Path("data/calair") / dt
    out_dir.mkdir(parents=True, exist_ok=True)

    # Paths para JSON
    stamped_json = out_dir / f"calair_tiemporeal_{ts}.json"
    latest_json = out_dir / "latest.json"

    # Paths para CSV
    stamped_csv = out_dir / f"calair_tiemporeal_{ts}.csv"
    latest_csv = out_dir / "latest.csv"

    # Guardar JSON
    with stamped_json.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    with latest_json.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Guardar CSV
    save_csv(data, stamped_csv)
    save_csv(data, latest_csv)

    print(f"✅ Guardado JSON en {stamped_json} y {latest_json}")
    print(f"✅ Guardado CSV en {stamped_csv} y {latest_csv}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
