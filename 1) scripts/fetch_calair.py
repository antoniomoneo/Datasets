#!/usr/bin/env python3
from __future__ import annotations
import json
import os
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

def main() -> int:
    data = fetch(API_URL)

    # Marca de tiempo en UTC para trazabilidad
    now_utc = datetime.now(timezone.utc)
    dt = now_utc.strftime("%Y-%m-%d")
    ts = now_utc.strftime("%Y-%m-%dT%H-%M-%SZ")

    # Carpeta destino con estructura por fecha
    out_dir = Path("data/calair") / dt
    out_dir.mkdir(parents=True, exist_ok=True)

    # Guarda dos archivos:
    # 1) con timestamp (histórico granular)
    # 2) "latest.json" sobreescribible del mismo día (útil para lecturas rápidas)
    stamped_path = out_dir / f"calair_tiemporeal_{ts}.json"
    latest_path = out_dir / "latest.json"

    # Asegura UTF-8 y formato legible
    with stamped_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    with latest_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved: {stamped_path}")
    print(f"Updated: {latest_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
