#!/usr/bin/env python3
"""Fetch accumulated real-time air quality data for Madrid.

This script calls the public endpoint documented in the OpenAPI spec
provided by the Ayuntamiento de Madrid:
https://datos.madrid.es/egob/catalogo/212504-1-calidad-aire-tiempo-real-acumulado.json

It stores the raw JSON response and a flattened CSV with one row per
station and contaminant measurement.
"""
from __future__ import annotations
import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List
from urllib.request import Request, urlopen

API_URL = "https://datos.madrid.es/egob/catalogo/212504-1-calidad-aire-tiempo-real-acumulado.json"


def fetch_payload(url: str = API_URL) -> Any:
    """Return JSON payload from the API."""
    req = Request(url, headers={"User-Agent": "datasets-fetch/0.1"})
    with urlopen(req, timeout=60) as resp:
        return json.load(resp)


def parse_rows(payload: Any) -> List[Dict[str, Any]]:
    """Flatten '@graph' entries into rows.

    Each row contains station id, title, relation, magnitud, valor and fecha.
    """
    rows: List[Dict[str, Any]] = []
    graph = payload.get("@graph", []) if isinstance(payload, dict) else []
    for station in graph:
        station_id = station.get("@id", "")
        title = station.get("title", "")
        relation = station.get("relation", "")
        measurements = station.get("medicion", [])
        for m in measurements:
            rows.append(
                {
                    "station_id": station_id,
                    "title": title,
                    "relation": relation,
                    "magnitud": m.get("magnitud", ""),
                    "valor": m.get("valor"),
                    "fecha": m.get("fecha", ""),
                }
            )
    return rows


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(output_dir: str) -> int:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    data = fetch_payload()
    rows = parse_rows(data)

    json_path = out / "latest.json"
    csv_path = out / "latest.csv"
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(csv_path, rows)
    print(f"Saved {len(rows)} rows to {csv_path}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch accumulated air quality data")
    parser.add_argument("--output-dir", default="data/calair_accumulated", help="Where to store output files")
    args = parser.parse_args()
    raise SystemExit(main(args.output_dir))
