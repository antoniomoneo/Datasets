#!/usr/bin/env python3
from __future__ import annotations
import argparse
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal_ult.json?pageSize=5000"


def http_get_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": "datasets-calair/1.0"})
    with urlopen(req, timeout=60) as resp:
        return json.load(resp)


def ymd_madrid_minus_1() -> tuple[str, str, str]:
    y = datetime.now(ZoneInfo("Europe/Madrid")) - timedelta(days=1)
    return f"{y:%Y}", f"{y:%m}", f"{y:%d}"


def ensure_fields() -> List[str]:
    base = [
        "PROVINCIA",
        "MUNICIPIO",
        "ESTACION",
        "MAGNITUD",
        "PUNTO_MUESTREO",
        "ANO",
        "MES",
        "DIA",
    ]
    hours = [f"H{i:02d}" for i in range(1, 25)]
    flags = [f"V{i:02d}" for i in range(1, 25)]
    return base + hours + flags


def write_csv(path: Path, rows: List[Dict[str, Any]]):
    fns = ensure_fields()
    # include extra fields if present
    extras: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in fns and k not in extras:
                extras.append(k)
    fieldnames = fns + extras
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def main():
    ap = argparse.ArgumentParser(description="Filter calair_tiemporeal_ult to yesterday and write CSV")
    ap.add_argument("--input", help="Path to input JSON (if omitted, fetch from URL)")
    ap.add_argument("--output", default="datasets/calidad-aire/latest.flat.csv", help="Output CSV path")
    args = ap.parse_args()

    if args.input:
        data = json.loads(Path(args.input).read_text(encoding="utf-8"))
    else:
        data = http_get_json(URL)

    recs = data.get("records") or []
    Y, M, D = ymd_madrid_minus_1()
    filtered = [r for r in recs if r.get("ANO") == Y and r.get("MES") == M and r.get("DIA") == D]

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    write_csv(outp, filtered)
    print(f"Wrote {len(filtered)} records for {Y}-{M}-{D} to {outp}")


if __name__ == "__main__":
    raise SystemExit(main())
