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


HEADERS = [
    "PROVINCIA",
    "MUNICIPIO",
    "ESTACION",
    "MAGNITUD",
    "PUNTO_MUESTREO",
    "ANO",
    "MES",
    "DIA",
    "Hora",
    "Valor",
    "Validacion",
]


def flatten_record(r: Dict[str, Any]) -> List[Dict[str, Any]]:
    base = {
        "PROVINCIA": r.get("PROVINCIA", ""),
        "MUNICIPIO": r.get("MUNICIPIO", ""),
        "ESTACION": r.get("ESTACION", ""),
        "MAGNITUD": r.get("MAGNITUD", ""),
        "PUNTO_MUESTREO": r.get("PUNTO_MUESTREO", ""),
        "ANO": r.get("ANO", ""),
        "MES": r.get("MES", ""),
        "DIA": r.get("DIA", ""),
    }
    out: List[Dict[str, Any]] = []
    for h in range(1, 25):
        hh = f"{h:02d}"
        val = r.get(f"H{hh}")
        if val in (None, ""):
            continue
        row = dict(base)
        row["Hora"] = h  # 1..24, without leading zero
        row["Valor"] = val
        row["Validacion"] = r.get(f"V{hh}", "")
        out.append(row)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Filter calair_tiemporeal_ult to yesterday and write flattened CSV")
    ap.add_argument("--input", help="Path to input JSON (if omitted, fetch from URL)")
    ap.add_argument("--output", default="datasets/data/calair/latest.flat.csv", help="Output CSV path")
    args = ap.parse_args()

    if args.input:
        data = json.loads(Path(args.input).read_text(encoding="utf-8"))
    else:
        data = http_get_json(URL)

    recs = data.get("records") or []
    Y, M, D = ymd_madrid_minus_1()
    filtered = [r for r in recs if r.get("ANO") == Y and r.get("MES") == M and r.get("DIA") == D]

    rows: List[Dict[str, Any]] = []
    for r in filtered:
        rows.extend(flatten_record(r))

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        if rows:
            w.writerows(rows)
    print(f"Wrote {len(rows)} flattened rows for {Y}-{M}-{D} to {outp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
