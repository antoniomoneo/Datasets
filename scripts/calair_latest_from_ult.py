#!/usr/bin/env python3
from __future__ import annotations
import json, csv
from urllib.request import Request, urlopen
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, Any, List

URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal_ult.json?pageSize=5000"


def http_get_json(url: str):
    req = Request(url, headers={"User-Agent": "datasets-calair/1.0"})
    with urlopen(req, timeout=60) as resp:
        return json.load(resp)


def ymd_madrid_minus_1():
    y = datetime.now(ZoneInfo("Europe/Madrid")) - timedelta(days=1)
    return f"{y:%Y}", f"{y:%m}", f"{y:%d}"


def station_id_from(r: Dict[str, Any]) -> str:
    pm = r.get("PUNTO_MUESTREO")
    if pm:
        return str(pm)
    prov = str(r.get("PROVINCIA", "")).zfill(2)
    muni = str(r.get("MUNICIPIO", "")).zfill(3)
    est = str(r.get("ESTACION", "")).zfill(3)
    return f"{prov}{muni}{est}"


def rows_for_record(r: Dict[str, Any], Y: str, M: str, D: str) -> List[Dict[str, Any]]:
    sid = station_id_from(r)
    mag = r.get("MAGNITUD", "")
    out: List[Dict[str, Any]] = []
    for h in range(1, 25):
        hh = f"{h:02d}"
        vflag = r.get(f"V{hh}")
        val = r.get(f"H{hh}")
        if vflag == "V" and val not in (None, ""):
            dt = datetime(int(Y), int(M), int(D), h - 1, 0, 0, tzinfo=ZoneInfo("Europe/Madrid"))
            out.append(
                {
                    "station_id": sid,
                    "title": "",
                    "relation": "",
                    "magnitud": mag,
                    "valor": val,
                    "fecha": dt.isoformat(),
                }
            )
    return out


def main() -> int:
    data = http_get_json(URL)
    recs = data.get("records") or []
    Y, M, D = ymd_madrid_minus_1()
    filt = [r for r in recs if r.get("ANO") == Y and r.get("MES") == M and r.get("DIA") == D]
    rows: List[Dict[str, Any]] = []
    for r in filt:
        rows.extend(rows_for_record(r, Y, M, D))

    outp = Path("datasets/calidad-aire/latest.flat.csv")
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["station_id", "title", "relation", "magnitud", "valor", "fecha"])
        w.writeheader()
        if rows:
            w.writerows(rows)
    print(f"Wrote {len(rows)} rows for {Y}-{M}-{D} to {outp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
