#!/usr/bin/env python3
"""Fetch CalAIR historical data for yesterday and dump it to JSON/CSV files.

This script is a trimmed version of :mod:`fetch_calair_fin_dia` that defaults to
fetching "ayer" (yesterday) and stores the resulting payload under
``data/calair/<fecha>/``.  Three variants are written: the raw payload as JSON,
a standard CSV and a flattened CSV where nested structures are expanded.

Usage::

    python scripts/fetch_calair_ayer.py           # fetch yesterday (Madrid)
    python scripts/fetch_calair_ayer.py --date 2024-01-31

"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List
from zoneinfo import ZoneInfo
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import ssl

API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_historico.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flatten(obj: Dict[str, Any], prefix: str = "", sep: str = ".") -> Dict[str, Any]:
    """Recursively flatten a nested mapping/list structure."""
    items: Dict[str, Any] = {}
    for key, value in obj.items():
        new_key = f"{prefix}{sep}{key}" if prefix else key
        if isinstance(value, dict):
            items.update(_flatten(value, new_key, sep))
        elif isinstance(value, list):
            for idx, sub in enumerate(value):
                sub_key = f"{new_key}{sep}{idx}"
                if isinstance(sub, dict):
                    items.update(_flatten(sub, sub_key, sep))
                else:
                    items[sub_key] = sub
        else:
            items[new_key] = value
    return items


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
    """Tries to find a list of dicts inside the payload."""
    if isinstance(payload, list) and (not payload or isinstance(payload[0], dict)):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "result", "results", "items", "rows"):
            val = payload.get(key)
            if isinstance(val, list) and (not val or isinstance(val[0], dict)):
                return val
    return []


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def fetch(date: str) -> Any:
    """Fetch raw payload for ``date`` (YYYY-MM-DD) from the API."""
    payload = {
        "fecha_ini": f"{date}T00:00:00",
        "fecha_fin": f"{date}T23:59:59",
    }
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        API_URL,
        data=data,
        headers={
            "User-Agent": "calair-ayer/1.0",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    ctx = ssl.create_default_context()
    try:
        with urlopen(req, timeout=90, context=ctx) as resp:
            raw = resp.read()
        try:
            return json.loads(raw)
        except Exception:
            return []
    except (HTTPError, URLError, ssl.SSLError) as exc:  # pragma: no cover - network errors
        print(f"⚠️  Unable to fetch data: {exc}")
        return []


def determine_date(arg_date: str | None) -> str:
    tz = ZoneInfo("Europe/Madrid")
    if arg_date:
        return datetime.strptime(arg_date, "%Y-%m-%d").date().isoformat()
    yesterday = datetime.now(tz).date() - timedelta(days=1)
    return yesterday.isoformat()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch CalAIR daily data")
    parser.add_argument("--date", help="Fecha YYYY-MM-DD (por defecto: ayer)")
    args = parser.parse_args(argv)

    date_str = determine_date(args.date)

    # Fetch
    payload = fetch(date_str)
    rows = _extract_rows(payload)

    # Prepare output paths
    out_dir = Path("data/calair") / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    base = out_dir / f"calair_ayer_{timestamp}"

    # Write JSON
    json_path = base.with_suffix(".json")
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    # Write CSV / flat CSV
    csv_path = base.with_suffix(".csv")
    flat_csv_path = base.with_suffix(".flat.csv")
    _write_csv(csv_path, rows)
    flat_rows = [_flatten(r) for r in rows]
    _write_csv(flat_csv_path, flat_rows)

    print(f"Escrito: {json_path}")
    print(f"Escrito: {csv_path}")
    print(f"Escrito: {flat_csv_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
