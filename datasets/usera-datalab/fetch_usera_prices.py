#!/usr/bin/env python3
"""Build housing price datasets for Madrid's Usera district.

This utility pulls monthly sale and rent price series from the Banco de Datos
of the Ayuntamiento de Madrid (Idealista data) and writes normalized CSV files
with monthly and yearly averages for the Usera district and its barrios.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import requests

BASE_URL = "https://servpub.madrid.es/CSEBD_WBINTER"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)
REQUEST_TIMEOUT = 45
MONTHS_ORDER = [
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
]
MONTH_TO_INDEX = {name: idx for idx, name in enumerate(MONTHS_ORDER, start=1)}



@dataclass
class ValueInfo:
    id: str
    label: str
    dependency: str
    flag: str
    extra: Optional[str] = None


@dataclass
class VariableInfo:
    id: str
    name: str
    dependency: str
    values: List[ValueInfo] = field(default_factory=list)

    def label_map(self) -> Dict[str, ValueInfo]:
        return {val.label.strip(): val for val in self.values}


class BancoDeDatosSeries:
    """Minimal client for Banco de Datos series downloads."""

    def __init__(self, series_id: str, session: Optional[requests.Session] = None) -> None:
        self.series_id = series_id
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    def fetch_selection_page(self) -> str:
        resp = self.session.get(
            f"{BASE_URL}/seleccionSerie.html",
            params={"numSerie": self.series_id},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.text

    def set_filters(self, row_vars: Sequence[str], col_vars: Sequence[str], value_ids: Sequence[str]) -> None:
        params = {"varFilas": " ".join(row_vars), "varColumnas": " ".join(col_vars)}
        resp = self.session.get(
            f"{BASE_URL}/setearFiltroS.html",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        params = {"valores": "-".join(value_ids)}
        resp = self.session.get(
            f"{BASE_URL}/setearFiltroValor.html",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

    def download_csv(self) -> bytes:
        resp = self.session.post(
            f"{BASE_URL}/detalleSerie.html",
            data={"generarCsv": "generarCsv"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.content


def parse_variables(script: str) -> Dict[str, VariableInfo]:
    """Parse variable/value declarations embedded in the selection page."""

    variables: Dict[str, VariableInfo] = {}
    current: Optional[VariableInfo] = None

    var_re = re.compile(
        r"varTmp\s*=\s*new\s+variable\s*\(\s*\"(?P<id>\d+)\"\s*,\s*\"(?P<name>[^\"]+)\"\s*,\s*\"(?P<dep>[^\"]*)\"(?:\s*,\s*\"(?P<num>[^\"]*)\")?\s*\);"
    )
    val_re = re.compile(
        r"valTmp\s*=\s*new\s+valor\s*\(\s*\"(?P<id>\d+)\"\s*,\s*\"(?P<label>[^\"]+)\"\s*,\s*\"(?P<dep>[^\"]*)\"\s*,\s*\"(?P<flag>[^\"]*)\"(?:\s*,\s*\"(?P<num>[^\"]*)\")?\s*\);"
    )

    for line in script.splitlines():
        line = line.strip()
        if not line:
            continue
        var_match = var_re.search(line)
        if var_match:
            current = VariableInfo(
                id=var_match.group("id"),
                name=var_match.group("name"),
                dependency=var_match.group("dep"),
            )
            variables[current.id] = current
            continue
        val_match = val_re.search(line)
        if val_match and current is not None:
            current.values.append(
                ValueInfo(
                    id=val_match.group("id"),
                    label=val_match.group("label"),
                    dependency=val_match.group("dep"),
                    flag=val_match.group("flag"),
                    extra=val_match.group("num"),
                )
            )

    return variables


def find_variable_by_name(variables: Dict[str, VariableInfo], name: str) -> VariableInfo:
    needle = name.lower()
    for var in variables.values():
        if var.name.lower() == needle:
            return var
    raise KeyError(f"Variable '{name}' not found in series metadata")


def select_year_ids(variable: VariableInfo, years: Iterable[int]) -> List[str]:
    mapping = variable.label_map()
    selected: List[str] = []
    for year in years:
        label = str(year)
        if label in mapping:
            selected.append(mapping[label].id)
    if not selected:
        raise ValueError("None of the requested years were available in the series metadata")
    return selected


def select_month_ids(variable: VariableInfo) -> List[str]:
    month_ids = []
    for val in variable.values:
        label = val.label.strip()
        if label in MONTH_TO_INDEX:
            month_ids.append((MONTH_TO_INDEX[label], val.id))
    month_ids.sort(key=lambda item: item[0])
    return [val_id for _, val_id in month_ids]


def select_district_id(variable: VariableInfo, district_label: str) -> str:
    for val in variable.values:
        if val.label.strip().lower() == district_label.lower():
            return val.id
    raise ValueError(f"District '{district_label}' not found in metadata")


def select_barrio_ids(variable: VariableInfo, district_value_id: str) -> List[ValueInfo]:
    barrios = [val for val in variable.values if val.dependency == district_value_id]
    if not barrios:
        raise ValueError("No barrio entries found for district value id '%s'" % district_value_id)
    return barrios


def parse_banco_csv(csv_bytes: bytes) -> List[List[str]]:
    text = csv_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text), delimiter=';')
    return [row for row in reader if any(cell.strip() for cell in row)]



def extract_monthly_records(
    rows: List[List[str]],
    metric: str,
    series_id: str,
    series_label: str,
    methodology: str,
    source_label: str,
    include_barrio: bool,
) -> List[Dict[str, object]]:
    """Transform raw CSV rows into normalized monthly records."""

    header_idx = None
    for idx, row in enumerate(rows):
        if any(cell in MONTH_TO_INDEX for cell in row):
            header_idx = idx
            break
    if header_idx is None:
        raise ValueError("Unable to locate header row with month names")

    header_row = rows[header_idx]
    descriptor_count = 0
    while descriptor_count < len(header_row) and not header_row[descriptor_count]:
        descriptor_count += 1
    month_names = [cell for cell in header_row[descriptor_count:] if cell]

    if include_barrio:
        descriptor_names = ["year", "district", "barrio"]
    else:
        descriptor_names = ["year", "district"]
    if descriptor_count != len(descriptor_names):
        descriptor_names = [f"descriptor_{i}" for i in range(descriptor_count)]

    records: List[Dict[str, object]] = []

    for row in rows[header_idx + 1 :]:
        if not row or not row[0].strip().isdigit():
            continue
        descriptors = row[:descriptor_count]
        month_values = row[descriptor_count : descriptor_count + len(month_names)]
        descriptor_map = {name: value.strip() for name, value in zip(descriptor_names, descriptors)}
        try:
            year = int(descriptor_map.get("year", ""))
        except ValueError:
            continue
        district_label = descriptor_map.get("district", "").strip()
        barrio_label = descriptor_map.get("barrio", "").strip() if include_barrio else None

        for month_name, raw_value in zip(month_names, month_values):
            value_clean = raw_value.strip()
            price: Optional[float]
            if not value_clean or value_clean == ".." or value_clean == "0":
                price = None
            else:
                try:
                    price = float(value_clean.replace(',', '.'))
                except ValueError:
                    price = None
            month_idx = MONTH_TO_INDEX.get(month_name)
            if month_idx is None:
                continue
            territory_level = "district"
            territory_code = None
            territory_name = district_label
            if include_barrio and barrio_label:
                if barrio_label.lower() == district_label.lower():
                    territory_level = "district"
                    code_match = re.match(r"(\d+)", district_label)
                    territory_code = code_match.group(1) if code_match else None
                    territory_name = district_label
                else:
                    territory_level = "barrio"
                    code_match = re.match(r"(\d+)", barrio_label)
                    territory_code = code_match.group(1) if code_match else None
                    territory_name = barrio_label
            else:
                code_match = re.match(r"(\d+)", district_label)
                territory_code = code_match.group(1) if code_match else None

            date_value = dt.date(year, month_idx, 1)
            records.append(
                {
                    "date": date_value.isoformat(),
                    "year": year,
                    "month": month_idx,
                    "metric": metric,
                    "series_id": series_id,
                    "series_label": series_label,
                    "methodology": methodology,
                    "source": source_label,
                    "territory_level": territory_level,
                    "territory_code": territory_code,
                    "territory_name": territory_name,
                    "price_eur_m2": price,
                }
            )
    return records


def aggregate_yearly(records: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    buckets: Dict[tuple, Dict[str, object]] = {}
    for rec in records:
        value = rec.get("price_eur_m2")
        if value is None:
            continue
        key = (
            rec["metric"],
            rec["series_id"],
            rec["series_label"],
            rec["methodology"],
            rec["source"],
            rec["territory_level"],
            rec["territory_code"],
            rec["territory_name"],
            rec["year"],
        )
        bucket = buckets.setdefault(key, {"sum": 0.0, "count": 0})
        bucket["sum"] += float(value)
        bucket["count"] += 1

    yearly_rows: List[Dict[str, object]] = []
    for key, agg in buckets.items():
        metric, series_id, series_label, methodology, source, level, code, name, year = key
        avg = agg["sum"] / agg["count"] if agg["count"] else None
        yearly_rows.append(
            {
                "year": year,
                "metric": metric,
                "series_id": series_id,
                "series_label": series_label,
                "methodology": methodology,
                "source": source,
                "territory_level": level,
                "territory_code": code,
                "territory_name": name,
                "observations": agg["count"],
                "average_price_eur_m2": round(avg, 2) if avg is not None else None,
            }
        )

    yearly_rows.sort(key=lambda row: (row["metric"], row["territory_level"], row["territory_code"] or "", row["year"]))
    return yearly_rows


def ensure_output_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, object]]) -> None:
    ensure_output_path(path)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


@dataclass
class SeriesConfig:
    series_id: str
    years: Sequence[int]
    metric: str
    series_label: str
    methodology: str
    source_label: str
    include_barrio: bool = True


SALES_SERIES: List[SeriesConfig] = [
    SeriesConfig(
        series_id="0504030000151",
        years=tuple(range(2010, 2019)),
        metric="sale_price",
        series_label="Precio vivienda segunda mano (€/m2) - metodología Idealista 2000-2018",
        methodology="Idealista (antigua metodología)",
        source_label="Banco de Datos del Ayuntamiento de Madrid",
    ),
    SeriesConfig(
        series_id="0504030000202",
        years=tuple(range(2019, 2023)),
        metric="sale_price",
        series_label="Precio vivienda segunda mano (€/m2) - 2019-2022",
        methodology="Idealista (revisión 2019)",
        source_label="Banco de Datos del Ayuntamiento de Madrid",
    ),
    SeriesConfig(
        series_id="0504030000153",
        years=tuple(range(2023, 2026)),
        metric="sale_price",
        series_label="Precio vivienda segunda mano (€/m2) - metodología actual",
        methodology="Idealista (metodología vigente)",
        source_label="Banco de Datos del Ayuntamiento de Madrid",
    ),
]

RENT_SERIES: List[SeriesConfig] = [
    SeriesConfig(
        series_id="0504030000213",
        years=tuple(range(2010, 2025)),
        metric="rent_price",
        series_label="Precio alquiler vivienda (€/m2) - distritos",
        methodology="Idealista (alquiler distritos)",
        source_label="Banco de Datos del Ayuntamiento de Madrid",
        include_barrio=False,
    )
]


def fetch_series_records(config: SeriesConfig, district_label: str) -> List[Dict[str, object]]:
    client = BancoDeDatosSeries(config.series_id)
    selection_html = client.fetch_selection_page()
    variables = parse_variables(selection_html)

    year_var = find_variable_by_name(variables, "Año")
    district_var = find_variable_by_name(variables, "Distrito")

    time_var: Optional[VariableInfo] = None
    for candidate in ("Mes", "Trimestre", "Semestre"):
        try:
            time_var = find_variable_by_name(variables, candidate)
            break
        except KeyError:
            continue
    if time_var is None:
        raise KeyError("No supported time variable (Mes/Trimestre/Semestre) found")

    year_ids = select_year_ids(year_var, config.years)
    district_id = select_district_id(district_var, district_label)
    time_label_map, time_value_ids = build_time_mapping(time_var)

    value_ids: List[str] = []
    value_ids.extend(year_ids)
    value_ids.append(district_id)

    row_vars: List[str] = [year_var.id, district_var.id]

    include_barrio = config.include_barrio and any(var.name.lower() == "barrio" for var in variables.values())
    if include_barrio:
        barrio_var = find_variable_by_name(variables, "Barrio")
        barrios = select_barrio_ids(barrio_var, district_id)
        value_ids.extend(val.id for val in barrios)
        row_vars.append(barrio_var.id)

    value_ids.extend(time_value_ids)

    client.set_filters(row_vars=row_vars, col_vars=[time_var.id], value_ids=value_ids)
    csv_bytes = client.download_csv()
    rows = parse_banco_csv(csv_bytes)
    records = extract_monthly_records(
        rows,
        metric=config.metric,
        series_id=config.series_id,
        series_label=config.series_label,
        methodology=config.methodology,
        source_label=config.source_label,
        include_barrio=include_barrio,
        time_label_map=time_label_map,
    )
    return records


def build_datasets(district_label: str) -> List[Dict[str, object]]:
    all_records: List[Dict[str, object]] = []
    for cfg in SALES_SERIES + RENT_SERIES:
        logging.info("Fetching series %s (%s)", cfg.series_id, cfg.series_label)
        records = fetch_series_records(cfg, district_label=district_label)
        all_records.extend(records)
    all_records.sort(key=lambda rec: (rec["metric"], rec["territory_level"], rec["territory_code"] or "", rec["date"]))
    return all_records


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Usera housing price datasets")
    parser.add_argument(
        "--district-label",
        default="12. Usera",
        help="Label of the district as it appears in Banco de Datos (default: '12. Usera')",
    )
    parser.add_argument(
        "--monthly-output",
        default="data/usera_housing_prices_monthly.csv",
        help="Path to write the normalized monthly dataset",
    )
    parser.add_argument(
        "--yearly-output",
        default="data/usera_housing_prices_yearly.csv",
        help="Path to write yearly averages",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (e.g. INFO, DEBUG)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s: %(message)s")

    monthly_records = build_datasets(district_label=args.district_label)
    monthly_path = Path(args.monthly_output)
    yearly_path = Path(args.yearly_output)

    write_csv(
        monthly_path,
        [
            "date",
            "year",
            "month",
            "metric",
            "series_id",
            "series_label",
            "methodology",
            "source",
            "territory_level",
            "territory_code",
            "territory_name",
            "price_eur_m2",
        ],
        monthly_records,
    )

    yearly_records = aggregate_yearly(monthly_records)
    write_csv(
        yearly_path,
        [
            "year",
            "metric",
            "series_id",
            "series_label",
            "methodology",
            "source",
            "territory_level",
            "territory_code",
            "territory_name",
            "observations",
            "average_price_eur_m2",
        ],
        yearly_records,
    )

    logging.info("Monthly dataset written to %s", monthly_path)
    logging.info("Yearly dataset written to %s", yearly_path)


if __name__ == "__main__":
    main()
