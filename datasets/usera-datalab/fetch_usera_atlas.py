#!/usr/bin/env python3
"""Fetch Atlas de Distribución de Renta indicators for specific territories.

This utility downloads the datasets published in the Atlas de Distribución de Renta
de los Hogares (ADRH) by the Instituto Nacional de Estadística (INE) and keeps only
the rows that match the requested municipality / district / census section codes.

Indicator slugs accepted with ``--indicator`` (defaults to all):

- ``income`` → Renta neta/bruta media (persona y hogar) + mediana de renta.
- ``income_sources`` → Distribución por fuente de ingresos.
- ``inequality`` → Índice de Gini y razón P80/P20.
- ``demographics`` → Indicadores demográficos ADRH.

Example usage targeting Madrid's Usera district (INE code 2807912)::

    python fetch_usera_atlas.py \
        --province Madrid \
        --district 2807912 \
        --output-dir data/usera \
        --indicator income --indicator income_sources --indicator inequality --indicator demographics \
        --from-year 2016 --to-year 2022

Each indicator group is written into a CSV file under the chosen output directory.
SSL certificates are validated via ``certifi`` when available; pass ``--insecure``
only if your network stack relies on a custom certificate that cannot be verified.
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import ssl
import sys
import unicodedata
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

try:  # pragma: no cover - certifi is expected but we keep a fallback
    import certifi  # type: ignore
except ImportError:  # pragma: no cover
    certifi = None  # type: ignore

DEFAULT_OPERATION_URL = (
    "https://www.ine.es/dyngs/INEbase/es/operacion.htm"
    "?c=Estadistica_C&cid=1254736177088&idp=1254735976608"
)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)
CSV_BASE_URL = "https://www.ine.es/jaxiT3/files/t/{fmt}/{table_id}.csv"
SUPPORTED_FMTS = {"csv_bd", "csv_bdsc"}


@dataclass(frozen=True)
class IndicatorConfig:
    slug: str
    group_name: str
    column_name: str
    label_filters: Optional[Sequence[str]] = None

    def accepts_label(self, label: str) -> bool:
        if not self.label_filters:
            return True
        clean_label = label.strip()
        for raw_pattern in self.label_filters:
            pattern = raw_pattern.strip()
            if not pattern:
                continue
            if pattern.endswith("*"):
                if clean_label.startswith(pattern[:-1]):
                    return True
            elif clean_label == pattern:
                return True
        return False


INDICATOR_CONFIGS: Dict[str, IndicatorConfig] = {
    "income": IndicatorConfig(
        slug="income",
        group_name="Indicadores de renta media y mediana",
        column_name="Indicadores de renta media y mediana",
        label_filters=(
            "Renta neta media por persona",
            "Renta bruta media por persona",
            "Renta bruta media por hogar",
            "Mediana de la renta por unidad de consumo",
        ),
    ),
    "income_sources": IndicatorConfig(
        slug="income_sources",
        group_name="Distribución por fuente de ingresos",
        column_name="Distribución por fuente de ingresos",
        label_filters=("Fuente de ingreso:*",),
    ),
    "inequality": IndicatorConfig(
        slug="inequality",
        group_name="Índice de Gini y Distribución de la renta P80/P20",
        column_name="Índice de Gini y Distribución de la renta P80/P20",
        label_filters=("Índice de Gini", "Distribución de la renta P80/P20"),
    ),
    "demographics": IndicatorConfig(
        slug="demographics",
        group_name="Indicadores demográficos",
        column_name="Indicadores demográficos",
    ),
}


class _AtlasIndexParser(HTMLParser):
    """Parse the ADRH operation page to map indicator sections to table IDs."""

    def __init__(self) -> None:
        super().__init__()
        self.current_section: Optional[str] = None
        self._capture_section = False
        self._capture_name = False
        self._pending_id: Optional[str] = None
        self.mapping: Dict[str, Dict[str, str]] = defaultdict(dict)

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_map = dict(attrs)
        if tag == "span" and attr_map.get("class") == "title":
            self._capture_section = True
            return
        if tag == "a":
            anchor_id = attr_map.get("id", "")
            if anchor_id.startswith("t_") and self.current_section:
                self._pending_id = anchor_id[2:]
                self._capture_name = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "span" and self._capture_section:
            self._capture_section = False
        if tag == "a" and self._capture_name:
            self._capture_name = False
            self._pending_id = None

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if not text:
            return
        if self._capture_section:
            self.current_section = text
            return
        if self._capture_name and self.current_section and self._pending_id:
            label = text.rstrip(".").strip()
            if label:
                self.mapping[self.current_section][label] = self._pending_id
            self._capture_name = False
            self._pending_id = None


def get_ssl_context(insecure: bool = False) -> ssl.SSLContext:
    if insecure:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE  # type: ignore[attr-defined]
        return ctx
    if certifi is not None:
        return ssl.create_default_context(cafile=certifi.where())  # type: ignore[arg-type]
    return ssl.create_default_context()


def fetch_operation_mapping(url: str, context: ssl.SSLContext) -> Dict[str, Dict[str, str]]:
    request = urllib.request.Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    with urllib.request.urlopen(request, context=context) as response:
        html = response.read().decode("utf-8", errors="ignore")
    parser = _AtlasIndexParser()
    parser.feed(html)
    if not parser.mapping:
        raise RuntimeError("No se pudo extraer el índice de tablas desde la página de la operación ADRH.")
    return parser.mapping


def normalize_name(text: str) -> str:
    nfkd = unicodedata.normalize("NFD", text)
    without_accents = "".join(ch for ch in nfkd if unicodedata.category(ch) != "Mn")
    return without_accents.lower().replace(".", "").replace(",", "").strip()


def resolve_table_id(mapping: Dict[str, str], province: str) -> str:
    target = normalize_name(province)
    lookup = {normalize_name(name): table_id for name, table_id in mapping.items()}
    if target in lookup:
        return lookup[target]
    for name_norm, table_id in lookup.items():
        if target in name_norm:
            return table_id
    options = ", ".join(sorted(mapping.keys())) or "<vacío>"
    raise KeyError(
        f"La provincia '{province}' no aparece en la operación ADRH. Opciones detectadas: {options}"
    )


def build_filters(args: argparse.Namespace) -> Dict[str, set]:
    return {
        "Municipios": set(args.municipality or []),
        "Distritos": set(args.district or []),
        "Secciones": set(args.section or []),
    }


def split_code_name(cell: str) -> Tuple[Optional[str], Optional[str]]:
    cell = (cell or "").strip()
    if not cell:
        return None, None
    parts = cell.split(None, 1)
    code = parts[0]
    name = parts[1].strip() if len(parts) > 1 else ""
    return code, name or None


def parse_value(raw: str) -> Optional[float]:
    raw = (raw or "").strip()
    if not raw or raw in {".", "..", "..."}:
        return None
    normalized = raw.replace(".", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


class YearSelector:
    def __init__(self, years: Optional[Sequence[int]] = None, start: Optional[int] = None, end: Optional[int] = None) -> None:
        self._years = {int(y) for y in years} if years else None
        self._start = int(start) if start else None
        self._end = int(end) if end else None

    def allows(self, value: str) -> bool:
        value = (value or "").strip()
        if not value.isdigit():
            return self._years is None and self._start is None and self._end is None
        year = int(value)
        if self._years is not None and year not in self._years:
            return False
        if self._start is not None and year < self._start:
            return False
        if self._end is not None and year > self._end:
            return False
        return True


def iter_table_rows(
    table_id: str,
    context: ssl.SSLContext,
    *,
    fmt: str,
    delimiter: str = "\t",
    encoding: str = "utf-8-sig",
) -> Iterator[Dict[str, str]]:
    if fmt not in SUPPORTED_FMTS:
        raise ValueError(f"Formato no soportado: {fmt}")
    url = CSV_BASE_URL.format(fmt=fmt, table_id=table_id)
    request = urllib.request.Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    try:
        with urllib.request.urlopen(request, context=context) as response:
            text_stream = io.TextIOWrapper(response, encoding=encoding, newline="")
            reader = csv.reader(text_stream, delimiter=delimiter)
            try:
                headers = next(reader)
            except StopIteration:
                return
            if headers:
                headers[0] = headers[0].lstrip("\ufeff")
            for row in reader:
                if len(row) != len(headers):
                    continue
                yield {headers[idx]: value for idx, value in enumerate(row)}
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"No se pudo descargar la tabla {table_id} ({exc.code}).") from exc


def row_matches_filters(row: Dict[str, str], filters: Dict[str, set]) -> bool:
    for field, codes in filters.items():
        if not codes:
            continue
        code, _ = split_code_name(row.get(field, ""))
        if code is None or code not in codes:
            return False
    return True


def transform_row(row: Dict[str, str], column_name: str) -> Dict[str, Optional[str]]:
    municipality_code, municipality_name = split_code_name(row.get("Municipios", ""))
    district_code, district_name = split_code_name(row.get("Distritos", ""))
    section_code, section_name = split_code_name(row.get("Secciones", ""))
    indicator_label = (row.get(column_name, "") or "").strip()
    period = (row.get("Periodo", "") or "").strip()
    raw_value = (row.get("Total", "") or "").strip()
    value = parse_value(raw_value)
    return {
        "indicator": indicator_label or None,
        "year": period or None,
        "raw_value": raw_value or None,
        "value": value,
        "municipality_code": municipality_code,
        "municipality_name": municipality_name,
        "district_code": district_code,
        "district_name": district_name,
        "section_code": section_code,
        "section_name": section_name,
    }


def write_csv(target: Path, rows: List[Dict[str, Optional[str]]]) -> None:
    fieldnames = [
        "indicator",
        "year",
        "raw_value",
        "value",
        "municipality_code",
        "municipality_name",
        "district_code",
        "district_name",
        "section_code",
        "section_name",
    ]
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: ("" if row.get(key) is None else row.get(key)) for key in fieldnames})
    logging.info("Guardado %d filas en %s", len(rows), target)


def build_suffix(filters: Dict[str, set], province: str) -> str:
    parts: List[str] = []
    if filters.get("Municipios"):
        parts.append("municipio-" + "-".join(sorted(filters["Municipios"])))
    if filters.get("Distritos"):
        parts.append("distrito-" + "-".join(sorted(filters["Distritos"])))
    if filters.get("Secciones"):
        parts.append("seccion-" + "-".join(sorted(filters["Secciones"])))
    if not parts:
        parts.append(f"provincia-{normalize_name(province).replace(' ', '-')}")
    return "_".join(parts)


def collect_indicator_rows(
    *,
    table_id: str,
    config: IndicatorConfig,
    context: ssl.SSLContext,
    filters: Dict[str, set],
    year_selector: YearSelector,
    fmt: str,
) -> List[Dict[str, Optional[str]]]:
    rows: List[Dict[str, Optional[str]]] = []
    for raw_row in iter_table_rows(table_id, context, fmt=fmt):
        if not row_matches_filters(raw_row, filters):
            continue
        indicator_label = (raw_row.get(config.column_name, "") or "").strip()
        if not config.accepts_label(indicator_label):
            continue
        period = raw_row.get("Periodo", "") or ""
        if not year_selector.allows(period):
            continue
        rows.append(transform_row(raw_row, config.column_name))
    return rows


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--province", required=True, help="Nombre de la provincia tal y como aparece en la operación ADRH")
    parser.add_argument("--municipality", "-m", action="append", help="Código INE de municipio")
    parser.add_argument("--district", "-d", action="append", help="Código INE de distrito")
    parser.add_argument("--section", "-s", action="append", help="Código INE de sección censal")
    parser.add_argument(
        "--indicator",
        "-i",
        action="append",
        choices=sorted(INDICATOR_CONFIGS.keys()),
        help="Indicadores a descargar (por defecto, todos)",
    )
    parser.add_argument("--year", "-y", action="append", type=int, help="Año específico a conservar (repetible)")
    parser.add_argument("--from-year", type=int, dest="from_year", help="Primer año a incluir")
    parser.add_argument("--to-year", type=int, dest="to_year", help="Último año a incluir")
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=Path("data/usera"),
        help="Directorio donde se guardarán los CSV filtrados",
    )
    parser.add_argument("--fmt", choices=sorted(SUPPORTED_FMTS), default="csv_bd", help="Formato de descarga INE")
    parser.add_argument("--insecure", action="store_true", help="Deshabilita la verificación SSL (no recomendado)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Activa logging detallado")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(message)s")

    indicators = args.indicator or list(INDICATOR_CONFIGS.keys())
    filters = build_filters(args)
    year_selector = YearSelector(years=args.year, start=args.from_year, end=args.to_year)
    context = get_ssl_context(args.insecure)

    logging.info("Descargando índice de tablas ADRH…")
    mapping = fetch_operation_mapping(DEFAULT_OPERATION_URL, context)

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    for slug in indicators:
        config = INDICATOR_CONFIGS[slug]
        section_tables = mapping.get(config.group_name)
        if not section_tables:
            logging.error("No se encontró la sección '%s' en el índice ADRH", config.group_name)
            continue
        try:
            table_id = resolve_table_id(section_tables, args.province)
        except KeyError as exc:
            logging.error(str(exc))
            continue
        logging.info("Procesando %s (tabla %s)…", config.group_name, table_id)
        rows = collect_indicator_rows(
            table_id=table_id,
            config=config,
            context=context,
            filters=filters,
            year_selector=year_selector,
            fmt=args.fmt,
        )
        suffix = build_suffix(filters, args.province)
        target_path = output_dir / f"{config.slug}_{suffix}.csv"
        write_csv(target_path, rows)

    logging.info("Hecho.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
