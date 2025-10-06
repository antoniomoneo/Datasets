#!/usr/bin/env python3
"""Descarga históricos anuales de comercios desde OpenStreetMap.

El script consulta la API de Overpass con una "foto" por año (31 de diciembre)
y genera un CSV con el nombre del comercio, la tipología según OSM, coordenadas
y año de observación. Por omisión se apunta al distrito de Usera (Madrid),
pero se puede ajustar mediante etiquetas del área.
Se filtran los valores de `amenity` a categorías comerciales habituales
(bares, restaurantes, bancos, farmacias, etc.); usa `--allow-all-amenities`
si quieres incluir todos los amenities o amplía la lista con `--allowed-amenity`.

Ejemplo de uso::

    python fetch_osm_businesses.py \
        --area-tag name=Usera \
        --area-tag boundary=administrative \
        --area-tag admin_level=9 \
        --from-year 2015 --to-year 2024 \
        --output data/osm_usera_comercios.csv
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

try:  # pragma: no cover - certifi es opcional
    import certifi  # type: ignore
except ImportError:  # pragma: no cover
    certifi = None  # type: ignore

DEFAULT_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)
DEFAULT_TIMEOUT = 180
DEFAULT_SLEEP_SECONDS = 5.0
DEFAULT_AREA_TAGS = (
    "name=Usera",
    "boundary=administrative",
    "admin_level=9",
)
DEFAULT_EXTRA_TAGS = (
    "addr:street",
    "addr:housenumber",
    "addr:postcode",
)
DEFAULT_CATEGORY_KEYS = (
    "shop",
    "amenity",
)
DEFAULT_ALLOWED_AMENITIES = (
    "bar",
    "biergarten",
    "cafe",
    "fast_food",
    "food_court",
    "ice_cream",
    "pub",
    "restaurant",
    "nightclub",
    "casino",
    "cinema",
    "theatre",
    "arts_centre",
    "planetarium",
    "studio",
    "internet_cafe",
    "marketplace",
    "bank",
    "bureau_de_change",
    "atm",
    "money_transfer",
    "pharmacy",
    "clinic",
    "dentist",
    "doctors",
    "optician",
    "veterinary",
    "fuel",
    "car_wash",
    "car_rental",
    "car_sharing",
    "charging_station",
    "bicycle_rental",
    "motorcycle_rental",
    "boat_rental",
    "post_office",
    "parcel_locker",
    "copyshop",
    "coworking_space",
    "events_venue",
    "conference_centre",
    "spa",
    "sauna",
    "massage",
)

logger = logging.getLogger("osm")


def build_ssl_context(*, cafile: Optional[Path], insecure: bool) -> ssl.SSLContext:
    """Crear el contexto TLS a utilizar en las peticiones HTTP."""

    if insecure:
        logger.warning(
            "TLS sin validación habilitado (--insecure); úsalo solo para depuración."
        )
        return ssl._create_unverified_context()

    context = ssl.create_default_context()

    if cafile:
        context.load_verify_locations(cafile=str(cafile))
    elif certifi is not None:
        try:
            context.load_verify_locations(cafile=certifi.where())
        except Exception:  # pragma: no cover - diagnóstico opcional
            logger.debug("No se pudo cargar certifi", exc_info=True)

    return context


def parse_area_tags(raw_tags: Sequence[str]) -> List[tuple[str, str]]:
    parsed: List[tuple[str, str]] = []
    for raw in raw_tags:
        if "=" not in raw:
            raise argparse.ArgumentTypeError(
                f"Etiqueta de área inválida '{raw}'. Usa el formato clave=valor."
            )
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise argparse.ArgumentTypeError(
                f"Etiqueta de área inválida '{raw}'. No puede haber claves o valores vacíos."
            )
        parsed.append((key, value))
    if not parsed:
        raise argparse.ArgumentTypeError("Debe indicarse al menos una etiqueta de área.")
    return parsed


def build_area_selector(area_tags: Sequence[tuple[str, str]]) -> str:
    filters = "".join(f"[\"{k}\"=\"{v}\"]" for k, v in area_tags)
    return f"area{filters}->.searchArea;"


def build_overpass_query(
    *,
    area_selector: str,
    category_keys: Sequence[str],
    iso_date: str,
    timeout: int,
) -> str:
    key_filters = "\n".join(
        f"  nwr[\"{key}\"](area.searchArea);" for key in category_keys
    )
    query = f"""
[out:json][timeout:{timeout}][date:\"{iso_date}\"];
{area_selector}
(
{key_filters}
);
out center tags;
""".strip()
    return query


def execute_overpass_query(
    *,
    query: str,
    overpass_url: str,
    timeout: int,
    sleep_seconds: float,
    ssl_context: ssl.SSLContext,
    max_retries: int = 5,
) -> Dict:
    encoded_query = urllib.parse.urlencode({"data": query}).encode("utf-8")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json",
        "User-Agent": DEFAULT_USER_AGENT,
    }

    for attempt in range(1, max_retries + 1):
        try:
            request = urllib.request.Request(
                overpass_url,
                data=encoded_query,
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(  # type: ignore[arg-type]
                request,
                timeout=timeout,
                context=ssl_context,
            ) as response:
                payload = response.read().decode("utf-8")
            return json.loads(payload)
        except urllib.error.HTTPError as exc:
            body = ""
            if exc.fp is not None:
                try:
                    body = exc.read().decode("utf-8", "ignore").strip()
                except Exception:  # pragma: no cover - logging auxiliar
                    body = ""
            should_retry = attempt < max_retries
            wait_seconds = sleep_seconds * attempt
            if body:
                logger.warning(
                    "Error %s consultando Overpass (intento %s/%s): %s",
                    exc.code,
                    attempt,
                    max_retries,
                    body,
                )
            else:
                logger.warning(
                    "Error %s consultando Overpass (intento %s/%s): %s",
                    exc.code,
                    attempt,
                    max_retries,
                    exc,
                )
            if not should_retry:
                raise
            time.sleep(wait_seconds)
        except (urllib.error.URLError, TimeoutError) as exc:
            should_retry = attempt < max_retries
            wait_seconds = sleep_seconds * attempt
            logger.warning(
                "Error consultando Overpass (intento %s/%s): %s", attempt, max_retries, exc
            )
            if not should_retry:
                raise
            time.sleep(wait_seconds)
    raise RuntimeError("Se alcanzó el número máximo de reintentos sin éxito.")


def iso_date_for_year(year: int, reference_date: Optional[dt.date] = None) -> str:
    if reference_date is None:
        reference_date = dt.date.today()
    if year < reference_date.year:
        return f"{year}-12-31T23:59:59Z"
    last_day = reference_date - dt.timedelta(days=1)
    return last_day.strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_coordinates(element: Dict) -> tuple[Optional[float], Optional[float]]:
    lat = element.get("lat")
    lon = element.get("lon")
    if lat is not None and lon is not None:
        return float(lat), float(lon)
    center = element.get("center")
    if isinstance(center, dict):
        clat = center.get("lat")
        clon = center.get("lon")
        if clat is not None and clon is not None:
            return float(clat), float(clon)
    return None, None


def element_to_record(
    element: Dict,
    *,
    observation_year: int,
    observation_date: str,
    category_keys: Sequence[str],
    extra_tags: Sequence[str],
    allowed_amenities: Sequence[str],
    allow_all_amenities: bool,
) -> Optional[Dict[str, Optional[str]]]:
    tags: Dict[str, str] = element.get("tags", {})
    category_key: Optional[str] = None
    category_value: Optional[str] = None
    for key in category_keys:
        raw_value = tags.get(key)
        if raw_value:
            category_key = key
            category_value = raw_value
            break
    if not category_key or not category_value:
        return None

    if (
        category_key == "amenity"
        and not allow_all_amenities
        and allowed_amenities
        and category_value.strip().lower() not in allowed_amenities
    ):
        return None

    lat, lon = _extract_coordinates(element)

    record: Dict[str, Optional[str]] = {
        "observation_year": str(observation_year),
        "observation_date": observation_date,
        "osm_type": element.get("type"),
        "osm_id": str(element.get("id")),
        "name": tags.get("name"),
        "category_key": category_key,
        "category_value": category_value,
        "latitude": f"{lat:.7f}" if lat is not None else None,
        "longitude": f"{lon:.7f}" if lon is not None else None,
    }

    for tag in extra_tags:
        record[tag] = tags.get(tag)

    return record


def collect_records(
    *,
    years: Iterable[int],
    query_params: Dict[str, str],
    overpass_url: str,
    sleep_seconds: float,
    category_keys: Sequence[str],
    extra_tags: Sequence[str],
    timeout: int,
    ssl_context: ssl.SSLContext,
    allowed_amenities: Sequence[str],
    allow_all_amenities: bool,
) -> List[Dict[str, Optional[str]]]:
    area_selector = query_params["area_selector"]
    records: List[Dict[str, Optional[str]]] = []

    for year in years:
        iso_date = iso_date_for_year(year)
        query = build_overpass_query(
            area_selector=area_selector,
            category_keys=category_keys,
            iso_date=iso_date,
            timeout=timeout,
        )
        logger.info("Consultando OSM para el año %s...", year)
        payload = execute_overpass_query(
            query=query,
            overpass_url=overpass_url,
            timeout=timeout,
            sleep_seconds=sleep_seconds,
            ssl_context=ssl_context,
        )
        elements = payload.get("elements", [])
        logger.info("  → %s elementos devueltos", len(elements))
        for element in elements:
            record = element_to_record(
                element,
                observation_year=year,
                observation_date=iso_date,
                category_keys=category_keys,
                extra_tags=extra_tags,
                allowed_amenities=allowed_amenities,
                allow_all_amenities=allow_all_amenities,
            )
            if record:
                records.append(record)
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return records


def write_csv(records: Sequence[Dict[str, Optional[str]]], output_path: Path) -> None:
    if not records:
        logger.warning("No se encontraron registros; no se generará el CSV.")
        return

    field_order = [
        "observation_year",
        "observation_date",
        "osm_type",
        "osm_id",
        "name",
        "category_key",
        "category_value",
        "latitude",
        "longitude",
    ]
    extra_fields = sorted({key for record in records for key in record.keys()} - set(field_order))
    fieldnames = field_order + extra_fields

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as csvfile:
        from csv import DictWriter

        writer = DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

    logger.info("CSV escrito en %s (%s filas)", output_path, len(records))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-year",
        type=int,
        default=2015,
        help="Primer año a consultar (inclusive).",
    )
    parser.add_argument(
        "--to-year",
        type=int,
        default=dt.date.today().year,
        help="Último año a consultar (inclusive).",
    )
    parser.add_argument(
        "--area-tag",
        action="append",
        default=list(DEFAULT_AREA_TAGS),
        help="Etiqueta clave=valor para definir el área administrativa (repetible).",
    )
    parser.add_argument(
        "--overpass-url",
        default=DEFAULT_OVERPASS_URL,
        help="Endpoint de Overpass API (por defecto overpass-api.de).",
    )
    parser.add_argument(
        "--category-key",
        action="append",
        default=list(DEFAULT_CATEGORY_KEYS),
        help="Claves de etiqueta OSM a considerar como tipología (por defecto shop y amenity).",
    )
    parser.add_argument(
        "--allowed-amenity",
        dest="allowed_amenity",
        action="append",
        default=None,
        help="Valores amenity a conservar como comercios (repetible).",
    )
    parser.add_argument(
        "--allow-all-amenities",
        action="store_true",
        help="Incluye cualquier valor de amenity sin filtrar.",
    )
    parser.add_argument(
        "--extra-tag",
        action="append",
        default=list(DEFAULT_EXTRA_TAGS),
        help="Etiquetas adicionales a volcar en el CSV (repetible).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/osm_usera_comercios.csv"),
        help="Ruta del CSV de salida.",
    )
    parser.add_argument(
        "--cafile",
        type=Path,
        help="Ruta a un fichero PEM con certificados raíz adicionales.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Desactiva la validación TLS (no recomendado).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="Segundos de espera entre años para respetar la cuota de Overpass.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Tiempo máximo por consulta (segundos).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Nivel de logging.",
    )
    return parser.parse_args(argv)


def validate_years(from_year: int, to_year: int) -> List[int]:
    if from_year > to_year:
        raise ValueError("from_year no puede ser mayor que to_year")
    current_year = dt.date.today().year
    if to_year > current_year:
        logger.warning(
            "El año final %s es mayor que el actual (%s); se usará la fecha actual.",
            to_year,
            current_year,
        )
    return list(range(from_year, to_year + 1))


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(message)s")

    years = validate_years(args.from_year, args.to_year)
    area_tags = parse_area_tags(args.area_tag)
    area_selector = build_area_selector(area_tags)
    cafile = args.cafile

    if cafile and not cafile.exists():
        raise FileNotFoundError(f"No se encuentra el fichero de certificados: {cafile}")

    ssl_context = build_ssl_context(cafile=cafile, insecure=args.insecure)
    allowed_amenities = args.allowed_amenity
    if allowed_amenities is None:
        allowed_amenities = list(DEFAULT_ALLOWED_AMENITIES)
    allowed_amenities_set = {
        value.strip().lower()
        for value in allowed_amenities
        if value and value.strip()
    }

    logger.info("Años a consultar: %s", ", ".join(str(y) for y in years))
    logger.info(
        "Área OSM: %s",
        " ".join(f"{k}={v}" for k, v in area_tags),
    )
    if args.allow_all_amenities:
        logger.info("Amenity sin filtro: se incluirán todos los valores registrados.")
    else:
        logger.info(
            "Amenity permitidos: %s",
            ", ".join(sorted(allowed_amenities_set)) if allowed_amenities_set else "(ninguno)",
        )

    records = collect_records(
        years=years,
        query_params={"area_selector": area_selector},
        overpass_url=args.overpass_url,
        sleep_seconds=args.sleep_seconds,
        category_keys=args.category_key,
        extra_tags=args.extra_tag,
        timeout=args.timeout,
        ssl_context=ssl_context,
        allowed_amenities=allowed_amenities_set,
        allow_all_amenities=args.allow_all_amenities,
    )

    write_csv(records, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
