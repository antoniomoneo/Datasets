#!/usr/bin/env python3
"""Utilities for querying the O*NET beta service for occupation data."""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, List

import requests

BASE_URL = "https://services-beta.onetcenter.org/ws"
DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "data"
DEFAULT_JSON_PATH = DEFAULT_DATA_DIR / "occupations.json"
DEFAULT_CSV_PATH = DEFAULT_DATA_DIR / "occupations.csv"
DEFAULT_DELAY = 0.2

HR_KEYWORDS = (
    "human resources",
    "hr",
    "talent",
    "recruit",
    "staffing",
    "people operations",
    "compensation",
    "benefits",
    "learning",
    "development",
)


def env_or_fail(name: str) -> str:
    """Return the value of an environment variable or exit if missing."""

    value = os.getenv(name)
    if not value:
        print(f"Environment variable {name} is required", file=sys.stderr)
        sys.exit(1)
    return value


def call_onet_api(endpoint: str, *, params: dict | None = None, base_url: str = BASE_URL) -> dict:
    """Perform an authenticated GET request to the O*NET service."""

    user = env_or_fail("ONET_USER")
    key = env_or_fail("ONET_KEY")
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    response = requests.get(url, params=params, auth=(user, key), timeout=30)
    response.raise_for_status()
    return response.json()


def search_occupations(keywords: Iterable[str], *, max_results: int = 25, base_url: str = BASE_URL) -> List[dict]:
    """Search O*NET occupations using the provided keywords."""

    keyword_query = " ".join(str(keyword) for keyword in keywords if keyword).strip()
    params = {"fmt": "json"}
    if keyword_query:
        params["keyword"] = keyword_query
    if max_results > 0:
        params["start"] = 1
        params["end"] = max_results
    data = call_onet_api("mnm/search", params=params, base_url=base_url)
    return data.get("occupation", [])


def get_occupation_profile(code: str, *, base_url: str = BASE_URL) -> dict:
    """Retrieve a detailed occupation profile from O*NET."""

    if not code:
        raise ValueError("An occupation code is required to fetch a profile")
    return call_onet_api(f"mnm/occupations/{code}", params={"fmt": "json"}, base_url=base_url)


def is_hr_related(occupation: dict) -> bool:
    """Determine whether an occupation appears related to Human Resources."""

    summary = occupation or {}
    title = str(summary.get("title", "")).lower()
    job_family = str(summary.get("job_family", "")).lower()
    description = str(summary.get("description", "")).lower()
    return any(keyword in title or keyword in job_family or keyword in description for keyword in HR_KEYWORDS)


def export_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)


def export_csv(rows: Iterable[dict], path: Path, *, fieldnames: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)


def build_csv_rows(occupations: Iterable[dict]) -> List[dict]:
    rows: List[dict] = []
    for item in occupations:
        summary = item.get("summary", {})
        details = item.get("details", {})
        rows.append(
            {
                "onet_code": summary.get("code"),
                "title": summary.get("title"),
                "job_family": summary.get("job_family"),
                "is_hr_related": summary.get("is_hr_related", False),
                "description": details.get("occupation", {}).get("description"),
            }
        )
    return rows


def fetch_and_collect(keywords: Iterable[str], *, max_results: int, delay: float, base_url: str) -> List[dict]:
    results: List[dict] = []
    search_results = search_occupations(keywords, max_results=max_results, base_url=base_url)
    for occ in search_results:
        code = occ.get("code")
        if not code:
            continue
        details = get_occupation_profile(code, base_url=base_url)
        time.sleep(delay)
        summary = {
            "code": code,
            "title": occ.get("title"),
            "job_family": occ.get("job_family"),
            "description": occ.get("description"),
            "is_hr_related": is_hr_related(occ),
        }
        results.append({"summary": summary, "details": details})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Search the O*NET beta API and store occupation results.")
    parser.add_argument("keywords", nargs="*", help="Keywords to search for (space separated)")
    parser.add_argument("--max-results", type=int, default=25, help="Maximum number of search results to retrieve")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Pause between detail requests")
    parser.add_argument("--base-url", default=BASE_URL, help="Base URL for the O*NET service")
    parser.add_argument("--json-output", default=str(DEFAULT_JSON_PATH), help="Path to JSON output file")
    parser.add_argument("--csv-output", default=str(DEFAULT_CSV_PATH), help="Path to CSV output file")
    args = parser.parse_args()

    if not args.keywords:
        parser.error("At least one keyword is required")

    collected = fetch_and_collect(
        args.keywords,
        max_results=max(0, args.max_results),
        delay=max(0.0, args.delay),
        base_url=args.base_url,
    )

    export_json({"keywords": args.keywords, "occupations": collected}, Path(args.json_output))

    csv_rows = build_csv_rows(collected)
    export_csv(
        csv_rows,
        Path(args.csv_output),
        fieldnames=("onet_code", "title", "job_family", "is_hr_related", "description"),
    )

    print("Saved outputs:")
    print(f"- JSON: {args.json_output}")
    print(f"- CSV:  {args.csv_output}")
    print(f"Total occupations downloaded: {len(collected)}")


if __name__ == "__main__":
    main()
