#!/usr/bin/env python3
"""Fetch Human Resources job family data from the O*NET beta service."""
from __future__ import annotations

import argparse
import csv
import getpass
import json
import os
import sys
import time
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import requests

BASE_URL = "https://services-beta.onetcenter.org/ws"
JOB_FAMILY_KEY = "human resources"
VARIANT_BUCKETS: Dict[str, tuple[str, ...]] = {
    "compensation": ("compensation", "total rewards", "benefits"),
    "professional_development": ("learning", "training", "development"),
    "business_partner": ("business partner", "hrbp", "partner"),
    "organization": ("organizational", "org development", "org effectiveness"),
    "talent_acquisition": ("talent acquisition", "recruit", "staffing", "sourcing"),
    "culture_engagement": ("culture", "engagement", "employee experience"),
    "advisors": ("advisor", "consultant", "specialist"),
}

DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "data"
DEFAULT_JSON_PATH = DEFAULT_DATA_DIR / "human_resources_buckets.json"
DEFAULT_CSV_PATH = DEFAULT_DATA_DIR / "human_resources_buckets.csv"


def credential_from_env_or_prompt(name: str, prompt: str, *, secret: bool = False) -> str:
    value = os.getenv(name)
    if value:
        return value
    prompt_fn = getpass.getpass if secret else input
    value = prompt_fn(prompt)
    if not value:
        print(f"Credential for {name} is required", file=sys.stderr)
        sys.exit(1)
    return value


@lru_cache(maxsize=1)
def get_onet_auth() -> Tuple[str, str]:
    user = credential_from_env_or_prompt("ONET_USER", "O*NET username: ")
    key = credential_from_env_or_prompt("ONET_KEY", "O*NET key: ", secret=True)
    return user, key


def fetch(endpoint: str, *, params: Dict[str, str | int] | None = None, base_url: str = BASE_URL) -> dict:
    user, key = get_onet_auth()
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    response = requests.get(url, params=params, auth=(user, key), timeout=30)
    response.raise_for_status()
    return response.json()


def load_job_family(job_family_key: str, *, base_url: str) -> List[dict]:
    data = fetch("mnm/occupations", params={"fmt": "json"}, base_url=base_url)
    occupations = data.get("occupation", [])
    filtered = [
        occ for occ in occupations
        if job_family_key in occ.get("job_family", "").lower()
    ]
    return filtered


def fetch_details(occupations: Iterable[dict], *, delay: float, base_url: str) -> List[dict]:
    detailed: List[dict] = []
    for occ in occupations:
        code = occ.get("code")
        if not code:
            continue
        detail = fetch(f"mnm/occupations/{code}", params={"fmt": "json"}, base_url=base_url)
        detail["summary"] = occ
        detailed.append(detail)
        time.sleep(delay)
    return detailed


def bucket_by_variant(items: Iterable[dict]) -> Dict[str, List[dict]]:
    buckets: Dict[str, List[dict]] = defaultdict(list)
    for detail in items:
        summary = detail.get("summary", {})
        title = summary.get("title", "").lower()
        description = detail.get("occupation", {}).get("description", "").lower()
        target = "other"
        for bucket, keywords in VARIANT_BUCKETS.items():
            if any(keyword in title or keyword in description for keyword in keywords):
                target = bucket
                break
        buckets[target].append(detail)
    return buckets


def export_json(data: Dict[str, List[dict]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)


def export_csv(data: Dict[str, List[dict]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: List[dict] = []
    for bucket, items in data.items():
        for detail in items:
            summary = detail.get("summary", {})
            occ = detail.get("occupation", {})
            rows.append({
                "bucket": bucket,
                "onet_code": summary.get("code"),
                "title": summary.get("title"),
                "job_family": summary.get("job_family"),
                "description": occ.get("description"),
            })
    fieldnames = ["bucket", "onet_code", "title", "job_family", "description"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Human Resources job family occupations from O*NET beta")
    parser.add_argument("--base-url", default=BASE_URL, help="O*NET service base URL")
    parser.add_argument("--job-family", default=JOB_FAMILY_KEY, help="Job family match (lowercase substring)")
    parser.add_argument("--delay", type=float, default=0.2, help="Pause between detail calls")
    parser.add_argument("--json", dest="json_path", default=str(DEFAULT_JSON_PATH), help="Path for grouped JSON output")
    parser.add_argument("--csv", dest="csv_path", default=str(DEFAULT_CSV_PATH), help="Path for summary CSV output")
    args = parser.parse_args()

    occupations = load_job_family(args.job_family.lower(), base_url=args.base_url)
    if not occupations:
        print("No occupations matched the requested job family", file=sys.stderr)
        sys.exit(2)

    details = fetch_details(occupations, delay=args.delay, base_url=args.base_url)
    buckets = bucket_by_variant(details)

    export_json(buckets, Path(args.json_path))
    export_csv(buckets, Path(args.csv_path))

    print("Saved outputs:")
    print(f"- JSON: {args.json_path}")
    print(f"- CSV:  {args.csv_path}")
    print("Counts by variant:")
    for bucket, items in buckets.items():
        print(f"  {bucket}: {len(items)}")


if __name__ == "__main__":
    main()
