#!/usr/bin/env python3
import csv
import io
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

from urllib.parse import quote

import requests
from icalendar import Calendar, Event


def build_ics_url(calendar_id: str) -> str:
    return f"https://calendar.google.com/calendar/ical/{quote(calendar_id)}/public/full.ics"


def get_ics_source() -> str:
    # Prefer explicit ICS_URL; otherwise build from CALENDAR_ID; otherwise default to the tangible calendar id.
    default_calendar_id = (
        "c_41592e57472725b685e2d4ffb20f05c12f117f9ea2a46431ea621ed686f870ff@group.calendar.google.com"
    )
    ics_url = os.getenv("ICS_URL")
    calendar_id = os.getenv("CALENDAR_ID")
    if ics_url:
        return ics_url
    if calendar_id:
        return build_ics_url(calendar_id)
    return build_ics_url(default_calendar_id)


def fetch_ics(url: str) -> bytes:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content


def parse_events(ics_bytes: bytes) -> List[Dict[str, Any]]:
    cal = Calendar.from_ical(ics_bytes)
    events: List[Dict[str, Any]] = []
    for component in cal.walk():
        if component.name != "VEVENT":
            continue

        def _get_text(field: str) -> str:
            val = component.get(field)
            return str(val) if val is not None else ""

        uid = _get_text("UID")
        summary = _get_text("SUMMARY")
        description = _get_text("DESCRIPTION")
        location = _get_text("LOCATION")

        dtstart = component.get("DTSTART")
        dtend = component.get("DTEND")
        last_modified = component.get("LAST-MODIFIED") or component.get("DTSTAMP")
        recurrence_id = component.get("RECURRENCE-ID")

        # Normalize datetime/date to ISO strings
        def _to_iso(val) -> str:
            if val is None:
                return ""
            obj = val.dt
            try:
                return obj.isoformat()
            except Exception:
                return str(obj)

        start_iso = _to_iso(dtstart)
        end_iso = _to_iso(dtend)
        last_mod_iso = _to_iso(last_modified)
        rec_id_iso = _to_iso(recurrence_id)

        # All-day if DTSTART is a date (not datetime)
        try:
            from datetime import date, datetime
            all_day = isinstance(dtstart.dt, date) and not isinstance(dtstart.dt, datetime)
        except Exception:
            all_day = False

        events.append(
            {
                "uid": uid,
                "title": summary,
                "description": description,
                "location": location,
                "start": start_iso,
                "end": end_iso,
                "all_day": all_day,
                "last_modified": last_mod_iso,
                "recurrence_id": rec_id_iso,
            }
        )

    # Sort by start then title
    def sort_key(e: Dict[str, Any]):
        return (e.get("start") or "", e.get("title") or "")

    events.sort(key=sort_key)
    return events


def render_csv(events: List[Dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "uid",
            "title",
            "description",
            "location",
            "start",
            "end",
            "all_day",
            "last_modified",
            "recurrence_id",
        ],
    )
    writer.writeheader()
    for ev in events:
        writer.writerow(ev)
    return output.getvalue()


def write_if_changed(target: Path, content: str) -> bool:
    if target.exists():
        current = target.read_text(encoding="utf-8")
        if current == content:
            return False
    target.write_text(content, encoding="utf-8")
    return True


def main() -> int:
    ics_url = get_ics_source()
    try:
        ics_bytes = fetch_ics(ics_url)
    except Exception as e:
        print(f"Error fetching ICS from {ics_url}: {e}", file=sys.stderr)
        return 2

    try:
        events = parse_events(ics_bytes)
    except Exception as e:
        print(f"Error parsing ICS: {e}", file=sys.stderr)
        return 3

    csv_text = render_csv(events)
    out_path = Path(__file__).parent / "calendar.csv"
    changed = write_if_changed(out_path, csv_text)
    print(f"Wrote {len(events)} events to {out_path} ({'changed' if changed else 'no changes'})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
