# Tangible Climate Calendar → CSV

This folder contains a small script that fetches events from a Google Calendar (via its public ICS feed) and writes them to `calendar.csv`.

By default it targets the Tangible Climate calendar ID you shared:

```
c_41592e57472725b685e2d4ffb20f05c12f117f9ea2a46431ea621ed686f870ff@group.calendar.google.com
```

If the calendar is not public, the ICS URL won’t be accessible and the script will fail. In that case, make the calendar public or switch to an authenticated approach.

## Usage

- Local run:
  - `python3 -m venv .venv && source .venv/bin/activate`
  - `pip install -r tangible-climate-calendar/requirements.txt`
  - `python tangible-climate-calendar/fetch_calendar.py`

- Configure target calendar (optional):
  - Use `CALENDAR_ID` to target a specific calendar ID, or `ICS_URL` to provide the full ICS URL.
  - Example:
    - `CALENDAR_ID=c_example@group.calendar.google.com python tangible-climate-calendar/fetch_calendar.py`
    - `ICS_URL=https://calendar.google.com/calendar/ical/…/public/full.ics python tangible-climate-calendar/fetch_calendar.py`

## Output

`calendar.csv` with columns:

- `uid`, `title`, `description`, `location`, `start`, `end`, `all_day`, `last_modified`, `recurrence_id`

Sorted by `start` then `title`.

