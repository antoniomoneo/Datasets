#!/usr/bin/env python3
import argparse
import csv
import re
from datetime import datetime, date
from typing import Optional, List

def _open_text_with_fallback(path: str):
    encodings = ("utf-8", "utf-8-sig", "iso-8859-1", "cp1252")
    last_err = None
    for enc in encodings:
        try:
            f = open(path, mode='r', encoding=enc, newline='')
            _ = f.read(4096)
            f.seek(0)
            return f
        except UnicodeDecodeError as e:
            last_err = e
            continue
    # Fallback with replacement
    return open(path, mode='r', encoding='utf-8', errors='replace', newline='')

def parse_args():
    ap = argparse.ArgumentParser(description='Filter Decide Madrid CSV by created_at date and drop columns')
    ap.add_argument('--in', dest='inp', required=True, help='Input CSV path')
    ap.add_argument('--out', dest='out', required=True, help='Output CSV path')
    ap.add_argument('--drop-column', dest='drop', action='append', default=[], help='Columns to drop')
    ap.add_argument('--from-date', dest='from_date', required=True, help='Inclusive lower bound (YYYY-MM-DD)')
    ap.add_argument('--to-date', dest='to_date', default=None, help='Inclusive upper bound (YYYY-MM-DD)')
    ap.add_argument('--normalize', dest='normalize', action='store_true', help='Normalize output to app schema (id;title;description;cached_votes_up;created_at;retired_at) using ; as delimiter')
    return ap.parse_args()

def extract_date(value: str) -> Optional[date]:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    # Try ISO first (yyyy-mm-dd)
    try:
        return datetime.strptime(s[:10], '%Y-%m-%d').date()
    except Exception:
        pass
    # Extract dd/mm/yyyy pattern regardless of trailing tokens like ' 08'
    m = re.search(r'(\d{2}/\d{2}/\d{4})', s)
    if m:
        try:
            return datetime.strptime(m.group(1), '%d/%m/%Y').date()
        except Exception:
            return None
    return None


def _read_rows_flexible(inp_path: str) -> tuple[list[dict[str, str]], list[str]]:
    """Read CSV handling BOM, optional sep= preamble, and , or ; delimiter.
    Returns (rows, headers_original_case)
    """
    with _open_text_with_fallback(inp_path) as f_in:
        text = f_in.read()
    # Remove BOM if present and skip sep= lines
    lines = [ln for ln in re.split(r'\r?\n', text) if ln]
    cleaned: list[str] = []
    for ln in lines:
        ln_strip = ln.lstrip('\ufeff').strip()
        if not ln_strip:
            continue
        if ln_strip.lower().startswith('sep='):
            continue
        cleaned.append(ln_strip)
    if not cleaned:
        return ([], [])
    sample = '\n'.join(cleaned[:5])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',;')
    except Exception:
        dialect = csv.excel
        dialect.delimiter = ';'
    reader = csv.reader(cleaned, dialect)
    headers = next(reader, [])
    rows: list[dict[str, str]] = []
    for arr in reader:
        if len(arr) == 1 and arr[0] == '':
            continue
        row = {headers[i]: (arr[i] if i < len(arr) else '') for i in range(len(headers))}
        rows.append(row)
    return (rows, headers)

def filter_csv(inp: str, out: str, drop: List[str], from_d: date, to_d: Optional[date], normalize: bool):
    rows, headers = _read_rows_flexible(inp)
    headers_lower = [h.lower() for h in headers]

    def get(row: dict[str, str], candidates: list[str]) -> str:
        for c in candidates:
            # find original header matching this lowercase
            for i, hl in enumerate(headers_lower):
                if hl == c:
                    return row.get(headers[i], '')
        return ''

    out_rows: list[dict[str, str]] = []
    for row in rows:
        # Flexible created_at lookup
        created_raw = get(row, ['created_at', 'created at', 'date', 'fecha']) or row.get('created_at', '')
        d = extract_date(created_raw)
        if d is None:
            continue
        if d < from_d:
            continue
        if to_d is not None and d > to_d:
            continue

        if normalize:
            norm = {
                'id': get(row, ['id', 'identifier']),
                'title': get(row, ['title', 'name', 'subject']),
                'description': get(row, ['description', 'summary', 'body', 'text', 'content']),
                'cached_votes_up': get(row, ['cached_votes_up', 'votes_up', 'cached_votes_score', 'votes']) or '0',
                'created_at': created_raw,
                'retired_at': get(row, ['retired_at', 'archived_at', 'retired at']),
            }
            if not norm['id']:
                continue
            out_rows.append(norm)
        else:
            # Build output row sans dropped columns
            out_rows.append({k: row.get(k, '') for k in headers if k not in drop})

    with open(out, 'w', encoding='utf-8', newline='') as f_out:
        if normalize:
            writer = csv.writer(f_out, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(['id','title','description','cached_votes_up','created_at','retired_at'])
            for r in out_rows:
                writer.writerow([r['id'], r['title'], r['description'], r['cached_votes_up'], r['created_at'], r['retired_at']])
        else:
            writer = csv.DictWriter(f_out, fieldnames=[h for h in headers if h not in drop])
            writer.writeheader()
            writer.writerows(out_rows)

def main():
    args = parse_args()
    from_d = datetime.strptime(args.from_date, '%Y-%m-%d').date()
    to_d = datetime.strptime(args.to_date, '%Y-%m-%d').date() if args.to_date else None
    filter_csv(args.inp, args.out, args.drop, from_d, to_d, args.normalize)

if __name__ == '__main__':
    main()
