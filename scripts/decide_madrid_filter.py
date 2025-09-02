#!/usr/bin/env python3
import argparse
import csv
from datetime import datetime, date
from typing import Optional, List, Tuple

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
    return ap.parse_args()

def extract_date(value: str) -> Optional[date]:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    # Extract dd/mm/yyyy pattern regardless of trailing tokens like ' 08'
    import re
    m = re.search(r'(\d{2}/\d{2}/\d{4})', s)
    if not m:
        return None
    dstr = m.group(1)
    try:
        return datetime.strptime(dstr, '%d/%m/%Y').date()
    except Exception:
        return None

def filter_csv(inp: str, out: str, drop: List[str], from_d: date, to_d: Optional[date]):
    with _open_text_with_fallback(inp) as f_in:
        reader = csv.DictReader(f_in)
        headers = [h for h in (reader.fieldnames or []) if h and h not in drop]
        # Ensure created_at exists for filtering but can be dropped if requested
        rows_out = []
        for row in reader:
            created_raw = row.get('created_at', '')
            d = extract_date(created_raw)
            if d is None:
                continue
            if d < from_d:
                continue
            if to_d is not None and d > to_d:
                continue
            # Build output row sans dropped columns
            rows_out.append({k: row.get(k, '') for k in headers})

    with open(out, 'w', encoding='utf-8', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows_out)

def main():
    args = parse_args()
    from_d = datetime.strptime(args.from_date, '%Y-%m-%d').date()
    to_d = datetime.strptime(args.to_date, '%Y-%m-%d').date() if args.to_date else None
    filter_csv(args.inp, args.out, args.drop, from_d, to_d)

if __name__ == '__main__':
    main()

