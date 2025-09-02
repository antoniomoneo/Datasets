#!/usr/bin/env python3
import argparse
import csv
import io
import json
import os
import subprocess
from typing import Optional, Dict, Any


def parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    v = value.strip()
    if v == "" or v.lower() == "null":
        return None
    try:
        return float(v)
    except Exception:
        return None


def parse_int(value: str) -> Optional[int]:
    f = parse_float(value)
    return int(f) if f is not None else None


def count_retired(row: Dict[str, str]) -> int:
    # Try several possible retire/retired column names
    for key in ("retire_at", "retired_at", "retired_on"):
        if key in row:
            v = (row.get(key) or "").strip()
            if v not in ("", "null", "None"):
                return 1
    return 0


def load_csv_counts(path: str) -> Dict[str, Any]:
    total_rows = 0
    sum_conf = 0.0
    cnt_conf = 0
    sum_up = 0
    cnt_up = 0
    sum_votes_total = 0
    cnt_votes_total = 0
    retired = 0

    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # Normalize headers to handle case differences
        reader.fieldnames = [h.strip() if h else h for h in reader.fieldnames or []]
        for row in reader:
            total_rows += 1

            # confidence_score
            cs = parse_float(row.get('confidence_score')) if 'confidence_score' in row else None
            if cs is not None:
                sum_conf += cs
                cnt_conf += 1

            # cached_votes_up
            up = parse_int(row.get('cached_votes_up')) if 'cached_votes_up' in row else None
            if up is not None:
                sum_up += up
                cnt_up += 1

            # cached_votes_total if present
            vt = parse_int(row.get('cached_votes_total')) if 'cached_votes_total' in row else None
            if vt is not None:
                sum_votes_total += vt
                cnt_votes_total += 1

            retired += count_retired(row)

    return {
        'proposals_count': total_rows,
        'confidence_score_mean': (sum_conf / cnt_conf) if cnt_conf else None,
        'cached_votes_up_sum': sum_up,
        'cached_votes_up_mean': (sum_up / cnt_up) if cnt_up else None,
        'cached_votes_total_sum': sum_votes_total if cnt_votes_total else None,
        'cached_votes_total_mean': (sum_votes_total / cnt_votes_total) if cnt_votes_total else None,
        'retired_count': retired,
    }


def load_previous_from_git(path_in_repo: str) -> Optional[Dict[str, Any]]:
    try:
        content = subprocess.check_output([
            'git', 'show', f'HEAD^:{path_in_repo}'
        ], stderr=subprocess.DEVNULL)
    except Exception:
        return None

    # Write to in-memory file and parse
    total_rows = 0
    with io.StringIO(content.decode('utf-8', errors='replace')) as f:
        reader = csv.DictReader(f)
        for _ in reader:
            total_rows += 1
    return {'proposals_count': total_rows}


def build_markdown(latest: Dict[str, Any], delta: Optional[int]) -> str:
    lines = []
    lines.append('# Decide Madrid – Proposals summary')
    lines.append('')
    lines.append('- Proposals: {}{}'.format(
        latest['proposals_count'],
        f" (Δ {delta:+d})" if isinstance(delta, int) else ''
    ))

    # Prefer total votes if available
    if latest.get('cached_votes_total_sum') is not None:
        lines.append('- Votes (total): {}'.format(int(latest['cached_votes_total_sum'])))
        if latest.get('cached_votes_total_mean') is not None:
            lines.append('- Mean votes (total): {:.3f}'.format(latest['cached_votes_total_mean']))

    lines.append('- Votes (cached_votes_up sum): {}'.format(int(latest['cached_votes_up_sum'])))
    if latest.get('cached_votes_up_mean') is not None:
        lines.append('- Mean cached_votes_up: {:.3f}'.format(latest['cached_votes_up_mean']))

    if latest.get('confidence_score_mean') is not None:
        lines.append('- Mean confidence_score: {:.6f}'.format(latest['confidence_score_mean']))

    lines.append('- Retired count: {}'.format(int(latest['retired_count'])))
    lines.append('')
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description='Summarize Decide Madrid proposals CSV')
    ap.add_argument('--in', dest='inp', default='decide-madrid/proposals_latest.csv', help='Path to latest CSV')
    ap.add_argument('--prev', dest='prev', default=None, help='Optional path to previous-day CSV')
    ap.add_argument('--compare-git', action='store_true', help='Compare proposals count against previous commit version')
    ap.add_argument('--out-json', dest='out_json', default=None, help='Optional path to write JSON summary')
    ap.add_argument('--out-md', dest='out_md', default=None, help='Optional path to write Markdown summary')
    args = ap.parse_args()

    latest = load_csv_counts(args.inp)

    delta = None
    prev_counts = None
    if args.prev and os.path.exists(args.prev):
        prev_counts = load_csv_counts(args.prev)
    elif args.compare_git:
        # Path relative to repo root
        rel_path = args.inp
        prev_counts = load_previous_from_git(rel_path)

    if prev_counts and 'proposals_count' in prev_counts:
        delta = latest['proposals_count'] - prev_counts['proposals_count']

    result = {
        'metrics': latest,
        'delta_vs_previous_day': delta,
        'source_file': args.inp,
    }

    if args.out_json:
        os.makedirs(os.path.dirname(args.out_json), exist_ok=True)
        with open(args.out_json, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    md = build_markdown(latest, delta)
    if args.out_md:
        os.makedirs(os.path.dirname(args.out_md), exist_ok=True)
        with open(args.out_md, 'w', encoding='utf-8') as f:
            f.write(md)

    # Also print to stdout for CI logs
    print(md)


if __name__ == '__main__':
    main()
