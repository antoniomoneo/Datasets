#!/usr/bin/env -S node
/*
 TypeScript summary script for Decide Madrid proposals.
 - Computes: proposals_count, confidence_score_mean, cached_votes_up sum/mean,
   cached_votes_total sum/mean (if present), retired_count.
 - Delta vs previous commit when --compare-git is passed.

 Usage:
   node scripts/decide_madrid_summary.js --in decide-madrid/proposals_latest.csv \
     --compare-git --out-json decide-madrid/proposals_summary.json \
     --out-md decide-madrid/proposals_summary.md

 Note: This .ts is committed for maintainability. At runtime in GitHub Actions,
 either compile to JS or run the existing Python script. This file mirrors that logic.
*/

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { spawnSync } from 'child_process';

type Row = Record<string, string>;
type Summary = {
  proposals_count: number;
  confidence_score_mean: number | null;
  cached_votes_up_sum: number;
  cached_votes_up_mean: number | null;
  cached_votes_total_sum: number | null;
  cached_votes_total_mean: number | null;
  retired_count: number;
};

type Result = {
  metrics: Summary;
  delta_vs_previous_day: number | null;
  source_file: string;
};

function isValidUTF8(buf: Buffer): boolean {
  // Minimal UTF-8 validation
  let i = 0;
  while (i < buf.length) {
    const c = buf[i];
    if (c <= 0x7F) { i++; continue; }
    let n = 0;
    if ((c & 0xE0) === 0xC0) n = 1;
    else if ((c & 0xF0) === 0xE0) n = 2;
    else if ((c & 0xF8) === 0xF0) n = 3;
    else return false;
    if (i + n >= buf.length) return false;
    for (let j = 1; j <= n; j++) {
      if ((buf[i + j] & 0xC0) !== 0x80) return false;
    }
    i += n + 1;
  }
  return true;
}

// Minimal CP1252 mapping for bytes 0x80–0x9F that differ from ISO-8859-1
const CP1252_MAP: Record<number, number> = {
  0x80: 0x20AC, // €
  0x82: 0x201A,
  0x83: 0x0192,
  0x84: 0x201E,
  0x85: 0x2026,
  0x86: 0x2020,
  0x87: 0x2021,
  0x88: 0x02C6,
  0x89: 0x2030,
  0x8A: 0x0160,
  0x8B: 0x2039,
  0x8C: 0x0152,
  0x8E: 0x017D,
  0x91: 0x2018,
  0x92: 0x2019,
  0x93: 0x201C,
  0x94: 0x201D,
  0x95: 0x2022,
  0x96: 0x2013,
  0x97: 0x2014,
  0x98: 0x02DC,
  0x99: 0x2122,
  0x9A: 0x0161,
  0x9B: 0x203A,
  0x9C: 0x0153,
  0x9E: 0x017E,
  0x9F: 0x0178,
};

function decodeCP1252(buf: Buffer): string {
  const codepoints: number[] = new Array(buf.length);
  for (let i = 0; i < buf.length; i++) {
    const b = buf[i];
    if (b >= 0x80 && b <= 0x9F && CP1252_MAP[b] !== undefined) {
      codepoints[i] = CP1252_MAP[b]!;
    } else {
      // Latin-1 direct mapping for the rest
      codepoints[i] = b;
    }
  }
  return String.fromCodePoint(...codepoints).normalize('NFC');
}

function decodeWithFallback(path: string): { text: string; encoding: string } {
  const buf = readFileSync(path);
  if (isValidUTF8(buf)) {
    return { text: buf.toString('utf8').normalize('NFC'), encoding: 'utf8' };
  }
  // Prefer CP1252 for Spanish content (preserves diacritics and punctuation like “ ” €)
  return { text: decodeCP1252(buf), encoding: 'cp1252' };
}

function parseCSV(text: string): Row[] {
  // Normalize line endings
  const s = text.replace(/\r\n?/g, '\n');
  const rows: string[][] = [];
  let cur: string[] = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (inQuotes) {
      if (ch === '"') {
        if (s[i + 1] === '"') { field += '"'; i++; }
        else { inQuotes = false; }
      } else {
        field += ch;
      }
    } else {
      if (ch === '"') { inQuotes = true; }
      else if (ch === ',') { cur.push(field); field = ''; }
      else if (ch === '\n') { cur.push(field); rows.push(cur); cur = []; field = ''; }
      else { field += ch; }
    }
  }
  // last field/row
  if (field.length > 0 || cur.length > 0) { cur.push(field); rows.push(cur); }
  if (rows.length === 0) return [];
  const headers = rows[0].map(h => (h ?? '').trim());
  const out: Row[] = [];
  for (let r = 1; r < rows.length; r++) {
    const arr = rows[r];
    if (arr.length === 1 && arr[0] === '') continue; // skip empty lines
    const obj: Row = {};
    for (let c = 0; c < headers.length; c++) {
      obj[headers[c]] = (arr[c] ?? '').trim();
    }
    out.push(obj);
  }
  return out;
}

function toFloat(v?: string): number | null {
  if (!v) return null;
  const t = v.trim();
  if (!t || t.toLowerCase() === 'null') return null;
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

function toInt(v?: string): number | null {
  const f = toFloat(v);
  return f !== null ? Math.trunc(f) : null;
}

function retired(row: Row): number {
  for (const key of ['retire_at', 'retired_at', 'retired_on']) {
    if (key in row) {
      const v = (row[key] || '').trim();
      if (v && v.toLowerCase() !== 'null' && v.toLowerCase() !== 'none') return 1;
    }
  }
  return 0;
}

function loadCsvCounts(path: string): Summary {
  const { text } = decodeWithFallback(path);
  const data = parseCSV(text);
  let total = 0;
  let sumConf = 0, cntConf = 0;
  let sumUp = 0, cntUp = 0;
  let sumTot = 0, cntTot = 0;
  let retiredCnt = 0;

  for (const row of data) {
    total++;
    const cs = toFloat(row['confidence_score']);
    if (cs !== null) { sumConf += cs; cntConf++; }
    const up = toInt(row['cached_votes_up']);
    if (up !== null) { sumUp += up; cntUp++; }
    const vt = toInt(row['cached_votes_total']);
    if (vt !== null) { sumTot += vt; cntTot++; }
    retiredCnt += retired(row);
  }

  return {
    proposals_count: total,
    confidence_score_mean: cntConf ? (sumConf / cntConf) : null,
    cached_votes_up_sum: sumUp,
    cached_votes_up_mean: cntUp ? (sumUp / cntUp) : null,
    cached_votes_total_sum: cntTot ? sumTot : null,
    cached_votes_total_mean: cntTot ? (sumTot / cntTot) : null,
    retired_count: retiredCnt,
  };
}

function loadPreviousFromGit(pathInRepo: string): { proposals_count: number } | null {
  try {
    const r = spawnSync('git', ['show', `HEAD^:${pathInRepo}`], { encoding: 'buffer' });
    if (r.status !== 0 || !r.stdout) return null;
    const buf = Buffer.from(r.stdout);
    const text = isValidUTF8(buf) ? buf.toString('utf8') : buf.toString('latin1');
    const rows = parseCSV(text);
    return { proposals_count: rows.length };
  } catch {
    return null;
  }
}

function buildMarkdown(latest: Summary, delta: number | null): string {
  const lines: string[] = [];
  lines.push('# Decide Madrid – Proposals summary');
  lines.push('');
  lines.push(`- Proposals: ${latest.proposals_count}${delta !== null ? ` (Δ ${delta >= 0 ? '+' : ''}${delta})` : ''}`);
  if (latest.cached_votes_total_sum !== null) {
    lines.push(`- Votes (total): ${latest.cached_votes_total_sum}`);
    if (latest.cached_votes_total_mean !== null) {
      lines.push(`- Mean votes (total): ${latest.cached_votes_total_mean.toFixed(3)}`);
    }
  }
  lines.push(`- Votes (cached_votes_up sum): ${latest.cached_votes_up_sum}`);
  if (latest.cached_votes_up_mean !== null) {
    lines.push(`- Mean cached_votes_up: ${latest.cached_votes_up_mean.toFixed(3)}`);
  }
  if (latest.confidence_score_mean !== null) {
    lines.push(`- Mean confidence_score: ${latest.confidence_score_mean.toFixed(6)}`);
  }
  lines.push(`- Retired count: ${latest.retired_count}`);
  lines.push('');
  return lines.join('\n');
}

function parseArgs(argv: string[]): Record<string, string | boolean> {
  const args: Record<string, string | boolean> = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (!next || next.startsWith('--')) { args[key] = true; }
      else { args[key] = next; i++; }
    }
  }
  return args;
}

function main() {
  const args = parseArgs(process.argv);
  const inp = (args['in'] as string) || 'decide-madrid/proposals_latest.csv';
  const prevPath = (args['prev'] as string) || undefined;
  const compareGit = Boolean(args['compare-git']);
  const outJson = (args['out-json'] as string) || undefined;
  const outMd = (args['out-md'] as string) || undefined;

  const latest = loadCsvCounts(inp);

  let delta: number | null = null;
  let prevCounts: { proposals_count: number } | null = null;
  if (prevPath && existsSync(prevPath)) {
    const m = loadCsvCounts(prevPath);
    prevCounts = { proposals_count: m.proposals_count };
  } else if (compareGit) {
    prevCounts = loadPreviousFromGit(inp);
  }
  if (prevCounts) {
    delta = latest.proposals_count - prevCounts.proposals_count;
  }

  const result: Result = { metrics: latest, delta_vs_previous_day: delta, source_file: inp };
  if (outJson) {
    writeFileSync(outJson, JSON.stringify(result, null, 2), { encoding: 'utf8' });
  }
  const md = buildMarkdown(latest, delta);
  if (outMd) {
    writeFileSync(outMd, md, { encoding: 'utf8' });
  }
  console.log(md);
}

main();
