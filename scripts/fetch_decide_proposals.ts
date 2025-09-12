#!/usr/bin/env -S node
/* Fetch latest proposals.csv and update decide-madrid/proposals_latest.csv
   - Follows redirects, sets browser-like headers
   - Validates content-type and avoids committing HTML error pages
   - Generates summary via compiled TS summary script
*/

import { writeFileSync, mkdirSync, existsSync, readFileSync } from 'fs';
import { spawnSync } from 'child_process';
import * as https from 'https';
import * as http from 'http';
import { URL } from 'url';
import { TextDecoder } from 'util';

const DEST_DIR = 'decide-madrid';
const DEST_FILE = `${DEST_DIR}/proposals_latest.csv`;
const DEST_TS = `${DEST_DIR}/proposals_latest.ts`;
const DEFAULT_URL = 'https://decide.madrid.es/system/api/proposals.csv';
const URL_STR = process.env.DECIDE_MADRID_URL || DEFAULT_URL;

type Headers = Record<string, string>;

type CookieJar = Record<string, string>;

function cookieHeader(jar: CookieJar): string | undefined {
  const entries = Object.entries(jar);
  if (entries.length === 0) return undefined;
  return entries.map(([k, v]) => `${k}=${v}`).join('; ');
}

function mergeSetCookies(jar: CookieJar, setCookieHdrs?: string | string[]): void {
  if (!setCookieHdrs) return;
  const arr = Array.isArray(setCookieHdrs) ? setCookieHdrs : [setCookieHdrs];
  for (const sc of arr) {
    const first = sc.split(';', 1)[0];
    const eq = first.indexOf('=');
    if (eq > 0) {
      const name = first.slice(0, eq).trim();
      const value = first.slice(eq + 1).trim();
      if (name && value) jar[name] = value;
    }
  }
}

function get(urlStr: string, headers: Headers, jar: CookieJar, maxRedirects = 5): Promise<{ status: number; headers: Headers; body: Buffer }> {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const opts: https.RequestOptions = {
      protocol: u.protocol,
      hostname: u.hostname,
      path: u.pathname + u.search,
      port: u.port || (u.protocol === 'https:' ? 443 : 80),
      method: 'GET',
      headers: {
        ...headers,
        ...(cookieHeader(jar) ? { 'Cookie': cookieHeader(jar)! } : {}),
      },
    };
    const mod = u.protocol === 'https:' ? https : http;
    const req = mod.request(opts, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
      res.on('end', () => {
        const status = res.statusCode || 0;
        const hdrs: Headers = {};
        Object.entries(res.headers).forEach(([k, v]) => {
          if (Array.isArray(v)) hdrs[k.toLowerCase()] = v.join(', ');
          else if (typeof v === 'string') hdrs[k.toLowerCase()] = v;
        });
        mergeSetCookies(jar, res.headers['set-cookie'] as any);
        const buf = Buffer.concat(chunks);
        if (status >= 300 && status < 400 && hdrs['location']) {
          if (maxRedirects <= 0) return reject(new Error('Too many redirects'));
          const next = new URL(hdrs['location'], u).toString();
          get(next, headers, jar, maxRedirects - 1).then(resolve).catch(reject);
          return;
        }
        resolve({ status, headers: hdrs, body: buf });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function headers(): Headers {
  return {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
    'Accept': 'text/csv, text/plain;q=0.9, */*;q=0.1',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Referer': 'https://decide.madrid.es/',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Dest': 'document',
    'Upgrade-Insecure-Requests': '1',
  };
}

function byteEqual(a: Buffer, b: Buffer): boolean {
  if (a.length !== b.length) return false;
  return a.compare(b) === 0;
}

function decodePreservingDiacritics(buf: Buffer): string {
  try {
    return new TextDecoder('utf-8', { fatal: true } as any).decode(buf);
  } catch (_) {
    try {
      return new TextDecoder('windows-1252' as any).decode(buf);
    } catch (_) {
      return new TextDecoder('latin1' as any).decode(buf);
    }
  }
}

function escapeForTemplateLiteral(s: string): string {
  return s.replace(/`/g, '\\`').replace(/\$\{/g, '\\${');
}

async function main() {
  mkdirSync(DEST_DIR, { recursive: true });
  console.log(`Descargando CSV desde: ${URL_STR}`);

  const jar: CookieJar = {};
  // Primer toque a la home para obtener cookies de sesión/CDN
  try {
    await get(new URL(URL_STR).origin + '/', { ...headers(), Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' }, jar);
  } catch {}

  const res = await get(URL_STR, headers(), jar);
  if (res.status < 200 || res.status >= 300) {
    throw new Error(`HTTP ${res.status}`);
  }
  const ctype = (res.headers['content-type'] || '').toLowerCase();
  if (ctype && !/(text\/csv|application\/csv|text\/plain)/.test(ctype)) {
    throw new Error(`Tipo de contenido inesperado: ${ctype}`);
  }
  if (res.body.length === 0) {
    console.log('El archivo descargado está vacío. Abortando sin cambios.');
    return;
  }
  const head = res.body.subarray(0, Math.min(20, res.body.length)).toString('utf8');
  if (/\<html/i.test(head)) {
    throw new Error('Servidor devolvió HTML (posible 403). Abortando.');
  }

  let changed = true;
  if (existsSync(DEST_FILE)) {
    const prev = readFileSync(DEST_FILE);
    changed = !byteEqual(prev, res.body);
  }
  if (!changed) {
    console.log('Sin cambios respecto a proposals_latest.csv. No se hace commit.');
    return;
  }

  writeFileSync(DEST_FILE, res.body);
  console.log(`Actualizado ${DEST_FILE}`);

  // Write TS module with preserved text (for TS consumption)
  const text = decodePreservingDiacritics(res.body);
  const escaped = escapeForTemplateLiteral(text);
  const ts = `// Auto-generated from ${URL_STR}\n// Encoding preserved. Do not edit by hand.\nexport const proposalsCsv: string = \`${escaped}\`;\n\nexport function getProposalsCsvBlob(): Blob {\n  return new Blob([proposalsCsv], { type: 'text/csv;charset=utf-8' });\n}\n\nexport default proposalsCsv;\n`;
  writeFileSync(DEST_TS, ts, { encoding: 'utf8' });
  console.log(`Actualizado ${DEST_TS}`);

  // Run summary (compiled JS) if available
  const summaryJs = 'scripts/dist/decide_madrid_summary.js';
  const args = [summaryJs, '--in', DEST_FILE, '--compare-git', '--out-json', `${DEST_DIR}/proposals_summary.json`, '--out-md', `${DEST_DIR}/proposals_summary.md`];
  const r = spawnSync('node', args, { stdio: 'inherit' });
  if (r.status !== 0) {
    console.warn('Resumen TS/JS falló o no disponible; continúa sin bloquear el fetch.');
  }

  // Stage and commit
  spawnSync('git', ['add', DEST_FILE, DEST_TS, `${DEST_DIR}/proposals_summary.json`, `${DEST_DIR}/proposals_summary.md`], { stdio: 'inherit' });
  const rows = spawnSync('wc', ['-l', DEST_FILE], { encoding: 'utf8' }).stdout?.trim().split(/\s+/)[0] || '';
  const commit = spawnSync('git', ['-c', 'user.name=github-actions[bot]', '-c', 'user.email=41898282+github-actions[bot]@users.noreply.github.com', 'commit', '-m', `chore(decide-madrid): update proposals (latest, ${rows} rows)`], { stdio: 'inherit' });
  if ((commit.status ?? 0) !== 0) {
    console.log('No hay cambios para commitear.');
  }
}

main().catch((e) => {
  console.error(String(e?.message || e));
  process.exit(1);
});
