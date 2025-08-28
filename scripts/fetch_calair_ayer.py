#!/usr/bin/env python3
from __future__ import annotations
import json, csv, sys, re, math, time, argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import ssl
from zoneinfo import ZoneInfo
import certifi

API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_historico.json"

def http_post_json(url: str, payload: dict, timeout: int = 90, retries: int = 5, backoff: float = 1.5):
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=data,
        headers={
            "User-Agent": "github-action-calair/2.3",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    ctx = ssl.create_default_context(cafile=certifi.where())

    last_err = None
    for i in range(retries):
        try:
            with urlopen(req, timeout=timeout, context=ctx) as resp:
                raw_bytes = resp.read()
                try:
                    # primer intento: utf-8
                    raw_text = raw_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    # fallback: latin-1
                    raw_text = raw_bytes.decode("latin-1")
                return json.loads(raw_text), raw_bytes
        except (HTTPError, URLError, ssl.SSLError, UnicodeDecodeError) as e:
            last_err = e
            sleep_s = (backoff ** i) + 0.1
            print(f"⚠️  Intento {i+1}/{retries} fallido: {e}. Reintentando en {sleep_s:.1f}s...", flush=True)
            time.sleep(sleep_s)
    raise last_err

# (resto del script igual que la última versión con --yesterday y --date)
