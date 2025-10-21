#!/usr/bin/env python3
import os, json, io, csv, sys
from datetime import datetime, timezone, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# ------------------- Config -------------------
DUNE_API_KEY   = os.environ["DUNE_API_KEY"]                    # required
DUNE_NAMESPACE = os.environ["DUNE_NAMESPACE"]                  # required: your dune username/team (case-sensitive in UI but use the exact namespace shown on Dune)
TABLE_NAME     = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")

# Create uses JSON; insert uses bytes (NDJSON or CSV) to the namespaced path.
DUNE_CREATE_URL = "https://api.dune.com/api/v1/table/create"
DUNE_INSERT_URL = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{TABLE_NAME}/insert"

HEADERS_JSON = {"Content-Type":"application/json", "X-DUNE-API-KEY": DUNE_API_KEY}
HEADERS_NDJ  = {"Content-Type":"application/x-ndjson", "X-DUNE-API-KEY": DUNE_API_KEY}
HEADERS_CSV  = {"Content-Type":"text/csv", "X-DUNE-API-KEY": DUNE_API_KEY}

# Keyless snapshot table
LLAMA_URL = "https://api.llama.fi/overview/derivatives"
SLUG      = "hyperliquid"

# ------------------- HTTP helpers -------------------
def http_json(url, headers=None, timeout=45):
    req = Request(url, headers=headers or {"Accept":"application/json"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def dune_post_json(url, payload):
    req = Request(url, data=json.dumps(payload).encode("utf-8"), headers=HEADERS_JSON, method="POST")
    try:
        with urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8","ignore") if e.fp else ""
        sys.exit(f"Dune JSON POST failed: {e.code} {body}")

def dune_post_bytes(url, body: bytes, headers: dict, label: str):
    req = Request(url, data=body, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=60) as resp:
            return resp.read().decode("utf-8","ignore")
    except HTTPError as e:
        body = e.read().decode("utf-8","ignore") if e.fp else ""
        raise SystemExit(f"Dune {label} insert failed: {e.code} {body}")

# ------------------- Dune table -------------------
def ensure_table():
    schema = [
        {"name":"date",                       "type":"date"},
        {"name":"volume24h_usd",             "type":"double"},
        {"name":"volume7d_usd",              "type":"double"},
        {"name":"volume30d_usd",             "type":"double"},
        {"name":"open_interest_usd",         "type":"double"},
        {"name":"total_volume_lifetime_usd", "type":"double"},
        {"name":"as_of_utc",                 "type":"timestamp"},
    ]
    payload = {
        "namespace": DUNE_NAMESPACE,
        "table_name": TABLE_NAME,
        "description": "Hyperliquid perps daily snapshot (DeFiLlama overview, keyless)",
        "is_private": False,
        "schema": schema
    }
    # Create is idempotent — ignore 'already exists'
    try:
        dune_post_json(DUNE_CREATE_URL, payload)
    except SystemExit as e:
        if "already exists" not in str(e).lower() and "already existed" not in str(e).lower():
            raise

# ------------------- Data logic -------------------
def get_snapshot_row():
    j = http_json(LLAMA_URL, headers={"Accept":"application/json","User-Agent":"hl-perps-snap/1.1"})
    arr = j.get("protocols") or j.get("data") or []
    r = next((x for x in arr if isinstance(x, dict) and (x.get("slug")==SLUG or str(x.get("name","")).lower().startswith("hyperliquid"))), None)
    if not r:
        sys.exit("Hyperliquid not found in overview/derivatives payload")
    def f(v): return float(v) if v is not None else 0.0
    today = date.today().isoformat()
    nowz  = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    return {
        "date": today,                                    # 'date' column (YYYY-MM-DD)
        "volume24h_usd":             f(r.get("volume24h")),
        "volume7d_usd":              f(r.get("volume7d")),
        "volume30d_usd":             f(r.get("volume30d")),
        "open_interest_usd":         f(r.get("openInterest")),
        "total_volume_lifetime_usd": f(r.get("totalVolume")),
        "as_of_utc": nowz                                 # ISO8601 timestamp
    }

def row_to_ndjson(row: dict) -> bytes:
    # single JSON line with exact column names
    return (json.dumps(row, separators=(",",":")) + "\n").encode("utf-8")

def row_to_csv(row: dict) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[
        "date","volume24h_usd","volume7d_usd","volume30d_usd",
        "open_interest_usd","total_volume_lifetime_usd","as_of_utc"
    ])
    w.writeheader()
    w.writerow(row)
    return buf.getvalue().encode("utf-8")

# ------------------- Main -------------------
def main():
    # 1) Ensure table exists with correct schema
    ensure_table()

    # 2) Build today’s row
    row = get_snapshot_row()

    # 3) Try NDJSON insert first (most permissive), then CSV as fallback, both to:
    #    POST /api/v1/table/{namespace}/{table}/insert
    try:
        ndj = row_to_ndjson(row)
        resp = dune_post_bytes(DUNE_INSERT_URL, ndj, HEADERS_NDJ, "NDJSON")
        print("Insert OK (NDJSON):", resp[:200])
        return
    except SystemExit as e:
        print("NDJSON insert failed, will try CSV. Details:", e)

    csv_bytes = row_to_csv(row)
    resp2 = dune_post_bytes(DUNE_INSERT_URL, csv_bytes, HEADERS_CSV, "CSV")
    print("Insert OK (CSV):", resp2[:200])

if __name__ == "__main__":
    main()
