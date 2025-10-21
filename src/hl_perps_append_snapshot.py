#!/usr/bin/env python3
import os, json, io, csv
from datetime import datetime, timezone, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

DUNE_API_KEY   = os.environ["DUNE_API_KEY"]                    # required
DUNE_NAMESPACE = os.environ["DUNE_NAMESPACE"]                  # required: your dune username/team
TABLE_NAME     = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")

# Dune endpoints (create uses JSON; insert uses CSV to the namespaced path)
DUNE_CREATE_URL = "https://api.dune.com/api/v1/table/create"
DUNE_INSERT_URL = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{TABLE_NAME}/insert"

HEADERS_JSON = {"Content-Type":"application/json", "X-DUNE-API-KEY": DUNE_API_KEY}
HEADERS_CSV  = {"Content-Type":"text/csv",        "X-DUNE-API-KEY": DUNE_API_KEY}

# Keyless snapshot table with 24h/7d/30d volume & OI
LLAMA_URL = "https://api.llama.fi/overview/derivatives"
SLUG      = "hyperliquid"

def http_json(url, headers=None, timeout=45):
    req = Request(url, headers=headers or {"Accept":"application/json"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def dune_post_json(url, payload):
    req = Request(url, data=json.dumps(payload).encode("utf-8"), headers=HEADERS_JSON, method="POST")
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))

def dune_post_csv(url, csv_bytes):
    req = Request(url, data=csv_bytes, headers=HEADERS_CSV, method="POST")
    with urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8")

def ensure_table():
    schema = [
        {"name":"date",                       "type":"date"},
        {"name":"volume24h_usd",             "type":"double"},
        {"name":"volume7d_usd",              "type":"double"},
        {"name":"volume30d_usd",             "type":"double"},
        {"name":"open_interest_usd",         "type":"double"},
        {"name":"total_volume_lifetime_usd", "type":"double"},
        {"name":"as_of_utc",                 "type":"timestamp"}
    ]
    payload = {
        "namespace": DUNE_NAMESPACE,            # explicit namespace
        "table_name": TABLE_NAME,
        "description": "Hyperliquid perps daily snapshot (keyless DeFiLlama overview)",
        "is_private": False,
        "schema": schema
    }
    try:
        dune_post_json(DUNE_CREATE_URL, payload)
    except HTTPError as e:
        msg = e.read().decode("utf-8","ignore")
        # ignore if already exists
        if "already existed" not in msg and "already exists" not in msg:
            raise

def get_snapshot_row():
    j = http_json(LLAMA_URL, headers={"Accept":"application/json","User-Agent":"hl-perps-snap/1.0"})
    arr = j.get("protocols") or j.get("data") or []
    r = next((x for x in arr if isinstance(x, dict) and (x.get("slug")==SLUG or str(x.get("name","")).lower().startswith("hyperliquid"))), None)
    if not r:
        raise SystemExit("Hyperliquid not found in overview/derivatives payload")

    def f(v): return float(v) if v is not None else 0.0
    today = date.today().isoformat()
    nowz  = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    return {
        "date": today,
        "volume24h_usd":             f(r.get("volume24h")),
        "volume7d_usd":              f(r.get("volume7d")),
        "volume30d_usd":             f(r.get("volume30d")),
        "open_interest_usd":         f(r.get("openInterest")),
        "total_volume_lifetime_usd": f(r.get("totalVolume")),
        "as_of_utc": nowz
    }

def row_to_csv_bytes(row: dict) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[
        "date","volume24h_usd","volume7d_usd","volume30d_usd","open_interest_usd","total_volume_lifetime_usd","as_of_utc"
    ])
    w.writeheader()
    w.writerow(row)
    return buf.getvalue().encode("utf-8")

def main():
    ensure_table()
    row = get_snapshot_row()
    csv_bytes = row_to_csv_bytes(row)
    dune_post_csv(DUNE_INSERT_URL, csv_bytes)
    print(f"Appended snapshot for {row['date']} to dune.{DUNE_NAMESPACE}.{TABLE_NAME}")

if __name__ == "__main__":
    main()
