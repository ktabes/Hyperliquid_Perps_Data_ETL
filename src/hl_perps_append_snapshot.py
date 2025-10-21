#!/usr/bin/env python3
import os, json
from datetime import datetime, timezone, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

DUNE_API_KEY   = os.environ["DUNE_API_KEY"]               # set in GitHub Actions secrets
TABLE_NAME     = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")
DUNE_BASE      = "https://api.dune.com/api/v1/table"
HEADERS        = {"Content-Type": "application/json", "X-DUNE-API-KEY": DUNE_API_KEY}

LLAMA_URL      = "https://api.llama.fi/overview/derivatives"   # keyless snapshot table
SLUG           = "hyperliquid"

def http_json(url, timeout=45):
    req = Request(url, headers={"Accept":"application/json",
                                "User-Agent":"ktabes-hl-perps-snap/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def dune_post(path, payload):
    req = Request(f"{DUNE_BASE}/{path}",
                  data=json.dumps(payload).encode("utf-8"),
                  headers=HEADERS, method="POST")
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))

def ensure_table():
    # Idempotent create; ignore “already exists”.
    schema = [
        {"name":"date",                       "type":"date"},
        {"name":"volume24h_usd",             "type":"double"},
        {"name":"volume7d_usd",              "type":"double"},
        {"name":"volume30d_usd",             "type":"double"},
        {"name":"open_interest_usd",         "type":"double"},
        {"name":"total_volume_lifetime_usd", "type":"double"},
        {"name":"as_of_utc",                 "type":"timestamp"},
    ]
    payload = {"table_name": TABLE_NAME, "schema": schema, "is_private": False}
    try:
        dune_post("create", payload)
    except Exception as e:
        # swallow “already exists”
        if "already exists" not in str(e).lower():
            raise

def get_snapshot_row():
    j = http_json(LLAMA_URL)
    arr = j.get("protocols") or j.get("data") or []
    row = next((r for r in arr
                if isinstance(r, dict) and (r.get("slug") == SLUG
                    or str(r.get("name","")).lower().startswith("hyperliquid"))), None)
    if not row:
        raise SystemExit("Hyperliquid not found in overview/derivatives payload")

    today = date.today().isoformat()
    nowz  = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    def f(x): return float(x) if x is not None else 0.0

    return {
        "date": today,
        "volume24h_usd":             f(row.get("volume24h")),
        "volume7d_usd":              f(row.get("volume7d")),
        "volume30d_usd":             f(row.get("volume30d")),
        "open_interest_usd":         f(row.get("openInterest")),
        "total_volume_lifetime_usd": f(row.get("totalVolume")),
        "as_of_utc": nowz,
    }

def insert_row(r):
    dune_post("insert", {"table_name": TABLE_NAME, "data": [r]})

def main():
    ensure_table()
    r = get_snapshot_row()
    insert_row(r)
    print(f"Appended snapshot for {r['date']} -> {TABLE_NAME}")

if __name__ == "__main__":
    main()
