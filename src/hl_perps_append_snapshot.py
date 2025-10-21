#!/usr/bin/env python3
import os, json, io, csv, sys
from datetime import datetime, timezone, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError

# -------- Config --------
DUNE_API_KEY   = os.environ["DUNE_API_KEY"]
DUNE_NAMESPACE = os.environ.get("DUNE_NAMESPACE", "ktabes")  # your namespace
TABLE_NAME     = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")

DUNE_CREATE_URL = "https://api.dune.com/api/v1/table/create"
DUNE_INSERT_URL = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{TABLE_NAME}/insert"

HEADERS_JSON = {"Content-Type":"application/json", "X-DUNE-API-KEY": DUNE_API_KEY}
HEADERS_NDJ  = {"Content-Type":"application/x-ndjson", "X-DUNE-API-KEY": DUNE_API_KEY}
HEADERS_CSV  = {"Content-Type":"text/csv", "X-DUNE-API-KEY": DUNE_API_KEY}

LLAMA_URL = "https://api.llama.fi/overview/derivatives"
SLUG      = "hyperliquid"

def log(m): print(f"[hl-perps] {m}")

def http_json(url):
    req = Request(url, headers={"Accept":"application/json","User-Agent":"hl-perps-snap/1.2"})
    with urlopen(req, timeout=45) as resp:
        return json.loads(resp.read().decode("utf-8"))

def dune_post_json(url, payload):
    req = Request(url, data=json.dumps(payload).encode("utf-8"), headers=HEADERS_JSON, method="POST")
    try:
        with urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8","ignore") if e.fp else ""
        sys.exit(f"[create] {e.code} {body}")

def dune_post_bytes(url, body: bytes, headers: dict, label: str):
    req = Request(url, data=body, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=60) as resp:
            txt = resp.read().decode("utf-8","ignore")
            # Insert returns JSON like {"rows_written":1,"bytes_written":...}
            try:
                j = json.loads(txt)
            except Exception:
                j = {"raw": txt}
            log(f"{label} insert response: {j}")
            if isinstance(j, dict) and j.get("rows_written") in (0, None):
                sys.exit(f"{label} insert wrote 0 rows; response={j}")
            return j
    except HTTPError as e:
        body = e.read().decode("utf-8","ignore") if e.fp else ""
        sys.exit(f"[insert {label}] {e.code} {body}")

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
        "description": "Hyperliquid perps daily snapshot (DeFiLlama overview)",
        "is_private": False,
        "schema": schema
    }
    try:
        out = dune_post_json(DUNE_CREATE_URL, payload)
        log(f"create table response: {out}")
    except SystemExit as e:
        msg = str(e)
        if ("already exists" not in msg.lower()) and ("already existed" not in msg.lower()):
            raise
        log("table already exists, continuing")

def get_snapshot_row():
    j = http_json(LLAMA_URL)
    arr = j.get("protocols") or j.get("data") or []
    r = next((x for x in arr if isinstance(x, dict) and (x.get("slug")==SLUG or str(x.get("name","")).lower().startswith("hyperliquid"))), None)
    if not r:
        sys.exit("Hyperliquid not found in overview/derivatives payload")
    def f(v): return float(v) if v is not None else 0.0
    today = date.today().isoformat()
    nowz  = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    row = {
        "date": today,
        "volume24h_usd":             f(r.get("volume24h")),
        "volume7d_usd":              f(r.get("volume7d")),
        "volume30d_usd":             f(r.get("volume30d")),
        "open_interest_usd":         f(r.get("openInterest")),
        "total_volume_lifetime_usd": f(r.get("totalVolume")),
        "as_of_utc": nowz
    }
    log(f"snapshot row: {row}")
    return row

def row_to_ndjson(row: dict) -> bytes:
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

def main():
    log(f"target table: dune.{DUNE_NAMESPACE}.{TABLE_NAME}")
    ensure_table()
    row = get_snapshot_row()

    # Try NDJSON first (most permissive)
    try:
        ndj = row_to_ndjson(row)
        dune_post_bytes(DUNE_INSERT_URL, ndj, HEADERS_NDJ, "NDJSON")
        log("✅ inserted via NDJSON")
        return
    except SystemExit as e:
        log(f"NDJSON failed: {e}")

    # Fallback to CSV
    csv_bytes = row_to_csv(row)
    dune_post_bytes(DUNE_INSERT_URL, csv_bytes, HEADERS_CSV, "CSV")
    log("✅ inserted via CSV")

if __name__ == "__main__":
    main()