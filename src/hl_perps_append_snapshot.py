#!/usr/bin/env python3
import os, json, io, csv, sys, time
from datetime import datetime, timezone, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# -------- Config --------
DUNE_API_KEY   = os.environ["DUNE_API_KEY"]
DUNE_NAMESPACE = os.environ.get("DUNE_NAMESPACE", "ktabes")
TABLE_NAME     = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")

DUNE_CREATE_URL = "https://api.dune.com/api/v1/table/create"
DUNE_INSERT_URL = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{TABLE_NAME}/insert"

HEADERS_JSON = {"Content-Type":"application/json", "X-DUNE-API-KEY": DUNE_API_KEY}
HEADERS_NDJ  = {"Content-Type":"application/x-ndjson", "X-DUNE-API-KEY": DUNE_API_KEY}

# Hyperliquid public API (no key)
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

def log(m): print(f"[hl-perps] {m}")

def post_json(url, payload, timeout=45):
    req = Request(url, data=json.dumps(payload).encode("utf-8"),
                  headers={"Content-Type":"application/json","Accept":"application/json",
                           "User-Agent":"ktabes-hl-perps/1.0"},
                  method="POST")
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def dune_post_json(url, payload):
    req = Request(url, data=json.dumps(payload).encode("utf-8"),
                  headers=HEADERS_JSON, method="POST")
    try:
        with urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8","ignore") if e.fp else ""
        sys.exit(f"[create] {e.code} {body}")

def dune_insert_ndjson(url, rows):
    body = ("\n".join(json.dumps(r, separators=(",",":")) for r in rows) + "\n").encode("utf-8")
    req = Request(url, data=body, headers=HEADERS_NDJ, method="POST")
    try:
        with urlopen(req, timeout=60) as resp:
            txt = resp.read().decode("utf-8","ignore")
            try:
                j = json.loads(txt)
            except Exception:
                j = {"raw": txt}
            log(f"insert response: {j}")
            if isinstance(j, dict) and j.get("rows_written") in (0, None):
                sys.exit(f"insert wrote 0 rows; response={j}")
            return j
    except HTTPError as e:
        body = e.read().decode("utf-8","ignore") if e.fp else ""
        sys.exit(f"[insert] {e.code} {body}")

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
        "description": "Hyperliquid perps daily snapshot (Hyperliquid info API: sum of assetCtxs)",
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

def get_hl_totals():
    """
    Call Hyperliquid info API with type=metaAndAssetCtxs, then sum:
      - open_interest_usd = sum(float(asset.openInterest))
      - volume24h_usd     = sum(float(asset.dayNtlVlm))
    The payload returns a two-element array: [meta, assetCtxs[]]
    """
    for attempt in range(3):
        try:
            resp = post_json(HL_INFO_URL, {"type": "metaAndAssetCtxs"})
            break
        except URLError:
            if attempt == 2: raise
            time.sleep(2)

    if not isinstance(resp, list) or len(resp) < 2:
        sys.exit("Unexpected HL response shape.")
    asset_ctxs = resp[1]
    oi_total = 0.0
    vol24_total = 0.0
    for a in asset_ctxs:
        # Fields are strings in docs; coerce carefully
        oi_total   += float(a.get("openInterest", "0") or 0)
        vol24_total+= float(a.get("dayNtlVlm",   "0") or 0)

    return oi_total, vol24_total

def get_snapshot_row():
    oi, vol24 = get_hl_totals()
    # We can't get 7d/30d/lifetime directly from HL; leave 0.0 so tiles can still show latest 24h & OI.
    today = date.today().isoformat()
    nowz  = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    row = {
        "date": today,
        "volume24h_usd":             float(vol24),
        "volume7d_usd":              0.0,
        "volume30d_usd":             0.0,
        "open_interest_usd":         float(oi),
        "total_volume_lifetime_usd": 0.0,
        "as_of_utc": nowz
    }
    log(f"snapshot row: {row}")
    return row

def main():
    log(f"target table: dune.{DUNE_NAMESPACE}.{TABLE_NAME}")
    ensure_table()
    row = get_snapshot_row()
    dune_insert_ndjson(DUNE_INSERT_URL, [row])
    log("✅ inserted via NDJSON")

if __name__ == "__main__":
    main()
