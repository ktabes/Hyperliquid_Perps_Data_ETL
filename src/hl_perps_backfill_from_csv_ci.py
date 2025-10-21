#!/usr/bin/env python3
import os, csv, json
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

DUNE_API_KEY = os.environ["DUNE_API_KEY"]
TABLE_NAME   = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")
DUNE_BASE    = "https://api.dune.com/api/v1/table"
HDRS         = {"Content-Type":"application/json", "X-DUNE-API-KEY": DUNE_API_KEY}

VOLUME_CSV = os.getenv("VOLUME_CSV", "/tmp/hyperliquid_perps_volume.csv")
OI_CSV     = os.getenv("OI_CSV", "/tmp/hyperliquid_perps_open_interest.csv")

def dune_post(path, payload):
    req = Request(f"{DUNE_BASE}/{path}", data=json.dumps(payload).encode("utf-8"),
                  headers=HDRS, method="POST")
    with urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))

def ensure_table():
    schema = [
        {"name":"date",                 "type":"date"},
        {"name":"volume_usd",          "type":"double"},
        {"name":"open_interest_usd",   "type":"double"},
        {"name":"as_of_utc",           "type":"timestamp"}
    ]
    payload = {"table_name": TABLE_NAME, "schema": schema, "is_private": False}
    try:
        dune_post("create", payload)
    except Exception as e:
        # ignore "already exists"
        if "already exists" not in str(e).lower():
            raise

def parse_csv(path):
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"missing CSV: {path}")
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)
    return rows

def extract_series(rows, is_volume):
    # Try to infer col names
    if not rows:
        return {}
    cols = rows[0].keys()
    date_col = next((c for c in cols if c.lower() in ("date","day","timestamp")), None)
    name_col = next((c for c in cols if c.lower() in ("name","protocol","dex","project")), None)
    metric_cols = [c for c in cols if ("volume" in c.lower())] if is_volume else \
                  [c for c in cols if ("open interest" in c.lower() or c.lower()=="openinterest")]
    out = {}
    for r in rows:
        name = (r.get(name_col) or "").lower()
        if "hyperliquid" not in name:
            continue
        d_raw = r.get(date_col) or ""
        # normalize date
        try:
            d = datetime.fromisoformat(d_raw.replace("Z","")).date().isoformat()
        except Exception:
            d = d_raw[:10]
        # parse number
        def num(s):
            if not s: return None
            s = s.replace("$","").replace(",","").strip()
            try: return float(s)
            except: return None
        val = None
        for c in metric_cols:
            val = num(r.get(c))
            if val is not None: break
        if d:
            out[d] = val
    return out  # {date: value}

def batch_insert(rows, batch=500):
    for i in range(0, len(rows), batch):
        payload = {"table_name": TABLE_NAME, "data": rows[i:i+batch]}
        dune_post("insert", payload)

def main():
    ensure_table()
    vol_rows = parse_csv(VOLUME_CSV)
    oi_rows  = parse_csv(OI_CSV)

    vol = extract_series(vol_rows, True)
    oi  = extract_series(oi_rows,  False)

    dates = sorted(set(vol) | set(oi))
    nowz  = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    data = []
    for d in dates:
        data.append({
            "date": d,
            "volume_usd": float(vol.get(d) or 0),
            "open_interest_usd": float(oi.get(d) or 0),
            "as_of_utc": nowz
        })
    if not data:
        raise SystemExit("no merged rows")

    batch_insert(data)
    print(f"Inserted {len(data)} rows into {TABLE_NAME}")

if __name__ == "__main__":
    main()