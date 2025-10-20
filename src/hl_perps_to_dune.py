import os, io, csv, time, json, sys
import datetime as dt
import requests

# ---------------- Config ----------------
TABLE_NAME = os.getenv("DUNE_TABLE_NAME", "hl_perps_daily")  # final will be dune.<you>.dataset_hl_perps_daily
ROLLING_DAYS = int(os.getenv("ROLLING_DAYS", "400"))         # keep it compact for free upload size
DUNE_API_KEY = os.environ["DUNE_API_KEY"]

# DeFiLlama derivatives summary for Hyperliquid (free plan works; you’re just rate-limited)
LLAMA_URL = os.getenv(
    "LLAMA_URL",
    "https://pro-api.llama.fi/api/summary/derivatives/hyperliquid"
)

DUNE_UPLOAD_URL = "https://api.dune.com/api/v1/table/upload/csv"

# --------------- Helpers ----------------
def http_get_json(url, retries=3, timeout=30):
    for i in range(retries):
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        # basic backoff
        time.sleep(2 * (i + 1))
    raise RuntimeError(f"GET failed {retries}x: {url} (status={r.status_code}, body={r.text[:200]})")

def build_csv(daily_rows, snapshot, as_of_utc):
    """
    daily_rows: list of dicts with keys: date (unix secs), volume, openInterest
    snapshot: dict with keys volume24h, volume7d, volume30d, openInterest, totalVolume
    Output CSV schema (typed in SQL-friendly way):
      date, volume_usd, open_interest_usd, snapshot_volume_24h, snapshot_volume_7d,
      snapshot_volume_30d, snapshot_open_interest_now, snapshot_total_volume, as_of_utc
    """
    # Keep rolling window
    cutoff = dt.datetime.utcnow().date() - dt.timedelta(days=ROLLING_DAYS)
    rows = []
    for r in daily_rows:
        # some payloads use 'date' in seconds
        d = dt.datetime.utcfromtimestamp(int(r["date"])).date()
        if d < cutoff:
            continue
        rows.append({
            "date": d.isoformat(),
            "volume_usd": r.get("volume"),
            "open_interest_usd": r.get("openInterest"),
            "snapshot_volume_24h": snapshot.get("volume24h"),
            "snapshot_volume_7d": snapshot.get("volume7d"),
            "snapshot_volume_30d": snapshot.get("volume30d"),
            "snapshot_open_interest_now": snapshot.get("openInterest"),
            "snapshot_total_volume": snapshot.get("totalVolume"),
            "as_of_utc": as_of_utc
        })

    # Convert to CSV text (no BOM)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()

def upload_csv_to_dune(csv_text, table_name, description="Hyperliquid perps: volume & open interest from DeFiLlama (free API)"):
    payload = {
        "data": csv_text.strip(),
        "description": description,
        "table_name": table_name,
        "is_private": False
    }
    headers = {
        "Content-Type": "application/json",
        "X-DUNE-API-KEY": DUNE_API_KEY
    }
    r = requests.post(DUNE_UPLOAD_URL, headers=headers, data=json.dumps(payload), timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Dune upload failed: {r.status_code} {r.text[:300]}")
    print("Dune upload ok:", r.text)

def main():
    # 1) Pull DeFiLlama Hyperliquid summary (free)
    res = http_get_json(LLAMA_URL)
    # Expected keys: volume24h, volume7d, volume30d, openInterest, totalVolume, dailyVolume[]
    daily = res.get("dailyVolume", [])
    if not daily:
        raise RuntimeError("No dailyVolume in DeFiLlama payload—cannot build time series.")

    snapshot = {
        "volume24h": res.get("volume24h"),
        "volume7d": res.get("volume7d"),
        "volume30d": res.get("volume30d"),
        "openInterest": res.get("openInterest"),
        "totalVolume": res.get("totalVolume"),
    }
    as_of_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # 2) Shape CSV (rolling window to fit free Dune storage)
    csv_text = build_csv(daily, snapshot, as_of_utc)

    # 3) Upload to Dune (overwrites table)
    upload_csv_to_dune(csv_text, TABLE_NAME)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
