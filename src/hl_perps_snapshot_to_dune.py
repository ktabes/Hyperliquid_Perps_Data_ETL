import os, re, io, csv, time, json, sys
import datetime as dt
import requests
from urllib.parse import urljoin

DUNE_API_KEY = os.environ["DUNE_API_KEY"]
TABLE_NAME   = os.getenv("DUNE_TABLE_NAME", "hl_perps_daily")  # dune.<you>.dataset_hl_perps_daily
DUNE_UPLOAD_URL = "https://api.dune.com/api/v1/table/upload/csv"

# Public pages with a "Download .csv" button (no key required)
PERPS_PAGE = "https://defillama.com/protocols/derivatives"    # leaderboard with 24h/7d/30d + OI
OI_PAGE    = "https://defillama.com/open-interest"            # backup

def get_csv_url_from_page(page_url):
    html = requests.get(page_url, timeout=30).text
    m = re.search(r'href="([^"]+\\.csv)"', html, re.IGNORECASE)
    if not m:
        raise RuntimeError(f"No CSV link found on {page_url}")
    href = m.group(1)
    return href if href.startswith("http") else urljoin(page_url, href)

def download_csv(csv_url):
    r = requests.get(csv_url, timeout=60)
    r.raise_for_status()
    return r.text

def shape_csv(perps_csv_text):
    """
    Take the protocols leaderboard CSV and extract Hyperliquid snapshot.
    Output schema keeps a time column so you can chart over runs:
      date, volume_usd, open_interest_usd,
      snapshot_volume_24h, snapshot_volume_7d, snapshot_volume_30d,
      snapshot_open_interest_now, snapshot_total_volume, as_of_utc
    """
    import pandas as pd
    from io import StringIO
    df = pd.read_csv(StringIO(perps_csv_text))

    # Column names on the page:
    # Name | Perp Volume 24h | Open Interest | Perp Volume 7d | Perp Volume 30d | ...
    mask = df["Name"].str.fullmatch(r"Hyperliquid|Hyperliquid Perps", case=False, na=False)
    row = df[mask].head(1)
    if row.empty:
        # try contains if exact match fails
        row = df[df["Name"].str.contains("Hyperliquid", case=False, na=False)].head(1)
    if row.empty:
        raise RuntimeError("Hyperliquid not found in the CSV")

    v24 = float(row.iloc[0]["Perp Volume 24h"])
    v7  = float(row.iloc[0]["Perp Volume 7d"])
    v30 = float(row.iloc[0]["Perp Volume 30d"])
    oi  = float(row.iloc[0]["Open Interest"])

    today = dt.date.today().isoformat()
    as_of = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=[
        "date","volume_usd","open_interest_usd",
        "snapshot_volume_24h","snapshot_volume_7d","snapshot_volume_30d",
        "snapshot_open_interest_now","snapshot_total_volume","as_of_utc"
    ])
    writer.writeheader()
    writer.writerow({
        "date": today,
        "volume_usd": "",  # leaderboard CSV has no daily timeseries → leave blank
        "open_interest_usd": "",
        "snapshot_volume_24h": v24,
        "snapshot_volume_7d": v7,
        "snapshot_volume_30d": v30,
        "snapshot_open_interest_now": oi,
        "snapshot_total_volume": "",
        "as_of_utc": as_of
    })
    return out.getvalue()

def upload_csv_to_dune(csv_text, table_name):
    payload = {
        "data": csv_text.strip(),
        "description": "Hyperliquid perps snapshot (DeFiLlama public CSV, keyless)",
        "table_name": table_name,
        "is_private": False
    }
    headers = {"Content-Type": "application/json", "X-DUNE-API-KEY": DUNE_API_KEY}
    r = requests.post(DUNE_UPLOAD_URL, headers=headers, data=json.dumps(payload), timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Dune upload failed: {r.status_code} {r.text[:300]}")
    print("Dune upload ok:", r.text)

def main():
    pages = [PERPS_PAGE, OI_PAGE]
    last_err = None
    for page in pages:
        try:
            csv_url = get_csv_url_from_page(page)
            csv_text = download_csv(csv_url)
            shaped = shape_csv(csv_text)
            upload_csv_to_dune(shaped, TABLE_NAME)
            return
        except Exception as e:
            last_err = e
            time.sleep(3)
    raise last_err

if __name__ == "__main__":
    main()
