#!/usr/bin/env python3
import os, csv, json
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

REPO_ROOT = Path(os.getenv("GITHUB_WORKSPACE", Path(__file__).resolve().parents[1])).resolve()
DATA_DIR = (REPO_ROOT / "data"); DATA_DIR.mkdir(parents=True, exist_ok=True)

IN_VOL = DATA_DIR / "hyperliquid_perps_volume.csv"
IN_OI  = DATA_DIR / "hyperliquid_perps_open_interest.csv"
OUT_CSV = DATA_DIR / "hyperliquid_perps_daily.csv"

DUNE_UPLOAD_URL = "https://api.dune.com/api/v1/table/upload/csv"
DUNE_API_KEY = os.getenv("DUNE_API_KEY")           # set as Actions secret
DUNE_TABLE_NAME = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")

def log(m): print(f"[hl-merge] {m}")

def read_first_series(csv_path: Path, name_key="Name"):
    """
    Parse the downloaded CSV (table export) and extract Hyperliquid’s daily series.
    DeFiLlama CSV formats vary slightly by page, but generally include columns like:
      Date, Name, Value, (sometimes multiple metrics)
    We’ll look for:
      - For volume CSV: a column containing "Volume" or "Perp Volume", take the daily series.
      - For OI CSV: a column containing "Open Interest", take the daily series.
    """
    if not csv_path.exists():
      raise SystemExit(f"Missing CSV: {csv_path}")

    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Heuristics: figure metric columns
    fieldnames = [c.lower() for c in (rows[0].keys() if rows else [])]
    # Find typical column names
    date_col = next((c for c in rows[0].keys() if c.lower() in ("date","day","timestamp")), None)
    name_col = next((c for c in rows[0].keys() if c.lower() in (name_key.lower(), "protocol","dex","project")), None)
    # Potential metric columns
    vol_cols = [c for c in rows[0].keys() if "volume" in c.lower()]
    oi_cols  = [c for c in rows[0].keys() if "open interest" in c.lower() or c.lower() == "openinterest"]

    series = []
    for r in rows:
        name = (r.get(name_col) or "").strip()
        if not name or "hyperliquid" not in name.lower():
            continue
        d_raw = r.get(date_col)
        if not d_raw:
            continue
        # Normalize date to YYYY-MM-DD
        try:
            # CSVs typically export ISO or YYYY-MM-DD; handle timestamps too
            if d_raw.isdigit():
                d = datetime.utcfromtimestamp(int(d_raw)).date().isoformat()
            else:
                d = datetime.fromisoformat(d_raw.replace("Z","")).date().isoformat()
        except Exception:
            # Last resort: try simple slice
            d = d_raw[:10]

        entry = {"date": d}
        # Try to parse metric numbers (strip $/commas)
        def parse_num(s):
            if s is None or s == "": return None
            s = s.replace("$","").replace(",","").strip()
            try:
                return float(s)
            except Exception:
                return None

        # capture both volume and OI fields if present
        for c in vol_cols:
            entry["volume"] = parse_num(r.get(c))
            if entry["volume"] is not None:
                break
        for c in oi_cols:
            entry["oi"] = parse_num(r.get(c))
            if entry["oi"] is not None:
                break

        series.append(entry)

    # De-dup by date (keep last)
    dd = {}
    for e in series:
        dd[e["date"]] = e
    return dd  # dict: date -> {date, volume?, oi?}

def merge_series(vol_map, oi_map):
    dates = sorted(set(vol_map.keys()) | set(oi_map.keys()))
    out = []
    for d in dates:
        v = vol_map.get(d, {})
        o = oi_map.get(d, {})
        volume = v.get("volume", o.get("volume"))
        oi     = o.get("oi", v.get("oi"))
        out.append([d,
                    "" if volume is None else f"{float(volume):.6f}",
                    "" if oi is None else f"{float(oi):.6f}",
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")])
    return out

def write_csv(rows):
    with OUT_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date","volume_usd","open_interest_usd","as_of_utc"])
        w.writerows(rows)
    log(f"wrote {OUT_CSV} rows={len(rows)}")

def upload_to_dune():
    if not DUNE_API_KEY:
        raise SystemExit("DUNE_API_KEY missing")
    payload = {
        "data": OUT_CSV.read_text().strip(),
        "description": "Hyperliquid perps (Daily): volume & open interest (DeFiLlama CSV, keyless)",
        "table_name": DUNE_TABLE_NAME,
        "is_private": False,
    }
    req = Request(
        DUNE_UPLOAD_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type":"application/json","X-DUNE-API-KEY": DUNE_API_KEY},
        method="POST"
    )
    try:
        with urlopen(req, timeout=60) as resp:
            log(f"Dune upload ok: {resp.read().decode('utf-8')[:200]}...")
    except HTTPError as e:
        body = e.read().decode("utf-8","ignore") if e.fp else ""
        raise SystemExit(f"Dune upload failed: {e.code} {body[:300]}")
    except URLError as e:
        raise SystemExit(f"Dune upload failed (network): {e}")

def main():
    vol_map = read_first_series(IN_VOL)
    oi_map  = read_first_series(IN_OI)
    rows = merge_series(vol_map, oi_map)
    write_csv(rows)
    upload_to_dune()

if __name__ == "__main__":
    main()