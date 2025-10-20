#!/usr/bin/env python3
import csv, json, sys, os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ---------- repo-local paths ----------
REPO_ROOT = Path(os.getenv("GITHUB_WORKSPACE", Path(__file__).resolve().parents[1])).resolve()
DATA_DIR = (REPO_ROOT / "data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV   = DATA_DIR / "hyperliquid_perps_daily.csv"
DEBUG_JSON = DATA_DIR / "hyperliquid_perps_debug.json"

# ---------- config ----------
SLUG = "hyperliquid"  # protocol slug
# Try multiple shapes; keep keyless + free API style.
URLS = [
    # Protocol summary with daily series (preferred)
    f"https://api.llama.fi/summary/derivatives/{SLUG}",
    # Fallbacks (keep for future-proofing; harmless if 404)
    f"https://api.llama.fi/overview/derivatives?protocol={SLUG}",
]
HDRS = {"User-Agent": "ktabes-hl-perps-etl/1.0 (+github.com/ktabes)", "Accept": "application/json"}

# Dune CSV upload (overwrite)
DUNE_UPLOAD_URL = "https://api.dune.com/api/v1/table/upload/csv"
DUNE_API_KEY = os.getenv("DUNE_API_KEY")  # set in GH Actions secrets
DUNE_TABLE_NAME = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")

def log(m: str): print(f"[hl-perps] {m}")

# ---------- HTTP + JSON ----------
def fetch_first_json(urls: List[str]) -> Any:
    last = None
    for u in urls:
        try:
            log(f"GET {u}")
            req = Request(u, headers=HDRS)
            with urlopen(req, timeout=45) as resp:
                raw = resp.read()
                log(f"Status {resp.status}, {len(raw)} bytes")
                j = json.loads(raw.decode("utf-8"))
                DEBUG_JSON.write_text(json.dumps(j, indent=2, sort_keys=True))
                log(f"Wrote debug JSON → {DEBUG_JSON}")
                return j
        except Exception as e:
            last = e
            log(f"Error: {e}")
    raise SystemExit(f"❌ failed to fetch any derivatives endpoint: {last}")

# ---------- shape detection ----------
def pick_daily_series(j: Any) -> List[Dict[str, Any]]:
    """
    Expect a daily time series for perps with fields like:
      - dailyVolume: [{ "date": <unix>, "volume": <usd>, "openInterest": <usd> }, ...]
    Return a list of dicts with keys we’ll normalize from.
    """
    if not isinstance(j, dict):
        return []
    # direct field
    dv = j.get("dailyVolume")
    if isinstance(dv, list) and dv:
        return dv
    # nested
    data = j.get("data", {})
    dv = data.get("dailyVolume")
    if isinstance(dv, list) and dv:
        return dv
    return []

def pick_snapshot(j: Any) -> Dict[str, Any]:
    """
    Snapshot counters if present: volume24h, volume7d, volume30d, openInterest, totalVolume.
    """
    src = j if isinstance(j, dict) else {}
    data = src.get("data", {})
    out = {}
    for k in ("volume24h","volume7d","volume30d","openInterest","totalVolume"):
        out[k] = src.get(k, data.get(k))
    return out

# ---------- normalization ----------
def ts_to_date_str(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()

def normalize_rows(daily: List[Dict[str, Any]], snap: Dict[str, Any]) -> List[List[str]]:
    rows, bad = [], 0
    for item in daily:
        try:
            ts = item.get("date")
            vol = item.get("volume")
            oi  = item.get("openInterest")
            if ts is None: bad += 1; continue
            d = ts_to_date_str(int(ts))
            rows.append([
                d,
                "" if vol is None else f"{float(vol):.6f}",
                "" if oi  is None else f"{float(oi):.6f}",
                "" if snap.get("volume24h")     is None else f"{float(snap['volume24h']):.6f}",
                "" if snap.get("volume7d")      is None else f"{float(snap['volume7d']):.6f}",
                "" if snap.get("volume30d")     is None else f"{float(snap['volume30d']):.6f}",
                "" if snap.get("openInterest")  is None else f"{float(snap['openInterest']):.6f}",
                "" if snap.get("totalVolume")   is None else f"{float(snap['totalVolume']):.6f}",
                datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z"),
            ])
        except Exception:
            bad += 1
    log(f"Parsed {len(rows)} rows; skipped {bad}")
    # De-dup by date (keep last)
    last_by_date: Dict[str, List[str]] = {}
    for r in rows: last_by_date[r[0]] = r
    return [last_by_date[k] for k in sorted(last_by_date.keys())]

def write_csv(rows: List[List[str]]) -> None:
    header = [
        "date",
        "volume_usd",
        "open_interest_usd",
        "snapshot_volume_24h",
        "snapshot_volume_7d",
        "snapshot_volume_30d",
        "snapshot_open_interest_now",
        "snapshot_total_volume",
        "as_of_utc",
    ]
    with OUT_CSV.open("w", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    log(f"✅ wrote {OUT_CSV} with {len(rows)} rows")

# ---------- Dune upload ----------
def upload_to_dune(csv_path: Path) -> None:
    if not DUNE_API_KEY:
        raise SystemExit("❌ DUNE_API_KEY missing (GitHub Actions secret)")
    data = csv_path.read_text().strip()
    payload = {
        "data": data,
        "description": "Hyperliquid perps: volume & open interest (DeFiLlama free API)",
        "table_name": DUNE_TABLE_NAME,
        "is_private": False,
    }
    import urllib.request
    req = Request(
        DUNE_UPLOAD_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-DUNE-API-KEY": DUNE_API_KEY,
            **HDRS,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", "ignore")
            log(f"Dune upload ok: {raw[:200]}...")
    except HTTPError as e:
        body = e.read().decode("utf-8","ignore") if e.fp else ""
        raise SystemExit(f"❌ Dune upload failed: {e.code} {body[:300]}")
    except URLError as e:
        raise SystemExit(f"❌ Dune upload failed (network): {e}")

def main():
    j = fetch_first_json(URLS)
    daily = pick_daily_series(j)
    snap  = pick_snapshot(j)
    if not daily:
        # Still produce an empty CSV with snapshot-only row if present
        log("No daily series found; emitting snapshot-only row based on debug JSON.")
        today = datetime.now(timezone.utc).date().isoformat()
        rows = normalize_rows([{"date": int(datetime.now(timezone.utc).timestamp())}], snap)
    else:
        rows = normalize_rows(daily, snap)
    write_csv(rows)
    upload_to_dune(OUT_CSV)

if __name__ == "__main__":
    main()