#!/usr/bin/env python3
import csv, json, sys, os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ---------------- Repo paths ----------------
REPO_ROOT = Path(os.getenv("GITHUB_WORKSPACE", Path(__file__).resolve().parents[1])).resolve()
DATA_DIR = (REPO_ROOT / "data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = DATA_DIR / "hyperliquid_perps_daily.csv"
DEBUG_JSON = DATA_DIR / "hyperliquid_perps_debug.json"

# ---------------- Config ----------------
SLUG = "hyperliquid"
URLS = [
    f"https://api.llama.fi/summary/derivatives/{SLUG}",  # primary (may include dailyVolume + snapshot)
    # (kept for future-proofing; harmless if unused)
    f"https://api.llama.fi/overview/derivatives?protocol={SLUG}",
]
OVERVIEW_URL = "https://api.llama.fi/overview/derivatives"  # snapshot fallback (no timeseries)

HDRS = {
    "User-Agent": "ktabes-hl-perps-etl/1.2 (+github.com/ktabes)",
    "Accept": "application/json",
}

# Dune CSV upload (overwrite semantics)
DUNE_UPLOAD_URL = "https://api.dune.com/api/v1/table/upload/csv"
DUNE_API_KEY = os.getenv("DUNE_API_KEY")  # REQUIRED
DUNE_TABLE_NAME = os.getenv("DUNE_TABLE_NAME", "hyperliquid_perps_daily")

def log(m: str) -> None:
    print(f"[hl-perps] {m}")

# ---------------- HTTP ----------------
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

def fetch_overview_snapshot(slug: str) -> Dict[str, Any]:
    """Keyless fallback: global derivatives overview (per-protocol snapshot)."""
    req = Request(OVERVIEW_URL, headers=HDRS)
    with urlopen(req, timeout=45) as resp:
        j = json.loads(resp.read().decode("utf-8"))
    arr = j.get("protocols") or j.get("data") or []
    for row in arr:
        if not isinstance(row, dict):
            continue
        if row.get("slug") == slug or str(row.get("name", "")).lower().startswith("hyperliquid"):
            return {
                "volume24h": row.get("volume24h"),
                "volume7d": row.get("volume7d"),
                "volume30d": row.get("volume30d"),
                "openInterest": row.get("openInterest"),
                "totalVolume": row.get("totalVolume"),
            }
    return {}

# ---------------- Parsing helpers ----------------
def pick_daily_series(j: Any) -> List[Dict[str, Any]]:
    """
    Prefer canonical: j['dailyVolume'] = [{date, volume, openInterest}]
    Accept several fallback shapes if needed.
    """
    if not isinstance(j, dict):
        return []

    # 1) canonical
    dv = j.get("dailyVolume")
    if isinstance(dv, list) and dv:
        return dv

    # 2) nested
    data = j.get("data", {})
    dv = data.get("dailyVolume")
    if isinstance(dv, list) and dv:
        return dv

    # 3) generic nests (defensive)
    for k in ("daily", "series", "chart", "timeseries"):
        blob = j.get(k, {}) or data.get(k, {})
        # dict of arrays
        if isinstance(blob, dict):
            for kk in ("dailyVolume", "volume", "volumes"):
                cand = blob.get(kk)
                if isinstance(cand, list) and cand:
                    out = []
                    for el in cand:
                        if isinstance(el, dict):
                            out.append({
                                "date": el.get("date") or el.get("timestamp") or el.get("time"),
                                "volume": el.get("volume") or el.get("value"),
                                "openInterest": el.get("openInterest") or el.get("oi"),
                            })
                        elif isinstance(el, (list, tuple)) and len(el) >= 2:
                            out.append({"date": el[0], "volume": el[1], "openInterest": None})
                    if out:
                        return out
        # list of points
        if isinstance(blob, list) and blob:
            out = []
            for el in blob:
                if isinstance(el, dict):
                    out.append({
                        "date": el.get("date") or el.get("timestamp") or el.get("time"),
                        "volume": el.get("volume") or el.get("value"),
                        "openInterest": el.get("openInterest") or el.get("oi"),
                    })
                elif isinstance(el, (list, tuple)) and len(el) >= 2:
                    out.append({"date": el[0], "volume": el[1], "openInterest": None})
            if out:
                return out

    return []

def _first_present(d: Dict[str, Any], keys: List[str]):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def pick_snapshot(j: Any) -> Dict[str, Any]:
    """
    Hunt snapshot counters in common spots and aliases:
    volume24h/7d/30d, openInterest, totalVolume
    """
    out = {"volume24h": None, "volume7d": None, "volume30d": None, "openInterest": None, "totalVolume": None}
    pools = [j] if isinstance(j, dict) else []
    if pools:
        for k in ("data", "protocol", "metrics", "summary"):
            v = j.get(k)
            if isinstance(v, dict):
                pools.append(v)

    for blob in pools:
        out["volume24h"]    = out["volume24h"]    or _first_present(blob, ["volume24h","volume24hUsd","vol24h"])
        out["volume7d"]     = out["volume7d"]     or _first_present(blob, ["volume7d","volume7dUsd","vol7d"])
        out["volume30d"]    = out["volume30d"]    or _first_present(blob, ["volume30d","volume30dUsd","vol30d"])
        out["openInterest"] = out["openInterest"] or _first_present(blob, ["openInterest","openInterestUsd","oi","oiUsd"])
        out["totalVolume"]  = out["totalVolume"]  or _first_present(blob, ["totalVolume","totalVolumeUsd","lifetimeVolume"])

    return out

# ---------------- Normalization ----------------
def ts_to_date_str(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()

def normalize_rows(daily: List[Dict[str, Any]], snap: Dict[str, Any]) -> List[List[str]]:
    """
    Output schema:
      date, volume_usd, open_interest_usd,
      snapshot_volume_24h, snapshot_volume_7d, snapshot_volume_30d,
      snapshot_open_interest_now, snapshot_total_volume, as_of_utc
    """
    rows, bad = [], 0
    for item in daily:
        try:
            ts = item.get("date")
            vol = item.get("volume")
            oi  = item.get("openInterest")
            if ts is None:
                bad += 1; continue
            d = ts_to_date_str(int(ts))
            rows.append([
                d,
                "" if vol is None else f"{float(vol):.6f}",
                "" if oi  is None else f"{float(oi):.6f}",
                "" if snap.get("volume24h")    is None else f"{float(snap['volume24h']):.6f}",
                "" if snap.get("volume7d")     is None else f"{float(snap['volume7d']):.6f}",
                "" if snap.get("volume30d")    is None else f"{float(snap['volume30d']):.6f}",
                "" if snap.get("openInterest") is None else f"{float(snap['openInterest']):.6f}",
                "" if snap.get("totalVolume")  is None else f"{float(snap['totalVolume']):.6f}",
                datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z"),
            ])
        except Exception:
            bad += 1
    log(f"Parsed {len(rows)} rows; skipped {bad}")

    # De-dup by date (keep last)
    last_by_date: Dict[str, List[str]] = {}
    for r in rows:
        last_by_date[r[0]] = r
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

# ---------------- Dune upload ----------------
def upload_to_dune(csv_path: Path) -> None:
    if not DUNE_API_KEY:
        raise SystemExit("❌ DUNE_API_KEY missing (GitHub Actions secret)")
    data = csv_path.read_text().strip()
    payload = {
        "data": data,
        "description": "Hyperliquid perps: volume & open interest (DeFiLlama free API with overview fallback)",
        "table_name": DUNE_TABLE_NAME,
        "is_private": False,
    }
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

# ---------------- Main ----------------
def main():
    j = fetch_first_json(URLS)

    daily = pick_daily_series(j)          # may be []
    snap  = pick_snapshot(j)              # may be partial

    # Fill missing counters from overview if needed
    if any(snap.get(k) is None for k in ("volume24h","volume7d","volume30d","openInterest")):
        log("Snapshot incomplete; fetching overview fallback…")
        ov = fetch_overview_snapshot(SLUG) or {}
        for k, v in ov.items():
            if snap.get(k) is None and v is not None:
                snap[k] = v

    # If still no daily series, emit one row for 'today' with snapshot-only fields
    if not daily:
        log("No daily series found; emitting snapshot-only row based on snapshot counters.")
        now_ts = int(datetime.now(timezone.utc).timestamp())
        daily = [{"date": now_ts, "volume": None, "openInterest": None}]

    rows = normalize_rows(daily, snap)
    write_csv(rows)
    upload_to_dune(OUT_CSV)

if __name__ == "__main__":
    main()