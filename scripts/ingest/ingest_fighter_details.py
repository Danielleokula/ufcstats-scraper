# scripts/ingest/ingest_fighter_details.py
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Dict, List, Set

import requests

from ufcstats.net import fetch_html, normalize_ufcstats_url, pick_base_url
from ufcstats.parse_fighter_details import parse_fighter_details
from ufcstats.snapshot import default_snapshot_utc, infer_snapshot_from_path


RAW_BASENAME = "fighter_details__ufcstats__{snapshot}.csv"


def read_fighter_directory_csv(path: Path) -> List[Dict[str, str]]:
    """
    Requires at least:
      - fighter_url (or profile_url if legacy)

    Optional pass-through (debug only):
      - first_name, last_name
    """
    if not path.exists():
        raise FileNotFoundError(f"Fighter directory CSV not found: {path}")

    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            fighter_url = (row.get("fighter_url") or row.get("profile_url") or "").strip()
            if not fighter_url:
                continue
            rows.append(row)
    return rows


def ensure_outdir(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def ingest_fighter_details(
    input_fighter_directory: Path,
    outdir: Path,
    snapshot: str | None,
    sleep_s: float,
    limit: int | None,
    timeout: int,
    retries: int,
) -> Path:
    """
    Reads fighter_directory snapshot, fetches each fighter-details page,
    and writes ONE ROW PER FIGHTER with ONLY:
      - snapshot, fighter_url
      - first_name, last_name (debug)
      - dob_raw + career stats
    """
    inferred = infer_snapshot_from_path(str(input_fighter_directory))
    snap = snapshot or inferred or default_snapshot_utc()

    ensure_outdir(outdir)
    outpath = outdir / RAW_BASENAME.format(snapshot=snap)

    base_rows = read_fighter_directory_csv(input_fighter_directory)
    if limit is not None and limit > 0:
        base_rows = base_rows[:limit]

    if not base_rows:
        raise RuntimeError("No fighters found in fighter directory CSV (missing fighter_url/profile_url?).")

    session = requests.Session()
    base_url = pick_base_url(session)

    out_rows: List[Dict[str, str]] = []
    seen: Set[str] = set()

    n = len(base_rows)
    for i, r in enumerate(base_rows, start=1):
        raw_url = (r.get("fighter_url") or r.get("profile_url") or "").strip()
        fighter_url = normalize_ufcstats_url(raw_url, base=base_url)
        if not fighter_url:
            continue

        # De-dupe by fighter_url
        if fighter_url in seen:
            continue
        seen.add(fighter_url)

        try:
            html = fetch_html(session, fighter_url, timeout=timeout, retries=retries)
        except Exception as e:
            print(f"[{i}/{n}] FAIL fetch {fighter_url}: {e}", file=sys.stderr)
            continue

        try:
            parsed = parse_fighter_details(html)  # dict: dob_raw + stats only
        except Exception as e:
            print(f"[{i}/{n}] FAIL parse {fighter_url}: {e}", file=sys.stderr)
            continue

        out_row = {
            "snapshot": snap,
            "fighter_url": fighter_url,
            "first_name": (r.get("first_name") or "").strip(),
            "last_name": (r.get("last_name") or "").strip(),
            "dob_raw": (parsed.get("dob_raw") or "").strip(),
            "slpm": (parsed.get("slpm") or "").strip(),
            "str_acc": (parsed.get("str_acc") or "").strip(),
            "sapm": (parsed.get("sapm") or "").strip(),
            "str_def": (parsed.get("str_def") or "").strip(),
            "td_avg": (parsed.get("td_avg") or "").strip(),
            "td_acc": (parsed.get("td_acc") or "").strip(),
            "td_def": (parsed.get("td_def") or "").strip(),
            "sub_avg": (parsed.get("sub_avg") or "").strip(),
        }

        out_rows.append(out_row)
        print(f"[{i}/{n}] fighter_details: {fighter_url}")

        if sleep_s > 0:
            time.sleep(sleep_s)

    if not out_rows:
        raise RuntimeError("No fighter details parsed. Check reachability / selectors.")

    fieldnames = [
        "snapshot",
        "fighter_url",
        "first_name",
        "last_name",
        "dob_raw",
        "slpm",
        "str_acc",
        "sapm",
        "str_def",
        "td_avg",
        "td_acc",
        "td_def",
        "sub_avg",
    ]

    write_csv(outpath, out_rows, fieldnames)
    return outpath


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ingest UFCStats fighter-details pages into a raw fighter_details snapshot (dob + career stats only)."
    )
    p.add_argument("--input", required=True, help="Path to data/raw/fighter_directory__ufcstats__YYYY-MM-DD.csv")
    p.add_argument("--outdir", default="data/raw", help="Output directory (default: data/raw)")
    p.add_argument("--snapshot", default=None, help="Snapshot YYYY-MM-DD (default inferred from input, else UTC today)")
    p.add_argument("--sleep", type=float, default=0.35, help="Seconds to sleep between requests (default: 0.35)")
    p.add_argument("--limit", type=int, default=None, help="Optional cap for testing")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--retries", type=int, default=3)
    return p


def main() -> int:
    args = build_argparser().parse_args()
    outpath = ingest_fighter_details(
        input_fighter_directory=Path(args.input),
        outdir=Path(args.outdir),
        snapshot=args.snapshot,
        sleep_s=float(args.sleep),
        limit=args.limit,
        timeout=int(args.timeout),
        retries=int(args.retries),
    )
    print(f"\nWrote: {outpath}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
