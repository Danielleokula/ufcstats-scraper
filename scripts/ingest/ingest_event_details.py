# scripts/ingest/ingest_event_details.py
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Dict, List, Set

import requests

from ufcstats.net import fetch_html, normalize_ufcstats_url, pick_base_url
from ufcstats.parse_event_details import parse_event_details
from ufcstats.snapshot import default_snapshot_utc, infer_snapshot_from_path

RAW_BASENAME = "event_details__ufcstats__{snapshot}.csv"


def read_event_directory_csv(path: Path) -> List[Dict[str, str]]:
    """
    Expect columns like:
      event_url, event_name, event_date_raw, event_location_raw
    """
    if not path.exists():
        raise FileNotFoundError(f"Event directory CSV not found: {path}")

    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            event_url = (row.get("event_url") or "").strip()
            if not event_url:
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


def ingest_event_details(
    input_event_directory: Path,
    outdir: Path,
    snapshot: str | None,
    sleep_s: float,
    limit: int | None,
    timeout: int,
    retries: int,
) -> Path:
    """
    Reads an event directory snapshot and scrapes each event-details page into a raw event_details snapshot.

    Output is ONE ROW PER BOUT, mirroring the event-details table.
    """
    inferred = infer_snapshot_from_path(str(input_event_directory))
    snap = snapshot or inferred or default_snapshot_utc()

    ensure_outdir(outdir)
    outpath = outdir / RAW_BASENAME.format(snapshot=snap)

    event_rows = read_event_directory_csv(input_event_directory)
    if limit is not None and limit > 0:
        event_rows = event_rows[:limit]

    if not event_rows:
        raise RuntimeError("No event rows found in event directory CSV (missing event_url?).")

    session = requests.Session()
    base_url = pick_base_url(session)

    all_bouts: List[Dict[str, str]] = []
    seen_keys: Set[str] = set()

    n = len(event_rows)
    for i, ev in enumerate(event_rows, start=1):
        raw_event_url = (ev.get("event_url") or "").strip()
        event_url = normalize_ufcstats_url(raw_event_url, base_url)

        try:
            html = fetch_html(session, event_url, timeout=timeout, retries=retries)
        except Exception as e:
            print(f"[{i}/{n}] FAIL fetch {event_url}: {e}", file=sys.stderr)
            continue

        try:
            event_meta, bouts = parse_event_details(html, base_url=base_url)
        except Exception as e:
            print(f"[{i}/{n}] FAIL parse {event_url}: {e}", file=sys.stderr)
            continue

        # Inject event context onto each bout row
        for br in bouts:
            br["snapshot"] = snap
            br["event_url"] = event_url
            br["event_name"] = (event_meta.get("event_name") or ev.get("event_name") or "").strip()
            br["event_date_raw"] = (event_meta.get("event_date_raw") or ev.get("event_date_raw") or "").strip()
            br["event_location_raw"] = (event_meta.get("event_location_raw") or ev.get("event_location_raw") or "").strip()

            # Dedup key: prefer bout_url if present; else event_url + bout_order
            bout_url = (br.get("bout_url") or "").strip()
            bout_order = (br.get("bout_order") or "").strip()
            key = bout_url if bout_url else f"{event_url}__{bout_order}"

            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            all_bouts.append(br)

        event_label = (event_meta.get("event_name") or ev.get("event_name") or "event").strip()
        print(f"[{i}/{n}] {event_label} -> bouts: {len(bouts)}")

        if sleep_s > 0:
            time.sleep(sleep_s)

    if not all_bouts:
        raise RuntimeError("No bouts parsed. Check site reachability and parser selectors.")

    # Stable schema for the raw snapshot (mirror of event-details table)
    fieldnames = [
        "snapshot",
        "event_url",
        "event_name",
        "event_date_raw",
        "event_location_raw",
        "bout_url",
        "bout_order",
        "fighter_1_name",
        "fighter_1_url",
        "fighter_1_result",
        "fighter_2_name",
        "fighter_2_url",
        "fighter_2_result",
        "kd_1",
        "str_1",
        "td_1",
        "sub_1",
        "kd_2",
        "str_2",
        "td_2",
        "sub_2",
        "weight_class_raw",
        "method_raw",
        "round_raw",
        "time_raw",
    ]

    write_csv(outpath, all_bouts, fieldnames=fieldnames)
    return outpath


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ingest UFCStats event-details pages into a raw event_details snapshot (one row per bout)."
    )
    p.add_argument(
        "--input",
        required=True,
        help="Path to data/raw/event_directory__ufcstats__YYYY-MM-DD.csv",
    )
    p.add_argument(
        "--outdir",
        default="data/raw",
        help="Output directory for raw snapshots (default: data/raw)",
    )
    p.add_argument(
        "--snapshot",
        default=None,
        help="Snapshot date YYYY-MM-DD. If omitted, inferred from --input filename, else UTC today.",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.35,
        help="Seconds to sleep between requests (default: 0.35)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of events to ingest (for testing).",
    )
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--retries", type=int, default=3)
    return p


def main() -> int:
    args = build_argparser().parse_args()

    outpath = ingest_event_details(
        input_event_directory=Path(args.input),
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
