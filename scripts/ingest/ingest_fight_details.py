# scripts/ingest/ingest_fight_details.py
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Dict, List, Set

import requests

from ufcstats.net import fetch_html, normalize_ufcstats_url, pick_base_url
from ufcstats.snapshot import default_snapshot_utc, infer_snapshot_from_path


from ufcstats.parse_fight_details import parse_fight_details


RAW_BASENAME = "fight_details__ufcstats__{snapshot}.csv"


def read_event_details_csv(path: Path) -> List[Dict[str, str]]:
    """
    Expect at least:
      fight_url
    Typically also:
      snapshot, event_url, fight_order, fighter_1_url, fighter_2_url, ...
    """
    if not path.exists():
        raise FileNotFoundError(f"Event details CSV not found: {path}")

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return []

    if "fight_url" not in rows[0]:
        raise ValueError(f"Expected column 'fight_url' in {path}, got: {list(rows[0].keys())}")

    return rows


def write_csv(rows: List[Dict[str, str]], outpath: Path) -> None:
    outpath.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # still write headerless file? prefer explicit header to avoid downstream surprises
        outpath.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with outpath.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)



def ingest_fight_details(
    input_event_details: Path,
    outdir: Path,
    snapshot: str,
    sleep_s: float = 1.0,
    timeout: int = 30,
    retries: int = 3,
    limit: int | None = None,
    resume: bool = False,
    ):

    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"fight_details__ufcstats__{snapshot}.csv"

    # 1) Read event_details and extract fight_url list (dedupe, keep order)
    import pandas as pd
    df = pd.read_csv(input_event_details)
    fight_urls = [u for u in df["fight_url"].dropna().astype(str).tolist()]
    if limit:
        fight_urls = fight_urls[:limit]
        print(f"[TEST MODE] Processing only {len(fight_urls)} fights")
    seen = set()
    fight_urls = [u for u in fight_urls if not (u in seen or seen.add(u))]

    if limit:
        fight_urls = fight_urls[:limit]

    # 2) Resume: load already-done fight_urls from existing output
    done = set()
    if resume and outpath.exists():
        try:
            done_df = pd.read_csv(outpath, usecols=["fight_url"])
            done = set(done_df["fight_url"].dropna().astype(str).tolist())
        except Exception:
            done = set()

    session = requests.Session()
    base = pick_base_url(session)

    # 3) CSV writer (append if exists)
    is_new = not outpath.exists()
    f = open(outpath, "a", newline="", encoding="utf-8")
    writer = None

    processed = 0
    try:
        for i, raw_url in enumerate(fight_urls, start=1):
            if raw_url in done:
                continue

            url = normalize_ufcstats_url(raw_url, base)

            # fetch
            try:
                html = fetch_html(session, url, timeout=timeout, retries=retries)
            except Exception as e:
                # backoff on failure
                time.sleep(max(2.0, sleep_s) * 3)
                continue

            # parse
            row = parse_fight_details(html=html, snapshot=snapshot, fight_url=url)

            # init writer once we know columns
            if writer is None:
                fieldnames = list(row.keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if is_new:
                    writer.writeheader()

            writer.writerow(row)
            f.flush()

            processed += 1
            if processed % 25 == 0:
                print(f"Processed {processed} fights… last={url}")

            time.sleep(sleep_s)

    finally:
        f.close()

    print(f"Wrote {processed} fights → {outpath}")
    return outpath



def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ingest UFCStats fight-details pages (fight-level).")
    p.add_argument(
        "--input",
        required=True,
        help="Path to event_details__ufcstats__{snapshot}.csv (must contain fight_url column).",
    )
    p.add_argument("--outdir", default=str(Path("data") / "raw"),
                   help="Output directory (default: data/raw).")
    p.add_argument("--snapshot", default="",
                   help="Snapshot date YYYY-MM-DD (default inferred or UTC today).")
    p.add_argument("--sleep", default="0.25",
                   help="Delay between requests (seconds).")
    p.add_argument("--timeout", default="30",
                   help="Request timeout (seconds).")
    p.add_argument("--retries", default="3",
                   help="Retries per request.")
    p.add_argument("--limit", type=int, default=None,
                   help="Process only the first N fights (testing mode).")
    p.add_argument("--resume", action="store_true",
                   help="Skip fights already written to output file.")
    return p


def main(argv: List[str] | None = None) -> int:
    args = build_argparser().parse_args(argv)

    input_path = Path(args.input)
    outdir = Path(args.outdir)

    snapshot = args.snapshot.strip()
    if not snapshot:
        inferred = infer_snapshot_from_path(input_path)
        snapshot = inferred or default_snapshot_utc()

    ingest_fight_details(
        input_event_details=input_path,
        outdir=outdir,
        snapshot=snapshot,
        sleep_s=float(args.sleep),
        timeout=int(args.timeout),
        retries=int(args.retries),
        limit=args.limit,
        resume=args.resume,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
