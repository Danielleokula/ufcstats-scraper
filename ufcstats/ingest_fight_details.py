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

# You will create this next:
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
    sleep_s: float = 0.25,
    timeout: int = 30,
    retries: int = 3,
) -> Path:
    base_rows = read_event_details_csv(input_event_details)

    # de-dupe fight urls
    fight_urls: List[str] = []
    seen: Set[str] = set()
    for r in base_rows:
        u = (r.get("fight_url") or "").strip()
        if not u:
            continue
        if u not in seen:
            seen.add(u)
            fight_urls.append(u)

    outpath = outdir / RAW_BASENAME.format(snapshot=snapshot)

    session = requests.Session()
    base = pick_base_url(session)

    all_rows: List[Dict[str, str]] = []

    for i, fight_url in enumerate(fight_urls, start=1):
        norm_url = normalize_ufcstats_url(fight_url, base)

        try:
            html = fetch_html(session, norm_url, timeout=timeout, retries=retries)
            # parse_fight_details should return either:
            #   - one dict row (fight-level totals)
            #   - OR multiple rows (e.g., totals + significant strikes totals)
            parsed = parse_fight_details(html=html, fight_url=norm_url, snapshot=snapshot)

            if isinstance(parsed, dict):
                all_rows.append(parsed)
            else:
                all_rows.extend(parsed)

        except Exception as e:
            print(f"[WARN] Failed fight {i}/{len(fight_urls)}: {norm_url} :: {e}", file=sys.stderr)

        time.sleep(sleep_s)

    if all_rows:
        write_csv(all_rows, outpath)
        print(f"Wrote {len(all_rows)} fight rows → {outpath}")
    else:
        outpath.write_text("", encoding="utf-8")
        print(f"Wrote 0 fight rows → {outpath}")

    return outpath


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ingest UFCStats fight-details pages (fight-level).")
    p.add_argument(
        "--input",
        required=True,
        help="Path to event_details__ufcstats__{snapshot}.csv (must contain fight_url column).",
    )
    p.add_argument("--outdir", default=str(Path("data") / "raw"), help="Output directory (default: data/raw).")
    p.add_argument("--snapshot", default="", help="Snapshot date YYYY-MM-DD (default inferred or UTC today).")
    p.add_argument("--sleep", default="0.25", help="Delay between requests (seconds).")
    p.add_argument("--timeout", default="30", help="Request timeout (seconds).")
    p.add_argument("--retries", default="3", help="Retries per request.")
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
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
