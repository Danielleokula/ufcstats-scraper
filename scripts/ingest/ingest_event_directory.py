# scripts/ingest/ingest_event_directory.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests

from ufcstats.net import fetch_html, pick_base_url, normalize_ufcstats_url
from ufcstats.parse_event_directory import parse_event_directory
from ufcstats.snapshot import default_snapshot_utc


DEFAULT_EVENTS_PATH = "/statistics/events/completed?page=all"


def _project_root() -> Path:
    """
    Assumes this file lives at: <repo>/scripts/ingest/ingest_event_directory.py
    """
    return Path(__file__).resolve().parents[2]


def _default_output_path(snapshot: str) -> Path:
    return _project_root() / "data" / "raw" / f"event_directory__ufcstats__{snapshot}.csv"


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _resolve_events_url(base_url: str) -> str:
    # base_url comes from pick_base_url(session) and is either http or https.
    # Normalize the directory URL onto that base.
    raw_url = f"{base_url}{DEFAULT_EVENTS_PATH}"
    return normalize_ufcstats_url(raw_url, base=base_url)


def ingest_event_directory(
    snapshot: str,
    out_path: Path,
    timeout: int = 30,
    retries: int = 3,
) -> Tuple[Path, int]:
    """
    Fetch UFCStats completed events directory and write a raw snapshot CSV.

    Returns: (written_path, n_rows)
    """
    session = requests.Session()
    base_url = pick_base_url(session)

    events_url = _resolve_events_url(base_url)

    html = fetch_html(session, events_url, timeout=timeout, retries=retries)

    rows = parse_event_directory(html, base_url=base_url)
    if not rows:
        raise RuntimeError(
            "Parsed 0 events. UFCStats layout may have changed, or request returned unexpected HTML."
        )

    df = pd.DataFrame(rows)

    # Minimal sanity: enforce expected columns
    expected_cols = ["event_url", "event_name", "event_date_raw", "event_location_raw"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Parser output missing expected columns: {missing}. Got: {list(df.columns)}")

    # De-dupe conservatively by event_url
    df = df.drop_duplicates(subset=["event_url"]).reset_index(drop=True)

    _ensure_parent_dir(out_path)
    df.to_csv(out_path, index=False)

    return out_path, int(df.shape[0])


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest UFCStats completed events directory into a raw snapshot CSV."
    )
    parser.add_argument(
        "--snapshot",
        help="Snapshot date in YYYY-MM-DD. Defaults to current UTC date.",
        default=None,
    )
    parser.add_argument(
        "--out",
        help="Output CSV path. Default: data/raw/event_directory__ufcstats__<snapshot>.csv",
        default=None,
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=3)

    args = parser.parse_args(argv)

    snapshot = args.snapshot or default_snapshot_utc()
    out_path = Path(args.out) if args.out else _default_output_path(snapshot)

    written, n = ingest_event_directory(
        snapshot=snapshot,
        out_path=out_path,
        timeout=args.timeout,
        retries=args.retries,
    )

    print(f"Wrote {n:,} events → {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
