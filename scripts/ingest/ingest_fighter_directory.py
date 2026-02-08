# scripts/ingest/ingest_fighter_directory.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from ufcstats.net import fetch_html, pick_base_url
from ufcstats.parse_fighter_directory import parse_fighter_directory
from ufcstats.snapshot import default_snapshot_utc


# UFCStats fighters directory requires char partitioning.
DEFAULT_CHARS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

FIGHTERS_PATH_TMPL = "/statistics/fighters?char={char}&page=all"


def _project_root() -> Path:
    # <repo>/scripts/ingest/ingest_fighter_directory.py -> parents[2] == <repo>
    return Path(__file__).resolve().parents[2]


def _default_output_path(snapshot: str) -> Path:
    return _project_root() / "data" / "raw" / f"fighter_directory__ufcstats__{snapshot}.csv"


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ingest_fighter_directory(
    snapshot: str,
    out_path: Path,
    chars: List[str] | None = None,
    timeout: int = 30,
    retries: int = 3,
) -> Tuple[Path, int]:
    """
    Fetch UFCStats fighters directory for each char and write a combined raw snapshot CSV.

    Output columns:
      snapshot, char,
      fighter_url, fighter_name, first_name, last_name, nickname_raw,
      height_raw, weight_raw, reach_raw, stance_raw, w_raw, l_raw, d_raw, belt_raw
    """
    chars = chars or DEFAULT_CHARS

    session = requests.Session()
    base_url = pick_base_url(session)

    all_rows: List[Dict[str, str]] = []

    for c in chars:
        url = f"{base_url}{FIGHTERS_PATH_TMPL.format(char=c)}"
        html = fetch_html(session, url, timeout=timeout, retries=retries)

        rows = parse_fighter_directory(html, base_url=base_url)
        if not rows:
            # Not fatal: some letters may be empty depending on UFCStats behavior.
            continue

        # Add traceability columns
        for r in rows:
            r["snapshot"] = snapshot
            r["char"] = c

        all_rows.extend(rows)

    if not all_rows:
        raise RuntimeError("Parsed 0 fighters across all chars. UFCStats layout may have changed.")

    df = pd.DataFrame(all_rows)

    # Minimal schema enforcement: ensure parser-provided columns exist
    expected = [
        "fighter_url",
        "fighter_name",
        "first_name",
        "last_name",
        "nickname_raw",
        "height_raw",
        "weight_raw",
        "reach_raw",
        "stance_raw",
        "w_raw",
        "l_raw",
        "d_raw",
        "belt_raw",
    ]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise RuntimeError(f"Parser output missing expected columns: {missing}. Got: {list(df.columns)}")

    # De-dupe conservatively by fighter_url (stable ID)
    df = df.drop_duplicates(subset=["fighter_url"]).reset_index(drop=True)

    # Order columns (clean diffs)
    ordered_cols = ["snapshot", "char"] + expected
    df = df[ordered_cols]

    _ensure_parent_dir(out_path)
    df.to_csv(out_path, index=False)

    return out_path, int(df.shape[0])


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Ingest UFCStats fighter directory into a raw snapshot CSV.")
    p.add_argument("--snapshot", default=None, help="Snapshot date YYYY-MM-DD (default: UTC today).")
    p.add_argument("--out", default=None, help="Output CSV path (default: data/raw/fighter_directory__ufcstats__<snapshot>.csv).")
    p.add_argument("--chars", default=None, help='Optional override, e.g. "ABC" or "A,B,C". Default A-Z.')
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--retries", type=int, default=3)

    args = p.parse_args(argv)

    snapshot = args.snapshot or default_snapshot_utc()
    out_path = Path(args.out) if args.out else _default_output_path(snapshot)

    if args.chars:
        raw = args.chars.strip()
        chars = raw.split(",") if "," in raw else list(raw)
        chars = [c.strip().upper() for c in chars if c.strip()]
    else:
        chars = None

    written, n = ingest_fighter_directory(
        snapshot=snapshot,
        out_path=out_path,
        chars=chars,
        timeout=args.timeout,
        retries=args.retries,
    )

    print(f"Wrote {n:,} fighters → {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
