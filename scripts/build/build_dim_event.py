# scripts/build/build_dim_event.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

from ufcstats.snapshot import default_snapshot_utc, infer_snapshot_from_path


OUT_BASENAME = "dim_event__ufcstats__{snapshot}.csv"


def _infer_snapshot(*paths: Path, explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    for p in paths:
        s = infer_snapshot_from_path(str(p))
        if s:
            return s
    return default_snapshot_utc()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _clean_text_series(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def build_dim_event(
    event_directory_csv: Path,
    outdir: Path,
    snapshot: Optional[str] = None,
) -> Path:
    snap = _infer_snapshot(event_directory_csv, explicit=snapshot)

    df = _read_csv(event_directory_csv)

    # normalize text fields
    df["event_url"] = _clean_text_series(df.get("event_url", ""))
    df["event_name"] = _clean_text_series(df.get("event_name", ""))
    df["event_date_raw"] = _clean_text_series(df.get("event_date_raw", ""))
    df["event_location_raw"] = _clean_text_series(df.get("event_location_raw", ""))

    # 1) Drop empty/malformed events
    df = df[df["event_name"] != ""]
    df = df[df["event_url"] != ""]

    # 2) is_ufc (project definition)
    name_u = df["event_name"].str.upper()
    df["is_ufc"] = name_u.str.startswith("UFC ") | name_u.str.contains("UFC FIGHT NIGHT", regex=False)

    # 3) Dedup on stable key
    df = df.drop_duplicates(subset=["event_url"], keep="first")

    # output schema
    df_out = df[
        [
            "event_url",
            "event_name",
            "event_date_raw",
            "event_location_raw",
            "is_ufc",
        ]
    ].copy()

    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / OUT_BASENAME.format(snapshot=snap)
    df_out.to_csv(outpath, index=False)
    return outpath


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build dim_event from UFCStats raw event_directory snapshot.")
    p.add_argument(
        "--event-directory",
        required=True,
        help="Path to data/raw/event_directory__ufcstats__YYYY-MM-DD.csv",
    )
    p.add_argument("--outdir", default="data/processed", help="Output directory (default: data/processed)")
    p.add_argument(
        "--snapshot",
        default=None,
        help="Snapshot YYYY-MM-DD (default inferred from input filename, else UTC today)",
    )
    return p


def main() -> int:
    args = build_argparser().parse_args()
    outpath = build_dim_event(
        event_directory_csv=Path(args.event_directory),
        outdir=Path(args.outdir),
        snapshot=args.snapshot,
    )
    print(f"Wrote: {outpath}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
