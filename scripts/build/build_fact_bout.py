# scripts/build/build_fact_bout.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

from ufcstats.snapshot import default_snapshot_utc, infer_snapshot_from_path


RAW_DETAILS_BASENAME = "event_details__ufcstats__{snapshot}.csv"
DIM_EVENT_BASENAME = "dim_event__ufcstats__{snapshot}.csv"
OUT_BASENAME = "fact_bout__ufcstats__{snapshot}.csv"


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


def _to_int_series(s: pd.Series) -> pd.Series:
    # keeps blanks as <NA>
    s = _clean_text_series(s)
    return pd.to_numeric(s.replace({"": pd.NA, "--": pd.NA}), errors="coerce").astype("Int64")


def _infer_is_ufc_from_name(event_name: pd.Series) -> pd.Series:
    name = _clean_text_series(event_name).str.upper()
    return (name.str.startswith("UFC ")) | (name.str.contains("UFC FIGHT NIGHT"))


def build_fact_bout(
    raw_event_details: Path,
    dim_event: Optional[Path],
    outdir: Path,
    snapshot: Optional[str],
) -> Path:
    snap = _infer_snapshot(raw_event_details, *( [dim_event] if dim_event else [] ), explicit=snapshot)

    df = _read_csv(raw_event_details)

    # Normalize expected columns
    required = [
        "snapshot",
        "event_url",
        "bout_url",
        "bout_order",
        "fighter_1_url",
        "fighter_2_url",
        "fighter_1_result",
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
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"raw_event_details missing columns: {missing}")

    # Clean key strings
    df["bout_url"] = _clean_text_series(df["bout_url"])
    df["event_url"] = _clean_text_series(df["event_url"])
    df["fighter_1_url"] = _clean_text_series(df["fighter_1_url"])
    df["fighter_2_url"] = _clean_text_series(df["fighter_2_url"])

    # Drop malformed rows (no bout_url)
    df = df[df["bout_url"] != ""]

    # De-dupe by bout_url (should already be unique)
    df = df.drop_duplicates(subset=["bout_url"], keep="first")

    # Type numeric fight stats (optional but useful downstream)
    df["bout_order"] = _to_int_series(df["bout_order"])
    df["kd_1"] = _to_int_series(df["kd_1"])
    df["str_1"] = _to_int_series(df["str_1"])
    df["td_1"] = _to_int_series(df["td_1"])
    df["sub_1"] = _to_int_series(df["sub_1"])
    df["kd_2"] = _to_int_series(df["kd_2"])
    df["str_2"] = _to_int_series(df["str_2"])
    df["td_2"] = _to_int_series(df["td_2"])
    df["sub_2"] = _to_int_series(df["sub_2"])
    df["round_raw"] = _clean_text_series(df["round_raw"])
    df["time_raw"] = _clean_text_series(df["time_raw"])

    # Winner flag (mirror result field)
    r1 = _clean_text_series(df["fighter_1_result"]).str.lower()
    r2 = _clean_text_series(df["fighter_2_result"]).str.lower()
    df["has_winner"] = (r1 == "win") | (r2 == "win")

    # is_ufc: prefer dim_event if provided, else infer from event_name
    if dim_event and Path(dim_event).exists():
        de = _read_csv(Path(dim_event))
        if "event_url" not in de.columns:
            raise ValueError("dim_event missing event_url")
        if "is_ufc" not in de.columns:
            raise ValueError("dim_event missing is_ufc")

        de = de[["event_url", "is_ufc"]].copy()
        de["event_url"] = _clean_text_series(de["event_url"])
        de["is_ufc"] = de["is_ufc"].astype(str)

        df = df.merge(de, on="event_url", how="left")
        # if null/blank, fall back to inference from event_name if present
        if "event_name" in df.columns:
            fallback = _infer_is_ufc_from_name(df["event_name"])
            df.loc[df["is_ufc"].isin(["", "nan", "None"]), "is_ufc"] = fallback.astype(str)
    else:
        if "event_name" in df.columns:
            df["is_ufc"] = _infer_is_ufc_from_name(df["event_name"]).astype(str)
        else:
            df["is_ufc"] = ""

    # Standard column order (lean fact table)
    cols = [
        "snapshot",
        "bout_url",
        "event_url",
        "bout_order",
        "fighter_1_url",
        "fighter_2_url",
        "fighter_1_result",
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
        "has_winner",
        "is_ufc",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]

    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / OUT_BASENAME.format(snapshot=snap)
    df.to_csv(outpath, index=False)
    return outpath


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build fact_bout from UFCStats raw event_details snapshot.")
    p.add_argument("--event-details", required=True, help="Path to data/raw/event_details__ufcstats__YYYY-MM-DD.csv")
    p.add_argument("--dim-event", default=None, help="Optional path to data/processed/dim_event__ufcstats__YYYY-MM-DD.csv")
    p.add_argument("--outdir", default="data/processed", help="Output directory (default: data/processed)")
    p.add_argument("--snapshot", default=None, help="Snapshot YYYY-MM-DD (default inferred from inputs, else UTC today)")
    return p


def main() -> int:
    args = build_argparser().parse_args()
    outpath = build_fact_bout(
        raw_event_details=Path(args.event_details),
        dim_event=Path(args.dim_event) if args.dim_event else None,
        outdir=Path(args.outdir),
        snapshot=args.snapshot,
    )
    print(f"Wrote: {outpath}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
