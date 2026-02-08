# scripts/build/build_dim_fighter.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd
import re


from ufcstats.snapshot import default_snapshot_utc, infer_snapshot_from_path


OUT_BASENAME = "dim_fighter__ufcstats__{snapshot}.csv"

def _normalize_ufcstats_url_series(s: pd.Series) -> pd.Series:
    """
    Canonicalize UFCStats URLs so joins don't fail due to scheme/www differences.
    Keeps path+id stable.
    """
    x = _clean_text_series(s)

    # normalize scheme+host variants to one form
    x = x.str.replace(r"^https?://www\.ufcstats\.com/", "http://ufcstats.com/", regex=True)
    x = x.str.replace(r"^https?://ufcstats\.com/", "http://ufcstats.com/", regex=True)

    # remove trailing slash
    x = x.str.replace(r"/+$", "", regex=True)
    return x



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
def _strip_newlines_df(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = (
                df[c]
                .astype(str)
                .str.replace("\r", " ", regex=False)
                .str.replace("\n", " ", regex=False)
            )
    return df


def _to_int64_bool(s: pd.Series) -> pd.Series:
    # expects boolean series, returns Int64 0/1/NA
    out = s.where(~s.isna(), pd.NA)
    return out.astype("Int64")


def _build_flags_from_fact_bout(fact_bout: pd.DataFrame) -> pd.DataFrame:
    required = {"is_ufc", "weight_class_raw", "fighter_1_url", "fighter_2_url"}
    missing = required - set(fact_bout.columns)
    if missing:
        raise ValueError(f"fact_bout missing required columns: {sorted(missing)}")

    # normalize is_ufc into boolean
    is_ufc_b = (
        fact_bout["is_ufc"]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(["1", "true", "t", "yes", "y"])
    )

    # women’s divisions contain "Women" in UFCStats weight class strings
    is_women_b = (
        fact_bout["weight_class_raw"]
        .astype(str)
        .str.contains("women", case=False, na=False)
    )

    f1 = pd.DataFrame(
        {
            "fighter_url": _normalize_ufcstats_url_series(fact_bout["fighter_1_url"]),
            "is_ufc_bout": is_ufc_b,
            "is_female_bout": is_women_b,
        }
    )
    f2 = pd.DataFrame(
        {
            "fighter_url": _normalize_ufcstats_url_series(fact_bout["fighter_2_url"]),
            "is_ufc_bout": is_ufc_b,
            "is_female_bout": is_women_b,
        }
    )


    long_df = pd.concat([f1, f2], ignore_index=True)
    long_df = long_df[long_df["fighter_url"] != ""]

    g = long_df.groupby("fighter_url", as_index=False)

    # any UFC appearance
    is_ufc_fighter = g["is_ufc_bout"].any().rename(columns={"is_ufc_bout": "is_ufc_fighter"})

    # mode of women/non-women across bouts
    def _mode_bool(x: pd.Series):
        if x.empty:
            return pd.NA
        vc = x.value_counts(dropna=True)
        if vc.empty:
            return pd.NA
        return bool(vc.index[0])

    is_female = g["is_female_bout"].apply(_mode_bool).rename(columns={"is_female_bout": "is_female"})

    flags = is_ufc_fighter.merge(is_female, on="fighter_url", how="outer")
    flags["is_ufc_fighter"] = _to_int64_bool(flags["is_ufc_fighter"])
    flags["is_female"] = _to_int64_bool(flags["is_female"])

    return flags



def build_dim_fighter(
    raw_fighter_directory: Path,
    raw_fighter_details: Path,
    fact_bout_csv: Path,
    outdir: Path,
    snapshot: Optional[str] = None,
) -> Path:
    dir_df = _read_csv(raw_fighter_directory)
    det_df = _read_csv(raw_fighter_details)
    bout_df = _read_csv(fact_bout_csv)

    # Drop snapshot columns from raw inputs (snapshot is encoded in filenames)
    for df in (dir_df, det_df):
        if "snapshot" in df.columns:
            df.drop(columns=["snapshot"], inplace=True)

    for df in (dir_df, det_df):
        for col in ["snapshot", "char"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

    # Normalize fighter_url in both sources
    dir_df["fighter_url"] = _normalize_ufcstats_url_series(dir_df["fighter_url"])
    det_df["fighter_url"] = _normalize_ufcstats_url_series(det_df["fighter_url"])


    # --- validate minimum keys ---
    for name, df, col in [
        ("fighter_directory", dir_df, "fighter_url"),
        ("fighter_details", det_df, "fighter_url"),
    ]:
        if col not in df.columns:
            raise ValueError(f"{name} missing required column: {col}")

    # --- normalize keys ---
    dir_df["fighter_url"] = _clean_text_series(dir_df["fighter_url"])
    det_df["fighter_url"] = _clean_text_series(det_df["fighter_url"])

    # drop empty keys
    dir_df = dir_df[dir_df["fighter_url"] != ""].copy()
    det_df = det_df[det_df["fighter_url"] != ""].copy()

    # de-dupe defensively
    dir_df = dir_df.drop_duplicates(subset=["fighter_url"]).copy()
    det_df = det_df.drop_duplicates(subset=["fighter_url"]).copy()

    # --- join directory + details (THIS is the correct place to merge those two) ---
    dim = dir_df.merge(det_df, on="fighter_url", how="left", suffixes=("", "_details"))

    # If directory first/last missing, fill from details
    for col in ["first_name", "last_name"]:
        dcol = f"{col}_details"
        if col in dim.columns and dcol in dim.columns:
            dim[col] = _clean_text_series(dim[col])
            dim[dcol] = _clean_text_series(dim[dcol])
            dim[col] = dim[col].where(dim[col] != "", dim[dcol])


    # If you don't want to keep the *_details columns after backfill:
    drop_cols = [c for c in ["first_name_details", "last_name_details"] if c in dim.columns]
    if drop_cols:
        dim = dim.drop(columns=drop_cols)

    # --- add derived flags from fact_bout ---
    flags = _build_flags_from_fact_bout(bout_df)
    dim = dim.merge(flags, on="fighter_url", how="left")
    # default flags for fighters with no bouts in fact_bout
    for col in ["is_ufc_fighter", "is_female"]:
        if col not in dim.columns:
            dim[col] = pd.NA
        dim[col] = (
            dim[col]
            .replace("", pd.NA)
            .fillna(0)
            .astype("Int64")
        )

    # --- optional: stable column order ---
    preferred = [
        "fighter_url",
        "fighter_name",
        "first_name",
        "last_name",
        "nickname_raw",
        "height_raw",
        "weight_raw",
        "reach_raw",
        "stance_raw",
        "dob_raw",
        "w_raw",
        "l_raw",
        "d_raw",
        "belt_raw",
        "slpm",
        "str_acc",
        "sapm",
        "str_def",
        "td_avg",
        "td_acc",
        "td_def",
        "sub_avg",
        "is_ufc_fighter",
        "is_female",
    ]
    cols = [c for c in preferred if c in dim.columns] + [c for c in dim.columns if c not in preferred]
    dim = dim[cols].copy()
    dim = _strip_newlines_df(dim)
    snap = _infer_snapshot(raw_fighter_directory, raw_fighter_details, fact_bout_csv, explicit=snapshot)
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / OUT_BASENAME.format(snapshot=snap)

    dim.to_csv(outpath, index=False, encoding="utf-8", lineterminator="\n")

    return outpath


def main() -> int:
    p = argparse.ArgumentParser(description="Build dim_fighter (directory + details) and add is_ufc_fighter/is_female from fact_bout.")
    p.add_argument("--fighter-directory", required=True, help="Path to data/raw/fighter_directory__ufcstats__YYYY-MM-DD.csv")
    p.add_argument("--fighter-details", required=True, help="Path to data/raw/fighter_details__ufcstats__YYYY-MM-DD.csv")
    p.add_argument("--fact-bout", required=True, help="Path to data/processed/fact_bout__ufcstats__YYYY-MM-DD.csv")
    p.add_argument("--outdir", default="data/processed", help="Output directory (default: data/processed)")
    p.add_argument("--snapshot", default=None, help="Snapshot YYYY-MM-DD (default inferred)")
    args = p.parse_args()

    outpath = build_dim_fighter(
        raw_fighter_directory=Path(args.fighter_directory),
        raw_fighter_details=Path(args.fighter_details),
        fact_bout_csv=Path(args.fact_bout),
        outdir=Path(args.outdir),
        snapshot=args.snapshot,
    )
    print(f"Wrote: {outpath}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
