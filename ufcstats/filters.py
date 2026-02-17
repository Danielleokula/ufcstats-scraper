# ufcstats/filters.py
from __future__ import annotations

import pandas as pd


def filter_has_fights(df: pd.DataFrame, fight_col: str = "fight_count") -> pd.DataFrame:
    """
    Keep only fighters with at least 1 recorded fight on UFCStats.

    - Treat missing/blank fight_count as 0.
    - Returns a copy (safe for chaining).
    """
    if fight_col not in df.columns:
        raise ValueError(f"Missing required column: '{fight_col}'")

    fights = pd.to_numeric(df[fight_col], errors="coerce").fillna(0).astype(int)
    return df.loc[fights > 0].copy()


def report_excluded_no_fights(
    df: pd.DataFrame,
    fight_col: str = "fight_count",
    name_cols: tuple[str, str] = ("first_name", "last_name"),
    url_col: str = "profile_url",
    max_print: int = 50,
) -> pd.DataFrame:
    """
    Return the excluded (fight_count == 0) rows and optionally print a preview.

    Useful for sanity checking directory-only profiles (e.g., Mike Zichelle-like cases).
    """
    if fight_col not in df.columns:
        raise ValueError(f"Missing required column: '{fight_col}'")

    fights = pd.to_numeric(df[fight_col], errors="coerce").fillna(0).astype(int)
    excluded = df.loc[fights <= 0].copy()

    if len(excluded) > 0 and max_print > 0:
        print("=== Excluded: fight_count == 0 (directory-only / no UFCStats fight log) ===")
        cols = [c for c in [*name_cols, url_col, fight_col] if c in excluded.columns]
        preview = excluded[cols].head(max_print)

        for _, r in preview.iterrows():
            first = str(r.get(name_cols[0], "")).strip()
            last = str(r.get(name_cols[1], "")).strip()
            url = str(r.get(url_col, "")).strip()
            bc = r.get(fight_col, "")
            name = f"{first} {last}".strip() or "(no name)"
            print(f"- {name} | fights={bc} | {url}")

        if len(excluded) > max_print:
            print(f"... plus {len(excluded) - max_print} more")

        print(f"Total excluded (fight_count==0): {len(excluded)}")
        print()

    return excluded
