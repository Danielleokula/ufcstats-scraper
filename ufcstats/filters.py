# ufcstats/filters.py
from __future__ import annotations

import pandas as pd


def filter_has_bouts(df: pd.DataFrame, bout_col: str = "bout_count") -> pd.DataFrame:
    """
    Keep only fighters with at least 1 recorded bout on UFCStats.

    - Treat missing/blank bout_count as 0.
    - Returns a copy (safe for chaining).
    """
    if bout_col not in df.columns:
        raise ValueError(f"Missing required column: '{bout_col}'")

    bouts = pd.to_numeric(df[bout_col], errors="coerce").fillna(0).astype(int)
    return df.loc[bouts > 0].copy()


def report_excluded_no_bouts(
    df: pd.DataFrame,
    bout_col: str = "bout_count",
    name_cols: tuple[str, str] = ("first_name", "last_name"),
    url_col: str = "profile_url",
    max_print: int = 50,
) -> pd.DataFrame:
    """
    Return the excluded (bout_count == 0) rows and optionally print a preview.

    Useful for sanity checking directory-only profiles (e.g., Mike Zichelle-like cases).
    """
    if bout_col not in df.columns:
        raise ValueError(f"Missing required column: '{bout_col}'")

    bouts = pd.to_numeric(df[bout_col], errors="coerce").fillna(0).astype(int)
    excluded = df.loc[bouts <= 0].copy()

    if len(excluded) > 0 and max_print > 0:
        print("=== Excluded: bout_count == 0 (directory-only / no UFCStats bout log) ===")
        cols = [c for c in [*name_cols, url_col, bout_col] if c in excluded.columns]
        preview = excluded[cols].head(max_print)

        for _, r in preview.iterrows():
            first = str(r.get(name_cols[0], "")).strip()
            last = str(r.get(name_cols[1], "")).strip()
            url = str(r.get(url_col, "")).strip()
            bc = r.get(bout_col, "")
            name = f"{first} {last}".strip() or "(no name)"
            print(f"- {name} | bouts={bc} | {url}")

        if len(excluded) > max_print:
            print(f"... plus {len(excluded) - max_print} more")

        print(f"Total excluded (bout_count==0): {len(excluded)}")
        print()

    return excluded
