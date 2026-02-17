# scripts/run_pipeline_raw.py
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from ufcstats.snapshot import default_snapshot_utc

RAW_DIR = Path("data") / "raw"


def _ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def _run(args: list[str]) -> None:
    print("\n$ " + " ".join(args))
    subprocess.run(args, check=True)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)

    _ensure_dirs()

    snap = default_snapshot_utc()
    print(f"Pipeline snapshot (UTC): {snap}")

    event_dir_csv = RAW_DIR / f"event_directory__ufcstats__{snap}.csv"
    event_details_csv = RAW_DIR / f"event_details__ufcstats__{snap}.csv"
    fighter_dir_csv = RAW_DIR / f"fighter_directory__ufcstats__{snap}.csv"
    fighter_details_csv = RAW_DIR / f"fighter_details__ufcstats__{snap}.csv"
    fight_details_csv = RAW_DIR / f"fight_details__ufcstats__{snap}.csv"

    py = sys.executable

    # 1) Event directory
    _run([py, "-m", "scripts.ingest.ingest_event_directory", "--out", str(event_dir_csv)])

    # 2) Fighter directory
    _run([py, "-m", "scripts.ingest.ingest_fighter_directory", "--out", str(fighter_dir_csv)])

    # 3) Event details (reads event directory)
    _run(
        [
            py,
            "-m",
            "scripts.ingest.ingest_event_details",
            "--input",
            str(event_dir_csv),
            "--outdir",
            str(RAW_DIR),
            "--snapshot",
            snap,
        ]
    )

    # 4) Fighter details (reads fighter directory)
    _run(
        [
            py,
            "-m",
            "scripts.ingest.ingest_fighter_details",
            "--input",
            str(fighter_dir_csv),
            "--outdir",
            str(RAW_DIR),
            "--snapshot",
            snap,
        ]
    )

    # 5) Fight details (reads event details; needs fight_url)
    fight_limit = os.getenv(" ")  # optional
    args = [
        py,
        "-m",
        "scripts.ingest.ingest_fight_details",
        "--input",
        str(event_details_csv),
        "--outdir",
        str(RAW_DIR),
        "--snapshot",
        snap,
    ]

    #Optional test mode: UFCPIPE_LIMIT_FIGHTS=25
    if fight_limit:
        args += ["--limit", str(int(fight_limit))]

    # Optional resume mode: UFCPIPE_RESUME=1
    if os.getenv("UFCPIPE_RESUME"):
        args += ["--resume"]

    _run(args)


    print("\n=== Raw pipeline complete ===")
    print(f"Wrote: {event_dir_csv}")
    print(f"Wrote: {fighter_dir_csv}")
    print(f"Wrote: {event_details_csv}")
    print(f"Wrote: {fighter_details_csv}")
    print(f"Wrote: {fight_details_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
