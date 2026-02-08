import re
import datetime as dt
from pathlib import Path

DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")

def default_snapshot_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

def infer_snapshot_from_path(path: str) -> str | None:
    name = Path(path).name
    m = DATE_RE.search(name)
    return m.group(1) if m else None
