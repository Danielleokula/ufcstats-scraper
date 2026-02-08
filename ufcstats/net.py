# ufcstats/net.py
import time
import requests
from typing import Optional


def pick_base_url(session: requests.Session) -> str:
    """
    Determine a reachable UFCStats base URL.
    Tries HTTPS first, then HTTP.
    """
    for base in ("https://www.ufcstats.com", "http://ufcstats.com"):
        try:
            r = session.get(base, timeout=10)
            r.raise_for_status()
            return base
        except Exception:
            continue
    raise RuntimeError("Could not reach UFCStats over https or http.")


def normalize_ufcstats_url(url: str, base: str) -> str:
    """
    Normalize any UFCStats URL to the selected base host.
    """
    u = (url or "").strip()
    if not u:
        return u

    prefixes = (
        "https://ufcstats.com",
        "http://ufcstats.com",
        "https://www.ufcstats.com",
        "http://www.ufcstats.com",
    )
    for p in prefixes:
        if u.startswith(p):
            return base + u[len(p):]
    return u


def fetch_html(
    session: requests.Session,
    url: str,
    params: Optional[dict] = None,
    timeout: int = 30,
    retries: int = 3,
) -> str:
    """
    Fetch HTML with retry and backoff.
    Shared across all UFCStats ingestion scripts.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_err = e
            time.sleep(0.75 * attempt)

    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_err}")
