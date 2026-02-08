# ufcstats/parse_event_directory.py
from __future__ import annotations

from typing import Dict, List

from bs4 import BeautifulSoup

from ufcstats.net import normalize_ufcstats_url


def clean_text(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()


def parse_event_directory_html(html: str, base_url: str) -> List[Dict[str, str]]:
    """
    Parse UFCStats Events directory page (Completed/Upcoming list).

    Expected source URL:
      /statistics/events/completed?page=all

    Returns list of dicts with:
      - event_url
      - event_name
      - event_date_raw
      - event_location_raw
    """
    soup = BeautifulSoup(html, "lxml")
    return parse_event_directory_soup(soup, base_url=base_url)


def parse_event_directory_soup(soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    table = soup.select_one("table.b-statistics__table-events") or soup.select_one("table")
    if not table:
        return rows

    for tr in table.select("tbody tr"):
        tds = tr.select("td")
        if len(tds) < 2:
            continue

        a = tds[0].select_one('a[href*="event-details"]')
        event_name = clean_text(a.get_text(" ", strip=True)) if a else ""

        date_el = tds[0].select_one("span.b-statistics__date")
        event_date_raw = clean_text(date_el.get_text(" ", strip=True)) if date_el else ""

        event_location_raw = clean_text(tds[1].get_text(" ", strip=True))
        event_url = normalize_ufcstats_url(a.get("href", ""), base=base_url)

        if not event_url or not event_name:
            continue

        rows.append(
            {
                "event_url": event_url,
                "event_name": event_name,
                "event_date_raw": event_date_raw,
                "event_location_raw": event_location_raw,
            }
        )

    return rows


def parse_event_directory(html: str, base_url: str) -> List[Dict[str, str]]:
    """Public entrypoint used by ingestion scripts."""
    return parse_event_directory_html(html, base_url=base_url)
