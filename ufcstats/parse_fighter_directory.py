# ufcstats/parse_fighter_directory.py
from __future__ import annotations

from typing import Dict, List
from bs4 import BeautifulSoup

from ufcstats.net import normalize_ufcstats_url


def clean_text(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()


def parse_fighter_directory_html(html: str, base_url: str) -> List[Dict[str, str]]:
    """
    Parses UFCStats fighters directory page:
      /statistics/fighters?char=A&page=all

    Expected visible columns:
      FIRST | LAST | NICKNAME | HT. | WT. | REACH | STANCE | W | L | D | BELT

    Returns rows with keys:
      fighter_url, fighter_name, first_name, last_name, nickname_raw,
      height_raw, weight_raw, reach_raw, stance_raw, w_raw, l_raw, d_raw, belt_raw
    """
    soup = BeautifulSoup(html, "lxml")

    table = soup.select_one("table.b-statistics__table")
    if not table:
        # fallback to first table if class changes
        table = soup.select_one("table")
    if not table:
        return []

    rows: List[Dict[str, str]] = []

    for tr in table.select("tbody tr"):
        tds = tr.select("td")
        if len(tds) < 8:
            continue

        # FIRST and LAST are separate columns
        first_name = clean_text(tds[0].get_text(" ", strip=True))
        last_name = clean_text(tds[1].get_text(" ", strip=True))

        # Fighter URL: first fighter-details link in the row
        a = tr.select_one('a[href*="fighter-details"]')
        fighter_url = normalize_ufcstats_url(a["href"], base_url) if a and a.get("href") else ""

        if not fighter_url:
            continue

        nickname_raw = clean_text(tds[2].get_text(" ", strip=True)) if len(tds) > 2 else ""
        height_raw = clean_text(tds[3].get_text(" ", strip=True)) if len(tds) > 3 else ""
        weight_raw = clean_text(tds[4].get_text(" ", strip=True)) if len(tds) > 4 else ""
        reach_raw = clean_text(tds[5].get_text(" ", strip=True)) if len(tds) > 5 else ""
        stance_raw = clean_text(tds[6].get_text(" ", strip=True)) if len(tds) > 6 else ""

        # W/L/D/Belt are typically last columns
        w_raw = clean_text(tds[7].get_text(" ", strip=True)) if len(tds) > 7 else ""
        l_raw = clean_text(tds[8].get_text(" ", strip=True)) if len(tds) > 8 else ""
        d_raw = clean_text(tds[9].get_text(" ", strip=True)) if len(tds) > 9 else ""
        belt_raw = clean_text(tds[10].get_text(" ", strip=True)) if len(tds) > 10 else ""

        fighter_name = clean_text(f"{first_name} {last_name}")

        rows.append(
            {
                "fighter_url": fighter_url,
                "fighter_name": fighter_name,
                "first_name": first_name,
                "last_name": last_name,
                "nickname_raw": nickname_raw,
                "height_raw": height_raw,
                "weight_raw": weight_raw,
                "reach_raw": reach_raw,
                "stance_raw": stance_raw,
                "w_raw": w_raw,
                "l_raw": l_raw,
                "d_raw": d_raw,
                "belt_raw": belt_raw,
            }
        )

    # de-dupe by URL
    seen = set()
    out = []
    for r in rows:
        u = r["fighter_url"]
        if u in seen:
            continue
        seen.add(u)
        out.append(r)

    return out

def parse_fighter_directory(html: str, base_url: str) -> List[Dict[str, str]]:
    """
    Public entrypoint used by ingestion scripts.
    """
    return parse_fighter_directory_html(html, base_url=base_url)
