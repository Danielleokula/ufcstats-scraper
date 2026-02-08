# ufcstats/parse_event_details.py
from __future__ import annotations

from typing import Dict, List, Tuple

from bs4 import BeautifulSoup

from ufcstats.net import normalize_ufcstats_url


def clean_text(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()


def _extract_labeled_value(soup: BeautifulSoup, label: str) -> str:
    """
    <li class="b-list__box-list-item">Date: January 24, 2026</li>
    <li class="b-list__box-list-item">Location: Las Vegas, Nevada, USA</li>
    """
    for li in soup.select("li.b-list__box-list-item"):
        txt = clean_text(li.get_text(" ", strip=True))
        if txt.lower().startswith(label.lower() + ":"):
            return clean_text(txt.split(":", 1)[1])
    return ""


def _two_lines(td) -> Tuple[str, str]:
    """
    Stat cells (KD/STR/TD/SUB) are stacked:
      top fighter value
      bottom fighter value
    We preserve ordering by splitting on newline.
    """
    parts = [clean_text(p) for p in td.get_text("\n", strip=True).split("\n") if clean_text(p)]
    if len(parts) >= 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], ""
    return "", ""


def _parse_wl(td) -> str:
    """
    The leftmost cell shows a WIN badge for the winner.
    We normalize to: win / loss / draw / nc / ""
    (Most commonly only 'win' is explicitly shown.)
    """
    txt = clean_text(td.get_text(" ", strip=True)).upper()
    if "WIN" in txt:
        return "win"
    if "LOSS" in txt:
        return "loss"
    if "DRAW" in txt:
        return "draw"
    if "NC" in txt or "NO CONTEST" in txt:
        return "nc"
    return ""


def parse_event_details_html(html: str, base_url: str) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """
    Mirror of event-details page:
      https://ufcstats.com/event-details/<event_id>

    Returns:
      event_meta:
        - event_name
        - event_date_raw
        - event_location_raw

      bouts: one row per bout with:
        - bout_url
        - bout_order
        - fighter_1_name, fighter_1_url, fighter_1_result
        - fighter_2_name, fighter_2_url, fighter_2_result
        - kd_1,str_1,td_1,sub_1
        - kd_2,str_2,td_2,sub_2
        - weight_class_raw
        - method_raw
        - round_raw
        - time_raw
    """
    soup = BeautifulSoup(html, "lxml")

    # Event title
    title_el = soup.select_one("h2.b-content__title")
    event_name = clean_text(title_el.get_text(" ", strip=True)) if title_el else ""

    # Event meta (Date / Location)
    event_date_raw = _extract_labeled_value(soup, "Date")
    event_location_raw = _extract_labeled_value(soup, "Location")

    event_meta = {
        "event_name": event_name,
        "event_date_raw": event_date_raw,
        "event_location_raw": event_location_raw,
    }

    table = soup.select_one("table.b-fight-details__table") or soup.select_one("table")
    bouts: List[Dict[str, str]] = []
    if not table:
        return event_meta, bouts

    bout_order = 0

    for tr in table.select("tbody tr"):
        tds = tr.select("td")
        if len(tds) < 10:
            continue

        # Fight details link (bout_url)
        bout_a = tr.select_one('a[href*="fight-details"]')
        bout_url = normalize_ufcstats_url(bout_a.get("href", ""), base_url) if bout_a else ""
        if not bout_url:
            continue

        bout_order += 1

        # W/L badge (usually indicates winner; loser often blank)
        fighter_1_result = _parse_wl(tds[0])
        fighter_2_result = "loss" if fighter_1_result == "win" else ""

        # Fighters (two anchors inside fighter cell)
        fighter_links = tds[1].select('a[href*="fighter-details"]')
        if len(fighter_links) < 2:
            continue

        fighter_1_name = clean_text(fighter_links[0].get_text(" ", strip=True))
        fighter_2_name = clean_text(fighter_links[1].get_text(" ", strip=True))
        fighter_1_url = normalize_ufcstats_url(fighter_links[0].get("href", ""), base_url)
        fighter_2_url = normalize_ufcstats_url(fighter_links[1].get("href", ""), base_url)

        # Stats (each has two stacked values)
        kd_1, kd_2 = _two_lines(tds[2])
        str_1, str_2 = _two_lines(tds[3])
        td_1, td_2 = _two_lines(tds[4])
        sub_1, sub_2 = _two_lines(tds[5])

        # Bout meta
        weight_class_raw = clean_text(tds[6].get_text(" ", strip=True))
        method_raw = clean_text(tds[7].get_text(" ", strip=True))
        round_raw = clean_text(tds[8].get_text(" ", strip=True))
        time_raw = clean_text(tds[9].get_text(" ", strip=True))

        bouts.append(
            {
                "bout_url": bout_url,
                "bout_order": str(bout_order),
                "fighter_1_name": fighter_1_name,
                "fighter_1_url": fighter_1_url,
                "fighter_1_result": fighter_1_result,
                "fighter_2_name": fighter_2_name,
                "fighter_2_url": fighter_2_url,
                "fighter_2_result": fighter_2_result,
                "kd_1": kd_1,
                "str_1": str_1,
                "td_1": td_1,
                "sub_1": sub_1,
                "kd_2": kd_2,
                "str_2": str_2,
                "td_2": td_2,
                "sub_2": sub_2,
                "weight_class_raw": weight_class_raw,
                "method_raw": method_raw,
                "round_raw": round_raw,
                "time_raw": time_raw,
            }
        )

    return event_meta, bouts


def parse_event_details(html: str, base_url: str):
    """
    Public entrypoint used by ingestion scripts.
    """
    return parse_event_details_html(html, base_url=base_url)
