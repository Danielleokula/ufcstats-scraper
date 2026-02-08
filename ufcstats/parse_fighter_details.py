# ufcstats/parse_fighter_details.py
from __future__ import annotations

from typing import Dict
from bs4 import BeautifulSoup


def clean_text(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()


def _extract_labeled_value(soup: BeautifulSoup, label: str) -> str:
    """
    On fighter-details pages, left box has rows like:
      <li class="b-list__box-list-item">DOB: Jul 13, 1978</li>
    """
    for li in soup.select("li.b-list__box-list-item"):
        txt = clean_text(li.get_text(" ", strip=True))
        if txt.lower().startswith(label.lower() + ":"):
            return clean_text(txt.split(":", 1)[1])
    return ""


def parse_fighter_details(html: str) -> Dict[str, str]:
    """
    Parse ONLY what you want from fighter-details:
      - dob_raw
      - career stats: SLpM, Str. Acc., SApM, Str. Def., TD Avg., TD Acc., TD Def., Sub. Avg.
    Returns a single dict (one fighter).
    """
    soup = BeautifulSoup(html, "lxml")

    dob_raw = _extract_labeled_value(soup, "DOB")

    # Career stats live in the middle box; UFCStats uses <li> items:
    # "SLpM: 4.89", "Str. Acc.: 48%", etc.
    wanted = {
        "SLpM": "slpm",
        "Str. Acc.": "str_acc",
        "SApM": "sapm",
        "Str. Def.": "str_def",
        "TD Avg.": "td_avg",
        "TD Acc.": "td_acc",
        "TD Def.": "td_def",
        "Sub. Avg.": "sub_avg",
    }

    out: Dict[str, str] = {"dob_raw": dob_raw}

    # Initialize empty strings (stable schema)
    for _, key in wanted.items():
        out[key] = ""

    for li in soup.select("li.b-list__box-list-item"):
        txt = clean_text(li.get_text(" ", strip=True))
        if ":" not in txt:
            continue
        k, v = [clean_text(x) for x in txt.split(":", 1)]
        if k in wanted:
            out[wanted[k]] = v

    return out
