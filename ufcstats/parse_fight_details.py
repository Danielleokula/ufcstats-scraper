# ufcstats/parse_fight_details.py
from __future__ import annotations

from bs4 import BeautifulSoup


def _text(el) -> str:
    if not el:
        return ""
    return " ".join(el.get_text(" ", strip=True).split())


def _safe_href(a) -> str:
    if not a:
        return ""
    return a.get("href", "").strip()


def parse_fight_details(html: str, snapshot: str, fight_url: str) -> dict:
    """
    Parse a UFCStats fight-details page into a single flat row (raw strings).

    Keeps values mostly as raw strings (e.g., "15 of 27", "55%", "5:47") to
    match your current 'raw' layer philosophy. Staging can normalize later.
    """
    soup = BeautifulSoup(html, "lxml")

    row: dict[str, str] = {
        "snapshot": snapshot,
        "fight_url": fight_url,
        "event_url": "",
        "event_name": "",
        "weight_class_raw": "",
        "method_raw": "",
        "round_raw": "",
        "time_raw": "",
        "time_format_raw": "",
        "referee_raw": "",
        "details_raw": "",
        "fighter_1_name": "",
        "fighter_1_url": "",
        "fighter_1_result": "",
        "fighter_2_name": "",
        "fighter_2_url": "",
        "fighter_2_result": "",
        # Totals (raw strings)
        "kd_1": "",
        "kd_2": "",
        "sig_str_1": "",
        "sig_str_2": "",
        "sig_str_pct_1": "",
        "sig_str_pct_2": "",
        "total_str_1": "",
        "total_str_2": "",
        "td_1": "",
        "td_2": "",
        "td_pct_1": "",
        "td_pct_2": "",
        "sub_att_1": "",
        "sub_att_2": "",
        "rev_1": "",
        "rev_2": "",
        "ctrl_1": "",
        "ctrl_2": "",
        # Sig strike breakdown totals (raw strings like "10 of 20")
        "head_1": "",
        "head_2": "",
        "body_1": "",
        "body_2": "",
        "leg_1": "",
        "leg_2": "",
        "distance_1": "",
        "distance_2": "",
        "clinch_1": "",
        "clinch_2": "",
        "ground_1": "",
        "ground_2": "",
    }

    # Event title & event URL (top h2 with link to event-details)
    h2 = soup.select_one("h2.b-content__title a.b-link")
    if h2:
        row["event_url"] = _safe_href(h2)
        row["event_name"] = _text(h2)

    # Fighter blocks (names/urls/results + nickname on 2nd block sometimes)
    people = soup.select("div.b-fight-details__persons div.b-fight-details__person")
    if len(people) >= 2:
        p1, p2 = people[0], people[1]
        row["fighter_1_result"] = _text(p1.select_one("i.b-fight-details__person-status"))
        a1 = p1.select_one("a.b-fight-details__person-link")
        row["fighter_1_name"] = _text(a1)
        row["fighter_1_url"] = _safe_href(a1)

        row["fighter_2_result"] = _text(p2.select_one("i.b-fight-details__person-status"))
        a2 = p2.select_one("a.b-fight-details__person-link")
        row["fighter_2_name"] = _text(a2)
        row["fighter_2_url"] = _safe_href(a2)

    # Weight class (fight title line)
    fight_title = soup.select_one("div.b-fight-details__fight-head i.b-fight-details__fight-title")
    if fight_title:
        row["weight_class_raw"] = _text(fight_title).replace("  ", " ").strip()

    # Method / round / time / time format / referee
    # They live in: p.b-fight-details__text with labels "Method:", "Round:" ...
    meta_ps = soup.select("div.b-fight-details__fight div.b-fight-details__content p.b-fight-details__text")
    for p in meta_ps:
        txt = _text(p)
        if "Method:" in txt:
            # e.g. "Method: Submission Round: 2 Time: 4:46 Time format: 5 Rnd ... Referee: Herb Dean"
            # We'll parse by labels conservatively
            def grab(label: str) -> str:
                if label not in txt:
                    return ""
                after = txt.split(label, 1)[1].strip()
                # stop at next known label
                stops = ["Method:", "Round:", "Time:", "Time format:", "Referee:", "Details:"]
                for s in stops:
                    if s != label and s in after:
                        after = after.split(s, 1)[0].strip()
                return after

            row["method_raw"] = grab("Method:")
            row["round_raw"] = grab("Round:")
            row["time_raw"] = grab("Time:")
            row["time_format_raw"] = grab("Time format:")
            row["referee_raw"] = grab("Referee:")

        if "Details:" in txt:
            # Often the last token(s) are the finish detail, but the HTML sometimes has lots of whitespace
            # We'll just take everything after "Details:"
            row["details_raw"] = txt.split("Details:", 1)[1].strip()

    # Totals table: first big table after "Totals"
    # Header: Fighter, KD, Sig. str., Sig. str. %, Total str., Td, Td %, Sub. att, Rev., Ctrl
    totals_table = soup.select_one("section.b-fight-details__section table[style*='width']")
    # The page has multiple tables; the first wide one after Totals is what we want.
    # We'll instead locate by thead text containing "Sig. str."
    for tbl in soup.select("table"):
        head = _text(tbl.select_one("thead"))
        if "Sig. str." in head and "Total str." in head and "Sub. att" in head and "Ctrl" in head:
            totals_table = tbl
            break

    if totals_table:
        # In tbody, each stat cell has two <p> lines (fighter1 then fighter2)
        cells = totals_table.select("tbody tr td")
        # cells[0] is fighter names column (skip)
        # then KD, SigStr, SigStr%, TotalStr, Td, Td%, SubAtt, Rev, Ctrl
        if len(cells) >= 10:
            def two_lines(td):
                ps = td.select("p.b-fight-details__table-text")
                v1 = _text(ps[0]) if len(ps) > 0 else ""
                v2 = _text(ps[1]) if len(ps) > 1 else ""
                return v1, v2

            kd1, kd2 = two_lines(cells[1])
            sig1, sig2 = two_lines(cells[2])
            sigp1, sigp2 = two_lines(cells[3])
            tot1, tot2 = two_lines(cells[4])
            td1, td2 = two_lines(cells[5])
            tdp1, tdp2 = two_lines(cells[6])
            sub1, sub2 = two_lines(cells[7])
            rev1, rev2 = two_lines(cells[8])
            ctrl1, ctrl2 = two_lines(cells[9])

            row.update(
                {
                    "kd_1": kd1,
                    "kd_2": kd2,
                    "sig_str_1": sig1,
                    "sig_str_2": sig2,
                    "sig_str_pct_1": sigp1,
                    "sig_str_pct_2": sigp2,
                    "total_str_1": tot1,
                    "total_str_2": tot2,
                    "td_1": td1,
                    "td_2": td2,
                    "td_pct_1": tdp1,
                    "td_pct_2": tdp2,
                    "sub_att_1": sub1,
                    "sub_att_2": sub2,
                    "rev_1": rev1,
                    "rev_2": rev2,
                    "ctrl_1": ctrl1,
                    "ctrl_2": ctrl2,
                }
            )

    # Significant strikes breakdown totals table (Head/Body/Leg/Distance/Clinch/Ground)
    sig_tables = []
    for tbl in soup.select("table"):
        head = _text(tbl.select_one("thead"))
        if "Head" in head and "Body" in head and "Leg" in head and "Distance" in head and "Clinch" in head and "Ground" in head:
            sig_tables.append(tbl)

    if sig_tables:
        sig_tbl = sig_tables[0]
        cells = sig_tbl.select("tbody tr td")
        # columns: Fighter, Sig.str, Sig.str%, Head, Body, Leg, Distance, Clinch, Ground
        if len(cells) >= 9:
            def two_lines(td):
                ps = td.select("p.b-fight-details__table-text")
                v1 = _text(ps[0]) if len(ps) > 0 else ""
                v2 = _text(ps[1]) if len(ps) > 1 else ""
                return v1, v2

            head1, head2 = two_lines(cells[3])
            body1, body2 = two_lines(cells[4])
            leg1, leg2 = two_lines(cells[5])
            dist1, dist2 = two_lines(cells[6])
            clin1, clin2 = two_lines(cells[7])
            gr1, gr2 = two_lines(cells[8])

            row.update(
                {
                    "head_1": head1,
                    "head_2": head2,
                    "body_1": body1,
                    "body_2": body2,
                    "leg_1": leg1,
                    "leg_2": leg2,
                    "distance_1": dist1,
                    "distance_2": dist2,
                    "clinch_1": clin1,
                    "clinch_2": clin2,
                    "ground_1": gr1,
                    "ground_2": gr2,
                }
            )

    return row
