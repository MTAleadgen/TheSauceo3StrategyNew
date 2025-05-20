"""
pipelines/cleaner/transformer.py
--------------------------------
Turn a *raw* row from `events` into the normalised record we insert
into `events_clean`.

The transformer is intentionally tolerant: if something cannot be
parsed we leave the field `None` rather than raise.
"""

from __future__ import annotations

import re
from datetime import datetime, date, time
from typing import Any, Dict, List, Tuple, Optional
import pytz
import logging

from dateutil.parser import parse as dt_parse


# ---------------------------------------------------------------------
# 1.  REGEX & CONSTANTS
# ---------------------------------------------------------------------

_PRICE_RE      = re.compile(r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?)")
_CURRENCY_RE   = re.compile(r"(r\$|us?\$|\$|€|£)", re.I)

_LIVE_BAND_RE  = re.compile(r"\b(banda|band|live\s+band|grupo)\b", re.I)
_CLASS_RE      = re.compile(r"\b(aula|class|work\s*shop|lesson|curso)\b", re.I)

DANCE_STYLE_KEYWORDS = {
    "salsa": [r"salsa"],
    "bachata": [r"bachata"],
    "kizomba": [r"kizomba", r"\bkiz\b"],
    "zouk": [r"zouk"],
    "forro": [r"forró", r"forro"],
    "samba": [r"samba"],
    "rumba": [r"rumba"],
    "line dancing": [r"line dance", r"line dancing", r"line-dancing"]
}


# ---------------------------------------------------------------------
# 2.  HELPERS
# ---------------------------------------------------------------------

def _extract_price(text: str | None) -> Tuple[Optional[int], Optional[int]]:
    """Return minimum / maximum price found in *text* (integers)."""
    if not text:
        return None, None

    # drop currency symbols & thousands separators
    text = _CURRENCY_RE.sub("", text)
    candidates = [
        float(t.replace(",", ".")) for t in _PRICE_RE.findall(text)
    ]

    if not candidates:
        return None, None

    return int(min(candidates)), int(max(candidates))


def extract_dance_styles(text: str) -> list:
    styles = []
    text_lower = text.lower()
    for style, keywords in DANCE_STYLE_KEYWORDS.items():
        for kw in keywords:
            if re.search(kw, text_lower):
                styles.append(style)
                break
    return styles


def _combine_date_time(ev_day: Any, ts: Any) -> Optional[datetime]:
    """
    *ts* may be:
        • None
        • str  -> parsed
        • datetime
    If *ts* has no date component we graft *ev_day* on.
    """
    if ts is None:
        return None

    if isinstance(ts, str):
        try:
            ts = dt_parse(ts)
        except Exception:
            return None

    if isinstance(ts, datetime):
        if ts.date() != datetime.min.date():
            return ts

        # date is missing ⇒ add event_day
        if ev_day is None:
            return None
        if isinstance(ev_day, str):
            try:
                ev_day = dt_parse(ev_day).date()
            except Exception:
                return None
        if isinstance(ev_day, date):
            return datetime.combine(ev_day, ts.time())

    return None


def _format_time_human(dt: datetime) -> str:
    if not dt:
        return None
    # Convert to local time if tz-aware, else use as is
    if dt.tzinfo:
        dt = dt.astimezone()
    hour = dt.hour % 12 or 12
    ampm = 'a.m.' if dt.hour < 12 else 'p.m.'
    return f"{hour} {ampm}"


# ---------------------------------------------------------------------
# 3.  MAIN ENTRY-POINT
# ---------------------------------------------------------------------

def transform_event_data(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Convert a row from *events* to the target structure expected by
    `events_clean`.  Returns **None** if the record should be skipped.
    """
    if not raw:
        return None

    logger = logging.getLogger(__name__)
    ev_day = raw.get("event_day")          # may be str or date

    # ---------- description ------------------------------------------------
    description = raw.get("description", "").strip()
    name = raw.get("name", "")
    # Log the text being checked for dance styles
    logger.info(f"Checking dance styles in text: {name} | {description}")
    styles = extract_dance_styles(f"{name} {description}")
    logger.info(f"Detected styles: {styles}")

    # ---------- times ------------------------------------------------------
    start_ts = _combine_date_time(ev_day, raw.get("start_time"))
    end_ts   = _combine_date_time(ev_day, raw.get("end_time"))
    time_str = raw.get("time")  # Expect LLM to provide normalized time

    # ---------- flags ------------------------------------------------------
    live_band    = raw.get("live_band")
    class_before = raw.get("class_before")
    price        = raw.get("price")

    # ----------------------------------------------------------------------
    cleaned: Dict[str, Any] = {
        "event_id":  raw.get("id") or raw.get("event_id"),
        "description":      description,
        "name":             name,
        "dance_styles":     styles,
        "price":            price,
        "live_band":        live_band,
        "class_before":     class_before,
        # NEW passthrough columns ------------------------------------------
        "venue":            raw.get("venue"),
        "address":          raw.get("address"),
        "event_day":        ev_day,
        "country":          raw.get("country"),
        "city":             raw.get("city"),
        # Add lat/lng passthrough
        "lat":              raw.get("lat"),
        "lng":              raw.get("lng"),
        # Add source_url passthrough
        "source_url":       raw.get("source_url"),
        "time":             time_str,
    }

    if not styles:
        logger.warning(f"No dance styles detected for event: {raw.get('id') or raw.get('event_id')} - {name}")

    return cleaned 