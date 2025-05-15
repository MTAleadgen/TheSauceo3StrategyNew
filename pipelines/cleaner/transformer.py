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

from dateutil.parser import parse as dt_parse


# ---------------------------------------------------------------------
# 1.  REGEX & CONSTANTS
# ---------------------------------------------------------------------

_PRICE_RE      = re.compile(r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?)")
_CURRENCY_RE   = re.compile(r"(r\$|us?\$|\$|€|£)", re.I)

_LIVE_BAND_RE  = re.compile(r"\b(banda|band|live\s+band|grupo)\b", re.I)
_CLASS_RE      = re.compile(r"\b(aula|class|work\s*shop|lesson|curso)\b", re.I)

_STYLE_KEYWORDS = {
    "samba":   ("samba",),
    "forro":   ("forró", "forro"),
    "bachata": ("bachata",),
    "kizomba": ("kizomba",),
    "salsa":   ("salsa",),
    "zouk":    ("zouk",),
    "lambada": ("lambada",),
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


def _extract_styles(*chunks: str) -> List[str]:
    """Return list of dance styles detected in *chunks*."""
    haystack = " ".join(filter(None, chunks)).lower()
    found: List[str] = []

    for style, variants in _STYLE_KEYWORDS.items():
        if any(v in haystack for v in variants):
            found.append(style)

    return found


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

    ev_day = raw.get("event_day")          # may be str or date

    # ---------- description ------------------------------------------------
    description = raw.get("description", "").strip()
    name = raw.get("name", "")

    # ---------- dance styles ----------------------------------------------
    styles = raw.get("dance_styles") or _extract_styles(
        raw.get("name", ""), description
    )

    # we need at least one style to consider this an eligible record
    if not styles:
        return None

    # ---------- times ------------------------------------------------------
    start_ts = _combine_date_time(ev_day, raw.get("start_time"))
    end_ts   = _combine_date_time(ev_day, raw.get("end_time"))

    # ---------- flags ------------------------------------------------------
    live_band    = raw.get("live_band")
    class_before = raw.get("class_before")
    price        = raw.get("price")

    # ----------------------------------------------------------------------
    cleaned: Dict[str, Any] = {
        "event_id":  raw.get("id") or raw.get("event_id"),
        "cleaned_at": datetime.utcnow().isoformat(timespec="seconds"),

        "description":      description,
        "name":             name,
        "dance_styles":     styles,
        "price":            price,
        "start_time":       _format_time_human(start_ts) if start_ts else None,
        "end_time":         end_ts.isoformat()   if end_ts   else None,
        "live_band":        live_band,
        "class_before":     class_before,

        # NEW passthrough columns ------------------------------------------
        "venue":            raw.get("venue"),
        "address":          raw.get("address"),
        "event_day":        ev_day,
        "country":          raw.get("country"),
        "city":             raw.get("city"),
    }

    return cleaned 