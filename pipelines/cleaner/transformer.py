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


def _extract_time_range_from_raw_when(raw_when: str | None) -> str | None:
    """
    Extracts a time range from raw_when and normalizes it to '9:00 p.m. to 1:00 a.m.' format.
    Returns None if no time found.
    """
    if not raw_when:
        return None
    # Match patterns like '13:00 – 21:30', '8:00–11:00 p.m.', '21:00 – sáb., 17 de mai., 0', etc.
    # Accepts both 24h and 12h with am/pm, and various dashes
    time_pattern = re.compile(r"(\d{1,2}[:h.]\d{2}|\d{1,2})(?:\s*[ap]\.?m\.?)?\s*[–-]\s*(\d{1,2}[:h.]\d{2}|\d{1,2})(?:\s*[ap]\.?m\.?)?", re.I)
    ampm_pattern = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*([ap]\.?m\.?)", re.I)

    # Try to find a time range
    match = time_pattern.search(raw_when)
    if match:
        start, end = match.group(1), match.group(2)
        # Try to find am/pm for each
        ampm = ampm_pattern.findall(raw_when)
        def fmt(t, ampm_val=None):
            if ':' in t:
                h, m = t.split(':')
            elif 'h' in t:
                h, m = t.split('h')
            elif '.' in t:
                h, m = t.split('.')
            else:
                h, m = t, '00'
            h = int(h)
            m = int(m)
            # Try to infer am/pm
            if ampm_val:
                suffix = ampm_val.lower().replace('.', '')
                if suffix.startswith('a'):
                    period = 'a.m.'
                else:
                    period = 'p.m.'
            else:
                period = ''
            return f"{h}:{m:02d} {period}".strip()
        # Assign am/pm if found
        start_ampm = ampm[0][2] if len(ampm) > 0 else None
        end_ampm = ampm[1][2] if len(ampm) > 1 else start_ampm
        start_fmt = fmt(start, start_ampm)
        end_fmt = fmt(end, end_ampm)
        return f"{start_fmt} to {end_fmt}"
    # If only a single time is found
    ampm_single = ampm_pattern.search(raw_when)
    if ampm_single:
        h, m, ap = ampm_single.groups()
        h = int(h)
        m = int(m) if m else 0
        period = 'a.m.' if ap.lower().startswith('a') else 'p.m.'
        return f"{h}:{m:02d} {period}"
    return None


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
    time_str = _extract_time_range_from_raw_when(raw.get("raw_when"))

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

    return cleaned 