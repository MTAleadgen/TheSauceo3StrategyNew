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
    "afrobeat": [r"afrobeat", r"afro-fusion"],
    "argentine tango": [r"argentine tango"],
    "azonto": [r"azonto"],
    "bachata": [r"bachata"],
    "balboa": [r"balboa"],
    "ballet": [r"ballet"],
    "belly dance": [r"belly dance"],
    "bhangra": [r"bhangra"],
    "bolero": [r"bolero"],
    "brazilian zouk": [r"brazilian zouk"],
    "breaking": [r"breaking", r"b-boying", r"break dance"],
    "capoeira": [r"capoeira"],
    "charleston": [r"charleston"],
    "cha-cha-cha": [r"cha[- ]?cha[- ]?cha", r"chacha"],
    "collegiate shag": [r"collegiate shag"],
    "contemporary": [r"contemporary"],
    "country two-step": [r"country two[- ]?step"],
    "cumbia": [r"cumbia"],
    "dancehall": [r"dancehall"],
    "east coast swing": [r"east coast swing"],
    "flamenco": [r"flamenco"],
    "forro": [r"forró", r"forro"],
    "foxtrot": [r"foxtrot"],
    "heels dance": [r"heels dance"],
    "hip-hop": [r"hip[- ]?hop", r"freestyle", r"new[- ]?style"],
    "house dance": [r"house dance"],
    "hustle": [r"hustle", r"disco"],
    "irish stepdance": [r"irish stepdance"],
    "jazz dance": [r"jazz dance"],
    "jive": [r"jive"],
    "k-pop choreography": [r"k[- ]?pop choreography"],
    "kizomba": [r"kizomba", r"\\bkiz\\b"],
    "krump": [r"krump"],
    "kuduro": [r"kuduro"],
    "lambada": [r"lambada"],
    "line dance": [r"line dance", r"line dancing", r"line-dancing"],
    "lindy hop": [r"lindy hop"],
    "locking": [r"locking"],
    "lyrical dance": [r"lyrical dance"],
    "mambo": [r"mambo"],
    "merengue": [r"merengue"],
    "modern dance": [r"modern dance"],
    "pasodoble": [r"pasodoble"],
    "peabody": [r"peabody"],
    "popping": [r"popping"],
    "polka": [r"polka"],
    "quickstep": [r"quickstep"],
    "rumba": [r"rumba"],
    "reggaeton dance": [r"reggaeton dance"],
    "samba": [r"samba", r"samba no pé", r"samba de gafieira", r"carnaval"],
    "samba de gafieira": [r"samba de gafieira"],
    "salsa": [r"salsa"],
    "shuffle dance": [r"shuffle dance", r"melbourne shuffle"],
    "tap dance": [r"tap dance"],
    "tango": [r"tango", r"ballroom", r"international standard"],
    "tutting": [r"tutting"],
    "vogue": [r"vogue"],
    "waacking": [r"waacking"],
    "waltz": [r"waltz"],
    "west coast swing": [r"west coast swing"],
    "tea dance": [r"tea dance"],
    "vallenato": [r"vallenato"],
    "pagode": [r"pagode"],
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


def extract_time_from_raw_when(raw_when: str) -> Optional[str]:
    logger = logging.getLogger(__name__)
    if not raw_when:
        return None
    # Try to find time ranges like "8:00 p.m. – 1:00 a.m." or "19:00 – 01:00" or "8:00 – 9:30 PM"
    time_range = re.search(r'(\d{1,2}[:h.,]?\d{0,2}\s*[ap]?\.?m?\.?|\d{1,2})\s*(?:–|to|a|al|até|\'al\'|-)\s*(\d{1,2}[:h.,]?\d{0,2}\s*[ap]?\.?m?\.?|\d{1,2})', raw_when, re.IGNORECASE)
    if time_range:
        t1 = time_range.group(1).strip()
        t2 = time_range.group(2).strip()
        # Only accept if at least one has a colon or am/pm
        if (":" in t1 or ":" in t2 or re.search(r'[ap]\.m\.', t1 + t2, re.IGNORECASE)):
            return f"{t1} to {t2}"
    # Try to find single times like "7:00 p.m." or "19:00"
    single_time = re.search(r'(\d{1,2}[:h.,]\d{2}\s*[ap]?\.?m?\.?|\d{1,2}\s*[ap]\.m\.)', raw_when, re.IGNORECASE)
    if single_time:
        return single_time.group(1).strip()
    # If only a number is found, ignore it (likely a day, not a time)
    just_number = re.search(r'\b(\d{1,2})\b', raw_when)
    if just_number:
        logger.info(f"Fallback found only a number in raw_when, ignoring as time: {just_number.group(1)} from '{raw_when}'")
        return None
    logger.warning(f"Fallback failed to extract valid time from raw_when: '{raw_when}'")
    return None


def clean_meridiem(time_str):
    """Fixes p:m: to p.m. and normalizes meridiem, only in the right context."""
    if not time_str:
        return time_str
    # Replace p:m: or a:m: with p.m. or a.m.
    time_str = re.sub(r'([ap])[:\.]m[:\.]', r'\1.m.', time_str, flags=re.IGNORECASE)
    # Replace p:m or a:m with p.m. or a.m.
    time_str = re.sub(r'([ap])[:\.]m', r'\1.m.', time_str, flags=re.IGNORECASE)
    # Replace p m or a m with p.m. or a.m.
    time_str = re.sub(r'([ap])\s*m', r'\1.m.', time_str, flags=re.IGNORECASE)
    # Remove duplicate colons
    time_str = re.sub(r':+', ':', time_str)
    # Remove trailing colons
    time_str = re.sub(r':\b', '', time_str)
    return time_str


def to_12_hour(hour, minute):
    hour = int(hour)
    minute = int(minute)
    ampm = 'a.m.' if hour < 12 else 'p.m.'
    hour12 = hour % 12 or 12
    return f'{hour12}:{minute:02d} {ampm}'


def fix_time_format(time_str):
    """Fixes time strings like '400' to '4:00', removes double periods, and trims whitespace."""
    if not time_str:
        return time_str
    # Insert colon if missing (e.g., 400 -> 4:00, 930 -> 9:30)
    time_str = re.sub(r'\b(\d{1,2})(\d{2})\b', r'\1:\2', time_str)
    # Remove double periods
    time_str = re.sub(r'\.\.', '.', time_str)
    # Remove extra spaces
    time_str = re.sub(r'\s+', ' ', time_str)
    return time_str.strip()


def normalize_time_am_pm(time_str: str) -> Optional[str]:
    if not time_str:
        return None
    # Remove extraneous date text (e.g., 'Thursday, June 19, 1:00 p.m. to Sunday, June 22, 5:00 p.m.')
    # Keep only the time range or single time
    # Try to extract the last two time-like strings (for ranges)
    matches = re.findall(r'(\d{1,2})(?::(\d{2}))?\s*([ap]\.m\.|[ap]m)?', time_str)
    # If range
    if 'to' in time_str or '–' in time_str or '-' in time_str:
        # Try to extract two times
        range_match = re.search(r'(\d{1,2})(?::(\d{2}))?\s*([ap]\.m\.|[ap]m)?\s*(?:to|–|-)\s*(\d{1,2})(?::(\d{2}))?\s*([ap]\.m\.|[ap]m)?', time_str, re.IGNORECASE)
        if range_match:
            h1, m1, ampm1, h2, m2, ampm2 = range_match.groups()
            m1 = m1 or '00'
            m2 = m2 or '00'
            # Convert to 12-hour if needed
            t1 = to_12_hour(h1, m1) if not ampm1 else f"{int(h1)%12 or 12}:{m1} {ampm1.strip('.').lower()}."
            t2 = to_12_hour(h2, m2) if not ampm2 else f"{int(h2)%12 or 12}:{m2} {ampm2.strip('.').lower()}."
            result = f"{t1} to {t2}"
            return fix_time_format(clean_meridiem(result))
    # If single time
    single_match = re.match(r'(\d{1,2})(?::(\d{2}))?\s*([ap]\.m\.|[ap]m)?', time_str, re.IGNORECASE)
    if single_match:
        h, m, ampm = single_match.groups()
        m = m or '00'
        if ampm:
            t = f"{int(h)%12 or 12}:{m} {ampm.strip('.').lower()}."
        else:
            t = to_12_hour(h, m)
        return fix_time_format(clean_meridiem(t))
    # If only a number (e.g., '22:00' or '23'), convert to am/pm
    just_number = re.match(r'^(\d{1,2})(:00)?$', time_str.strip())
    if just_number:
        t = just_number.group(1)
        try:
            return fix_time_format(clean_meridiem(to_12_hour(t, '00')))
        except Exception:
            return time_str
    # If nothing matches, return as is
    return fix_time_format(clean_meridiem(time_str.strip()))


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
    if not time_str:
        raw_when = raw.get("raw_when", "")
        logger.info(f"LLM did not provide time. Attempting fallback extraction from raw_when: {raw_when}")
        fallback_time = extract_time_from_raw_when(raw_when)
        logger.info(f"Fallback extracted time: {fallback_time}")
        time_str = fallback_time
    # Normalize time to am/pm format
    time_str = normalize_time_am_pm(time_str) if time_str else None
    if not time_str:
        logger.warning(f"Event missing or ambiguous time after normalization. raw_when: {raw.get('raw_when')}, description: {description}, name: {name}")
    else:
        # Log the final normalized time for debugging
        logger.info(f"Final normalized time: {time_str}")

    # ---------- flags ------------------------------------------------------
    live_band    = raw.get("live_band")
    class_before = raw.get("class_before")
    price        = raw.get("price")

    # ---------- is_dance_event passthrough ---------------------------------
    is_dance_event = raw.get("is_dance_event")
    logger.info(f"is_dance_event from LLM: {is_dance_event}")

    # ---------- dance event logic ------------------------------------------
    # Genres to consider as dance events
    allowed_genres = {"bachata", "zouk", "salsa", "pagode"}
    # Concert keywords
    concert_keywords = [
        "concert", "performs live", "band", "show", "live at", "music event", "dj set", "performs on stage", "live performance", "musical performance"
    ]
    text = f"{name} {description}".lower()
    is_concert = any(kw in text for kw in concert_keywords)
    # If LLM set is_dance_event, use it
    if is_dance_event is not None:
        pass  # Use LLM's value
    else:
        # If it's a concert and none of the allowed genres are present, set to False
        if is_concert and not any(style in allowed_genres for style in styles):
            is_dance_event = False
        else:
            # Otherwise, default to True (unless obviously not a dance event)
            is_dance_event = True

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
        # Add is_dance_event passthrough
        "is_dance_event":   is_dance_event,
    }

    return cleaned 