"""
pipelines/apis/ticketmaster.py
Hybrid Ticketmaster → Supabase pipeline

• Pulls full Dance segment (with pagination)
• Pulls per‑style keyword pages (without segment filter) for extra recall
• Merges + de‑dupes raw events
• Filters out stage shows / concerts via PERFORMANCE_NOISE & venue guards
• Keeps an event if (segment == Dance) OR (activity word present) OR (classified to watch style)
• Writes to events_ticketmaster via bulk upsert
"""

from __future__ import annotations
import os, re, time, requests
from datetime import datetime, UTC
from urllib.parse import urljoin
from typing import List, Dict, Any
from dotenv import load_dotenv
from supabase import create_client, Client

# ─────────────────── ENV & CLIENTS ───────────────────
load_dotenv()
TM_KEY = os.getenv("TICKETMASTER_CONSUMER_KEY")
SB_URL, SB_KEY = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
if not TM_KEY:
    raise RuntimeError("TICKETMASTER_CONSUMER_KEY missing in .env")

supabase: Client | None = create_client(SB_URL, SB_KEY) if SB_URL and SB_KEY else None
BASE_URL = "https://app.ticketmaster.com/discovery/v2/events.json"

# ─────────────────── PATTERNS & CONSTANTS ────────────
# More conservative list for high-confidence participatory signals
STRONG_ACTIVITY_INDICATORS = re.compile(
    r"\b(workshop|dance\s+class|dance\s+classes|lesson|dance\s+social|social\s+dance|milonga|praktika|(?:dance|disco)\s+party|participatory\s+dance|dance\s+practice|dance\s+session|tea\s+dance|day\s+disco|dance\s+night)\b", re.I
)

# General activity terms, more specific than before
ACTIVITY = re.compile(
    r"\b(workshop|dance\s+class|dance\s+classes|lesson|dance\s+social|social\s+dance|milonga|praktika|(?:dance|disco)\s+party|participatory\s+dance|dance\s+practice|dance\s+session|tea\s+dance|day\s+disco|dance\s+night|"
    r"battle|swing|dance\s+jam|contact\s+jam|zumba|cypher|freestyle\s+session|open\s+styles|\bdance\b)\b", re.I
)

THEATRE_ARENA = re.compile(
    r"\b(theatre|theater|arena|stadium|coliseum|amphitheat(er|re)|centre|center|auditorium|hall|ballroom|music fair|pavilion|conservatory|city center|performing\s+arts\s+center|opera\s+house|guildhall|arts\s+centre|"
    r"echoplex|the echo|house of blues|kia center|win entertainment centre|thalia hall|beachland ballroom|mcmenamins crystal ballroom|la boom|cypress|"
    r"O2 Guildhall|Martin Marietta Center for the Performing Arts|James K. Polk Theatre|The National|Mullett Arena|Harrison Opera House|Balboa Theatre|"
    r"El Mocambo|Edinburgh Corn Exchange|The Blue Note)\b",
    re.I,
)

PERFORMANCE_NOISE = re.compile(
    r"\b(ballet|swan lake|nutcracker|cinderella|giselle|romeo and juliet|don quixote|"
    r"sylphide|showcase|recital|broadway|disney|musical|opera|orchestra|choir|philharmonic|"
    r"high school musical|bop to the top|film|screening|movie|play|production|theatrical|"
    r"concert|live music|ft\.|feat\.|presents|gala|awards|tour(e)?|comedy|stand-up|exhibition|"
    r"sonero|live band|album release|listening party|artist performs|band performs|dj set|headliner|opening act|special guest|life & trials of|festival|"
    r"school\s+of\s+dance|year-end\s+recital|student\s+showcase|dance\s+academy\s+presents|"
    r"dance\s+company|ballet\s+company|dance\s+troupe|professional\s+dancers|profesionales|"
    r"championship|competition|tribute\s+to|a\s+tribute|tribute|"
    r"live\s+on\s+stage|high\s+energy\s+show|fundraiser|charity\s+event|watch\s+party|"
    r"broadway\s+rave|musical\s+theatre\s+dance\s+party|"
    r"band|dance\s+show|dance\s+recital|dance\s+gavin\s+dance|"
    r"vibe\s+year-end\s+recitals|heist-\s+eleve\s+dance|an\s+irish\s+christmas)\b",
    re.I,
)

STYLE_REGEX: dict[str, re.Pattern] = {
    "Salsa": re.compile(r"\bsalsa\b", re.I),
    "Bachata": re.compile(r"\bbachata\b", re.I),
    "Hip-Hop": re.compile(r"\bhip[\s-]?hop\b", re.I),
    "House": re.compile(r"\bhouse\b", re.I),
    "Afrobeat": re.compile(r"\bafro[\s-]?beats?\b", re.I),
    "Zouk": re.compile(r"\bzouk\b", re.I),
    "Kizomba": re.compile(r"\bkiz(omba)?\b", re.I),
    "Balboa": re.compile(r"\bbalboa\b", re.I),
    "Breaking": re.compile(r"\bbreak\s?danc|\bb-boy|\bb-girl", re.I),
    "East Coast Swing": re.compile(r"\beast\s+coast\s+swing\b|\becs\b", re.I),
    "West Coast Swing": re.compile(r"\bwest\s+coast\s+swing\b|\bwcs\b", re.I),
    "Ballroom": re.compile(r"\bballroom\b", re.I),
    "Hustle": re.compile(r"\bhustle\b", re.I),
    "Samba": re.compile(r"\bsamba\b", re.I),
    "Pagode": re.compile(r"\bpagode\b", re.I),
    "Lindy Hop": re.compile(r"\blindy\s+hop\b", re.I),
    "Cha Cha": re.compile(r"\bcha\s*cha(?:\s*cha)?\b", re.I),
}

AMBIGUOUS = {"Hip-Hop", "House", "Afrobeat", "Balboa", "Breaking", "Hustle", "Samba"}
WATCH_STYLES = list(STYLE_REGEX.keys())

# ─────────────────── FETCH HELPERS ───────────────────

def fetch_pages(params: Dict[str, Any]) -> List[dict]:
    """Fetch all pages for given query params (Ticketmaster pagination)."""
    events = []
    url = BASE_URL
    while True:
        data = requests.get(url, params=params, timeout=10).json()
        events.extend(data.get("_embedded", {}).get("events", []))
        next_href = data.get("_links", {}).get("next", {}).get("href")
        if not next_href:
            break
        url = urljoin("https://app.ticketmaster.com", next_href)
        params = {}
        time.sleep(0.1)
    return events


def fetch_dance_segment() -> List[dict]:
    return fetch_pages({"apikey": TM_KEY, "classificationName": "Dance", "size": 200, "sort": "date,asc"})


def fetch_style_keyword(style: str) -> List[dict]:
    kw_override = {
        "Breaking": "breakdance OR b-boy OR b-girl",
        "East Coast Swing": "East Coast Swing OR ECS",
        "West Coast Swing": "West Coast Swing OR WCS",
        "Cha Cha": "Cha Cha OR Cha Cha Cha",
    }
    kw = kw_override.get(style, style)
    return fetch_pages({"apikey": TM_KEY, "keyword": kw, "size": 200, "sort": "date,asc"})

# ─────────────────── CLASSIFY & FILTER ───────────────

def segment_is_dance(ev: dict) -> bool:
    return any(c.get("segment", {}).get("name", "").lower() == "dance" for c in ev.get("classifications", []))


def classify_style(text: str) -> str | None:
    for name, pat in STYLE_REGEX.items():
        if pat.search(text):
            return name
    return None


def passes_filters(ev: dict, style: str | None, title: str, desc: str, venue: str) -> bool:
    blob = f"{title} {desc}".lower()
    venue_lower = venue.lower()
    seg_dance = segment_is_dance(ev)

    # 1. Performance Noise Check (Reverting to stricter: seg_dance alone won't save it)
    if PERFORMANCE_NOISE.search(blob):
        # If it looks like a performance, it MUST have STRONG activity indicators to be saved.
        if not STRONG_ACTIVITY_INDICATORS.search(blob):
            return False

    # 2. Venue-based rules (Keeping the more relaxed rule from iteration 31 for THEATRE_ARENA)
    if THEATRE_ARENA.search(venue_lower):
        # Event in a performance venue, allow if TM says it's dance OR it has a general activity word.
        if not (seg_dance or ACTIVITY.search(blob)):
            return False
    
    # 3. Style-specific ambiguity (original logic, using refined ACTIVITY)
    if style in AMBIGUOUS and not seg_dance and not ACTIVITY.search(blob):
        return False

    # 4. Final allowance
    # Keep if segment=Dance OR contains (refined) activity OR classified to a watch_style
    return seg_dance or ACTIVITY.search(blob) is not None or (style in WATCH_STYLES and style is not None and style != "Unknown")

# ─────────────────── BUILD ROW ───────────────────────

def build_row(ev: dict) -> Dict[str, Any]:
    title = ev.get("name", "")
    desc = ev.get("info") or ev.get("pleaseNote") or ""
    venue_obj = (ev.get("_embedded", {}).get("venues") or [{}])[0]
    img = next((i for i in ev.get("images", []) if i.get("ratio") in {"16_9", "3_2"}), None) or (ev.get("images") or [{}])[0]
    num = lambda x: float(x) if x and x != "0" else None
    return {
        "source_platform": "Ticketmaster",
        "source_id": ev["id"],
        "name": title,
        "description": desc,
        "venue": venue_obj.get("name"),
        "address": venue_obj.get("address", {}).get("line1"),
        "city": venue_obj.get("city", {}).get("name"),
        "country": venue_obj.get("country", {}).get("name"),
        "lat": num(venue_obj.get("location", {}).get("latitude")),
        "lng": num(venue_obj.get("location", {}).get("longitude")),
        "event_day": ev.get("dates", {}).get("start", {}).get("localDate"),
        "event_time": ev.get("dates", {}).get("start", {}).get("localTime"),
        "retrieved_at": datetime.now(UTC).isoformat(),
        "source_url": ev.get("url"),
        "raw_when": f"{ev.get('dates', {}).get('start', {}).get('localDate')} {ev.get('dates', {}).get('start', {}).get('localTime')}",
        "image_url": img.get("url"),
    }

# ─────────────────── MAIN PIPELINE ───────────────────

def main() -> None:
    raw_events = fetch_dance_segment()
    for st in WATCH_STYLES:
        raw_events.extend(fetch_style_keyword(st))

    print(f"Total raw events pulled: {len(raw_events)}")

    unique_raw = {e["id"]: e for e in raw_events}.values()

    kept_rows: List[Dict[str, Any]] = []
    per_style_count = {s: 0 for s in WATCH_STYLES}


    for ev in unique_raw:
        title = ev.get("name", "")
        desc = ev.get("info") or ev.get("pleaseNote") or ""
        venue = (ev.get("_embedded", {}).get("venues") or [{}])[0].get("name", "")
        style = classify_style(f"{title} {desc}")

        if passes_filters(ev, style or "Unknown", title, desc, venue):
            kept_rows.append(build_row(ev))
            if style in per_style_count:
                per_style_count[style] += 1

    # Report results
    print("Kept counts per style:")
    for s, c in per_style_count.items():
        print(f"  {s:<9} {c}")

    print(f"Total kept rows: {len(kept_rows)}")

    if not kept_rows:
        print("No rows to upsert.")
        return

    unique_rows = {r['source_id']: r for r in kept_rows}.values()
    # Upsert to Supabase
    if supabase:
        supabase.table('events_ticketmaster').upsert(
            list(unique_rows), on_conflict='source_id'
        ).execute()
        print('✅ Upsert complete.')
    else:
        print('Supabase not configured – skipping upsert.')

if __name__ == "__main__":
    main()
