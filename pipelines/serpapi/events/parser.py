from typing import Any, Dict, Optional
from datetime import datetime, timezone, timedelta
import hashlib
import logging
import re # Ensure re is imported at the top

# Attempt to import dateutil
try:
    from dateutil import parser as dateutil_parser
except ImportError:
    logging.error("The 'python-dateutil' library is required for robust date parsing. Please install it: pip install python-dateutil")
    dateutil_parser = None

# Configure logging (get logger from root or create one)
logger = logging.getLogger(__name__)

def generate_deterministic_id(link: Optional[str]) -> Optional[str]:
    """Generates an MD5 hash from the event link to use as a stable ID."""
    if link:
        return hashlib.md5(link.encode('utf-8')).hexdigest()
    return None

# 1) Map local month abbreviations → English three-letter
_MONTH_REPLACEMENTS = {
    r'\bene\.?\b':   'jan',  # enero
    r'\bfeb\.?\b':   'feb',
    r'\bmar\.?\b':   'mar',
    r'\babr\.?\b':   'apr',  # abril / abril
    r'\bmai\.?\b':   '5',    # maio / mayo 
    r'\bmay\.?\b':   '5',    # mayo (English/Spanish)
    r'\bjun\.?\b':   '6',    # junio/junho
    r'\bjul\.?\b':   '7',
    r'\bago\.?\b':   '8',    # agosto
    r'\bsep(?:t)?\.?\b': '9',
    r'\boct\.?\b':   '10',
    r'\bnov\.?\b':   '11',
    r'\bdez\.?\b':   '12',   # dezembro
    r'\bdic\.?\b':   '12',   # diciembre
}

# 2) Strip weekday names in English/Spanish/Portuguese
_DAY_REPLACEMENTS = [
    r'\b(dom|domingo)\.?,?', r'\b(lun|lunes)\.?,?', r'\b(mar|martes)\.?,?',
    r'\b(mi[eé]|miercoles|miércoles)\.?,?', r'\b(jue|jueves)\.?,?',
    r'\b(vie|viernes)\.?,?', r'\b(sab|s[áa]bado)\.?,?'
]

# 3) Remove "de" connectors and AM/PM markers
_DE_PATTERN    = r'\bde\b'
_AMPM_PATTERN  = r'\b[ap]\.?m\.?\b'

# 4) Remove trailing date-range (everything after "–" or "-")
_RANGE_PATTERN = r'\s*(?:–|-)\s*.*$'

def _clean_date_string_for_parsing(date_str: str) -> str:
    """
    Normalize a raw SerpAPI date string so dateutil.parser.parse()
    can ingest it.  Returns something like "12 may 2025 19:00".
    """
    s = (date_str or "").lower().strip()

    # 1) months
    for pat, repl in _MONTH_REPLACEMENTS.items():
        s = re.sub(pat, repl, s)

    # 2) weekdays
    for pat in _DAY_REPLACEMENTS:
        s = re.sub(pat, '', s)

    # 3) connectors & AM/PM
    s = re.sub(_DE_PATTERN, ' ', s)
    s = re.sub(_AMPM_PATTERN, '', s)

    # 4) drop date-range suffix
    s = re.sub(_RANGE_PATTERN, '', s)

    # 5) strip punctuation & collapse spaces
    s = re.sub(r'[\\,\\.\\;]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()

    return s

def parse_event_result(event_data: Dict, max_days_forward: int = 365, city_info: Dict = None) -> Optional[Dict]:
    """Parses a raw event dictionary from SerpAPI into our standardized format.
    Returns None if the event should be excluded."""

    # 1. Basic validation - must have a name at minimum
    if not event_data.get('title'):
        logger.warning("Event without a title/name - skipping")
        return None

    # 2. Extract/compute core fields
    link = event_data.get('link')
    source_id = generate_deterministic_id(link)

    # Skip events without valid source ID (rare, but possible)
    if not source_id:
        logger.warning(f"Cannot generate source_id for event: {event_data.get('title')}")
        return None

    # 3. Try to parse dates
    # Raw date strings to preserve regardless of parsing success
    raw_start_date = event_data.get('date', {}).get('start_date', '')
    raw_when = event_data.get('date', {}).get('when', '')
    
    # Initialize to None (will remain None if parsing fails)
    event_day = None
    start_time = None
    
    try:
        # Apply our cleaning function to both date strings
        cleaned_start = _clean_date_string_for_parsing(raw_start_date)
        cleaned_when = _clean_date_string_for_parsing(raw_when)
        
        # Combine the two cleaned strings for best chance of parsing
        # 'when' often has time info that 'start_date' doesn't
        date_parts = []
        if cleaned_start:
            date_parts.append(cleaned_start)
        if cleaned_when:
            date_parts.append(cleaned_when)
            
        # Combine them with space so dateutil can parse better
        combined_date_str = ' '.join(date_parts).strip()
        
        logger.debug(f"Attempting to parse date: '{combined_date_str}' (from '{raw_start_date}' and '{raw_when}')")
        
        if combined_date_str:
            # Special handling for our numeric month format
            # Look for patterns like "5 16" (month day) and convert to "5/16/2024"
            month_day_pattern = r'(\d{1,2})\s+(\d{1,2})'
            match = re.match(month_day_pattern, combined_date_str)
            if match:
                month, day = match.groups()
                year = datetime.now().year  # Current year
                reformatted_date = f"{month}/{day}/{year}"
                try:
                    dt = datetime.strptime(reformatted_date, "%m/%d/%Y")
                    event_day = dt.date().isoformat()
                    
                    # Try to extract time from the remaining string
                    time_pattern = r'(\d{1,2}):(\d{2})'
                    time_match = re.search(time_pattern, combined_date_str)
                    if time_match:
                        hour, minute = time_match.groups()
                        start_time = f"{int(hour):02d}:{minute}"
                    logger.debug(f"Successfully parsed numeric date: {event_day} {start_time}")
                except Exception as e:
                    logger.warning(f"Error parsing numeric date pattern: {e}")
                    # Fall through to standard parsing below
            
            # If we don't have event_day yet, try standard dateutil parsing
            if not event_day:
                try:
                    # Standard dateutil parsing
                    dt = dateutil_parser.parse(combined_date_str, fuzzy=True)
                    
                    # Set the event_day (date portion only)
                    event_day = dt.date().isoformat()
                    
                    # Set start_time (time portion in HH:MM format)
                    if dt.hour != 0 or dt.minute != 0:  # Only set if we have a non-midnight time
                        start_time = f"{dt.hour:02d}:{dt.minute:02d}"
                        
                    logger.debug(f"Successfully parsed date: {event_day} {start_time}")
                except Exception as e:
                    logger.warning(f"Dateutil parse error: {e}")
    except Exception as e:
        # If parsing fails, log it but allow event to proceed with nulls
        error_msg = str(e)
        logger.warning(f"Date parsing error for event '{event_data.get('title')}': {error_msg}")
        
        # Log to date_failures.tsv
        try:
            with open('data/date_failures.tsv', 'a', encoding='utf-8') as f:
                f.write(f"{source_id}\t{raw_start_date}\t{raw_when}\t{error_msg}\n")
        except Exception as log_error:
            logger.error(f"Error logging date failure: {log_error}")
    
    # 4. Construct the standardized event record
    event_record = {
        "source_id": source_id,
        "source_url": link,
        "source_platform": "serpapi_google_events",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "name": event_data.get('title'),
        "description": event_data.get('description', ''),
        "event_day": event_day,  # Will be None if parsing failed
        "start_time": start_time,  # Will be None if parsing failed
        "raw_start_date": raw_start_date,
        "raw_when": raw_when, 
        "end_time": None,  # TODO: Parse end time from raw_when if possible
        "venue": event_data.get('venue', {}).get('name'),
        "address": event_data.get('address', []),
    }

    # --- Enforce max_days_forward filter ---
    if event_day:
        try:
            event_date = datetime.strptime(event_day, "%Y-%m-%d").date()
            today = datetime.now(timezone.utc).date()
            if (event_date - today).days > max_days_forward:
                logger.info(f"Skipping event {event_data.get('title')} on {event_day}: beyond {max_days_forward} days forward.")
                return None
        except Exception as e:
            logger.warning(f"Error checking days forward for event '{event_data.get('title')}': {e}")

    # --- Extract Venue/Location ---
    venue_data = event_data.get("venue", {})
    venue = venue_data.get("name")
    address_list = event_data.get("address", []) # SerpApi often returns address as a list
    address = ", ".join(address_list) if isinstance(address_list, list) else address_list
    
    lat, lng = None, None
    location_info = event_data.get("location_info", {})
    gps_coords = event_data.get("gps_coordinates") or venue_data.get("coordinates") # Check both fields
    if gps_coords and isinstance(gps_coords, dict):
        lat = gps_coords.get("latitude")
        lng = gps_coords.get("longitude")
    # Attempt fallback from link if lat/lng still missing (less reliable)
    # if lat is None and link and "google.com/maps/search/" in link:
        # try: # Example, might need refinement
        #     coords_part = link.split('@')[1].split(',')
        #     lat = float(coords_part[0])
        #     lng = float(coords_part[1])
        # except Exception:
        #     pass # Ignore parsing errors

    # --- Assemble Final Record ---
    # Only 'name' and 'source_id' are strictly critical now for initial ingestion.
    # source_id is derived from link, so check name and link.
    if not event_data.get('title'): # Link check is implicitly handled by source_id check earlier
         logger.warning(f"Skipping event because 'name' is missing. Link: {link}")
         return None

    event_record.update({
        # Location Info
        "venue": venue,
        "address": address, 
        "city": city_info.get("name"),
        "country": city_info.get("country_code"),
        "lat": lat,
        "lng": lng,
    })
    
    logger.debug(f"Successfully parsed event: {source_id} - {event_data.get('title')[:50]}...")
    return event_record

# Removed old if __name__ == '__main__': block 