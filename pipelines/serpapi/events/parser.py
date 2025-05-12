from typing import Any, Dict, Optional
from datetime import datetime, timezone, timedelta
import hashlib
import logging

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

def parse_event_result(raw: dict[str, Any], days_forward: int) -> Optional[dict[str, Any]]:
    """
    Map ONE SerpAPI events_result item -> DB row.
    Filters events older than `days_forward`.
    Generates source_id from the event link.
    Returns None if the event is filtered out or missing critical fields.
    """
    if dateutil_parser is None:
        raise RuntimeError("python-dateutil library is not available. Cannot parse dates.")

    # --- Extract Basic Fields ---
    name = raw.get("title")
    description = raw.get("description")
    link = raw.get("link")
    # Use link to generate source_id
    source_id = generate_deterministic_id(link)
    
    # Basic validation - need at least name and a source_id (derived from link)
    if not name or not source_id:
        logger.warning(f"Skipping event due to missing title ('{name}') or link (required for source_id). Link: {link}")
        return None

    # --- Extract Venue/Location ---
    venue_data = raw.get("venue", {})
    venue = venue_data.get("name")
    address_list = raw.get("address", []) # SerpApi often returns address as a list
    address = ", ".join(address_list) if isinstance(address_list, list) else address_list
    
    lat, lng = None, None
    location_info = raw.get("location_info", {})
    gps_coords = raw.get("gps_coordinates") or venue_data.get("coordinates") # Check both fields
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

    # --- Extract and Filter Date/Time ---
    date_info = raw.get("date", {})
    start_date_str = date_info.get("start_date")
    when = date_info.get("when") # Often includes time details

    start_dt_utc = None
    event_day = None
    end_dt_utc = None # Placeholder for potential end date parsing

    if start_date_str:
        try:
            # Use dateutil.parser for flexibility (handles various formats)
            # Combine start_date with 'when' if 'when' seems to contain time info
            full_date_str = start_date_str
            if when and ("-" in when or ":" in when or "AM" in when.upper() or "PM" in when.upper()):
                # Basic check if 'when' looks like it includes time
                 # Combine intelligently, avoiding duplicate date parts if possible
                if start_date_str not in when: 
                     full_date_str = f"{start_date_str} {when}"
                else:
                    full_date_str = when # Assume 'when' is more complete
            
            # Parse the combined string or just start_date
            # fuzzy=True helps with slightly malformed strings but can be risky
            dt_obj = dateutil_parser.parse(full_date_str, fuzzy=False) 
            
            # Convert to UTC
            if dt_obj.tzinfo is None:
                # If naive, *assume* it's local time for the event. 
                # THIS IS AN ASSUMPTION - SerpApi doesn't always provide timezone.
                # For filtering, comparing naive datetime to UTC cutoff might be okay
                # but storing naive time is problematic. Let's convert to UTC
                # *after* filtering using today's UTC cutoff.
                 start_dt_local = dt_obj # Keep local for now for filtering logic clarity
            else:
                # Convert timezone-aware datetime to UTC
                start_dt_local = dt_obj # Still consider this local time relative to event location
                start_dt_utc = dt_obj.astimezone(timezone.utc)

            # --- Date Filtering --- 
            # Compare using timezone-naive approach against UTC cutoff
            # This assumes event start times are generally comparable to UTC cutoff date
            # without complex local timezone lookups, which we don't have.
            now_utc = datetime.now(timezone.utc)
            cutoff_utc = now_utc + timedelta(days=days_forward)
            
            # Compare date parts only or full datetime? Let's use full datetime.
            if start_dt_local.replace(tzinfo=None) > cutoff_utc.replace(tzinfo=None):
                logger.debug(f"Skipping event '{name}' starting {start_date_str} (parsed: {start_dt_local}) - exceeds {days_forward}-day cutoff ({cutoff_utc.date()})")
                return None # Event is too far in the future

            # If passed filter, ensure we have a UTC datetime for storage
            if start_dt_utc is None: # If it was naive, convert assuming some default or error out
                 # We lack original timezone. Cannot accurately convert naive to UTC.
                 # Option 1: Skip event (safer)
                 # logger.warning(f"Skipping event '{name}' starting {start_date_str} due to naive datetime and inability to determine timezone.")
                 # return None 
                 # Option 2: Assume UTC (potentially incorrect time of day)
                 logger.warning(f"Assuming UTC for naive start time {start_dt_local} for event '{name}'")
                 start_dt_utc = start_dt_local.replace(tzinfo=timezone.utc)
                 
            event_day = start_dt_utc.date() # Derive event_day from the UTC datetime

        except Exception as e:
            logger.warning(f"Could not parse date ('{start_date_str}', 'when': '{when}') for event '{name}': {e}")
            # If date parsing fails, we can't filter or set event_day, skip event
            return None 
    else:
         logger.warning(f"Skipping event '{name}' due to missing start_date.")
         return None # Cannot process without a start date

    # --- Assemble Final Record ---
    # Ensure critical fields for DB are present (adjust based on schema constraints)
    if not all([event_day, venue, name]): # Check based on new upsert columns
         logger.warning(f"Skipping event '{name}' (ID: {source_id}) due to missing critical field for upsert: event_day='{event_day}', venue='{venue}', name='{name}'")
         return None

    record = {
        # Identifiers
        "source_id": source_id,
        "source_url": link,
        "retrieved_at": datetime.now(timezone.utc).isoformat(), # Add retrieval timestamp

        # Core Event Info
        "name": name,
        "description": description,
        "event_day": event_day.isoformat() if event_day else None,
        "start_time": start_dt_utc.isoformat() if start_dt_utc else None,
        "end_time": end_dt_utc.isoformat() if end_dt_utc else None, # Include if end date is parsed

        # Location Info
        "venue": venue,
        "address": address, 
        "latitude": lat,
        "longitude": lng,

        # Add raw source data for debugging/future use (optional)
        # "raw_source_data": raw 
    }
    
    # Remove keys with None values before returning? Optional.
    # record = {k: v for k, v in record.items() if v is not None}

    logger.debug(f"Successfully parsed event: {source_id} - {name[:50]}...")
    return record

# Removed old if __name__ == '__main__': block 