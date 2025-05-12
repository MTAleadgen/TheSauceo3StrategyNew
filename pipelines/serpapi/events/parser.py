from typing import Any, Dict, Optional, List
from datetime import datetime
import re

def parse_datetime_from_string(date_str: Optional[str]) -> Optional[datetime]:
    """
    Attempts to parse a datetime object from various possible string formats
    found in SerpAPI event results.
    """
    if not date_str:
        return None
    # Common formats seen in SerpAPI: "October 16", "Oct 16", "2023-10-16"
    # This is a simplified parser; more robust parsing might be needed.
    # It will often lack year or precise time, so it's a starting point.
    # For now, we primarily aim to get the date part for 'event_day'.
    # Full timestamp conversion would require more context or assumptions.
    try:
        # Attempt to parse "Month Day" (e.g., "October 16")
        return datetime.strptime(date_str, "%B %d")
    except ValueError:
        pass
    try:
        # Attempt to parse "Mon Day" (e.g., "Oct 16")
        return datetime.strptime(date_str, "%b %d")
    except ValueError:
        pass
    try:
        # Attempt to parse "YYYY-MM-DD"
        return datetime.strptime(date_str.split('T')[0], "%Y-%m-%d") # Handle ISO date part
    except ValueError:
        pass
    # Add more parsing attempts if other formats are common
    # print(f"Warning: Could not parse date string: {date_str}")
    return None

def extract_lat_lng_from_link(link: Optional[str]) -> Optional[Dict[str, float]]:
    """
    Extracts latitude and longitude from a Google Maps link if present.
    Example link: "https://maps.google.com/maps?q=..." or "https://www.google.com/maps/search/?api=1&query=lat,lng"
    """
    if not link:
        return None
    # Regex for "query=lat,lng" or "@lat,lng"
    # Need to escape backslashes in the regex string itself
    match = re.search(r"query=([-+]?\d*\.?\d+),([-+]?\d*\.?\d+)", link)
    if not match:
        match = re.search(r"@([-+]?\d*\.?\d+),([-+]?\d*\.?\d+)", link)

    if match and len(match.groups()) == 2:
        try:
            return {"latitude": float(match.group(1)), "longitude": float(match.group(2))}
        except ValueError:
            return None
    return None

def parse_event_result(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Take one element from SerpAPI events_results
    Return dict matching columns in the events table.
    """
    parsed_data: Dict[str, Any] = {}

    parsed_data['source_platform'] = 'serpapi_google_events'
    parsed_data['source_id'] = event.get('event_id')
    parsed_data['name'] = event.get('title')
    parsed_data['description'] = event.get('description')

    venue_info = event.get('venue', {})
    parsed_data['venue'] = venue_info.get('name') if isinstance(venue_info, dict) else None

    address_list = event.get('address')
    if isinstance(address_list, list):
        parsed_data['address'] = ', '.join(filter(None, address_list))
    elif isinstance(address_list, str):
        parsed_data['address'] = address_list
    else:
        parsed_data['address'] = None
    
    # City and Country are not directly available in each event item usually.
    # They are part of the search query. For now, setting to None.
    # Could potentially try to parse from address or venue if available and reliable.
    parsed_data['city'] = None 
    parsed_data['country'] = None

    gps_coords = event.get('gps_coordinates')
    event_location_map = event.get('event_location_map', {})
    
    parsed_data['lat'] = None
    parsed_data['lng'] = None
    if isinstance(gps_coords, dict) and gps_coords.get('latitude') and gps_coords.get('longitude'):
        parsed_data['lat'] = gps_coords.get('latitude')
        parsed_data['lng'] = gps_coords.get('longitude')
    elif isinstance(event_location_map, dict) and event_location_map.get('link'):
        coords_from_link = extract_lat_lng_from_link(event_location_map.get('link'))
        if coords_from_link:
            parsed_data['lat'] = coords_from_link.get('latitude')
            parsed_data['lng'] = coords_from_link.get('longitude')

    # Dance styles would likely need to be inferred from title/description
    # or by matching keywords, which is beyond simple parsing.
    parsed_data['dance_styles'] = None 

    ticket_info_list = event.get('ticket_info')
    parsed_data['price'] = None
    if isinstance(ticket_info_list, list) and len(ticket_info_list) > 0:
        # Assuming the first ticket info is most relevant
        first_ticket = ticket_info_list[0]
        if isinstance(first_ticket, dict):
            # Price extraction is highly dependent on SerpAPI's structure for ticket_info
            # This is a placeholder; more sophisticated parsing may be needed.
            parsed_data['price'] = first_ticket.get('link_text') or first_ticket.get('summary')

    date_details = event.get('date', {})
    start_date_str = date_details.get('start_date') # E.g., "October 16"
    when_str = date_details.get('when')           # E.g., "Tue, Oct 17, 7 – 10 PM" or "Tomorrow"

    # Prioritize 'when' for more detail, but fall back to 'start_date'
    # Storing raw string for start_time as conversion is complex
    parsed_start_time_str = when_str if when_str else start_date_str
    parsed_data['start_time'] = parsed_start_time_str 
    
    # Derive event_day (as date) from the parsed start_time string if possible.
    parsed_data['event_day'] = None
    dt_object_for_day = parse_datetime_from_string(start_date_str)
    if dt_object_for_day:
        # If year is missing (e.g. from "October 16"), assume current year.
        if dt_object_for_day.year == 1900: # Default year from strptime if not provided
             try:
                 # Ensure the date exists in the target year (handles leap years)
                 dt_object_for_day = dt_object_for_day.replace(year=datetime.now().year)
             except ValueError:
                 # Handle cases like Feb 29 in a non-leap year if needed
                 # For simplicity, we might just skip or use the previous year
                 pass # Or log a warning
        if dt_object_for_day.year != 1900: # Check again if year replacement was successful
            parsed_data['event_day'] = dt_object_for_day.date().isoformat()

    # Fallback if start_date parsing fails or is missing, try from 'when'
    # This part is complex due to varied 'when' formats and requires more robust parsing.
    # E.g., "Tue, Oct 17, 7 – 10 PM" -> needs parsing.
    # Currently, event_day remains None if start_date is not parsable.

    parsed_data['end_time'] = None # Extracting a specific end_time timestamp is complex.
    
    # Fields not directly mappable from typical SerpAPI event results:
    parsed_data['rewritten_description'] = None
    parsed_data['live_band'] = None
    parsed_data['class_before'] = None
    
    # Ensure all keys from the schema are present, defaulting to None
    db_columns = [
        'source_platform', 'source_id', 'name', 'description', 'venue', 
        'address', 'city', 'country', 'lat', 'lng', 'dance_styles', 'price',
        'start_time', 'end_time', 'event_day', 'live_band', 'class_before'
    ]
    final_parsed_data = {col: parsed_data.get(col) for col in db_columns}
            
    return final_parsed_data

# Example usage (you'll need a sample SerpAPI event dict):
if __name__ == '__main__':
    sample_serpapi_event = {
        "title": "Salsa Night Downtown",
        "date": {"start_date": "October 25", "when": "Wed, Oct 25, 8 PM – 11 PM"},
        "address": ["123 Main St", "Downtown, Anytown"],
        "link": "https://example.com/salsa-night",
        "gps_coordinates": {"latitude": 34.0522, "longitude": -118.2437},
        "event_location_map": {
            "image": "https://maps.google.com/maps/api/staticmap?...&center=34.0522,-118.2437...",
            "link": "https://maps.google.com/maps?q=Salsa+Night+Downtown+123+Main+St+Downtown,+Anytown&ll=34.0522,-118.2437&z=15",
            "serpapi_link": "https://serpapi.com/search.json?engine=google_maps..."
        },
        "description": "Join us for a fun night of salsa dancing!",
        "event_id": "serp_event_12345",
        "venue": {"name": "The Dance Hall", "rating": 4.5, "reviews": 120},
        "ticket_info": [{"link_text": "$15 Online"}, {"link_text": "$20 At Door"}],
        # ... other fields
    }
    
    sample_serpapi_event_minimal = {
        "title": "Kizomba Workshop",
        "date": {"start_date": "Nov 5"}, # Year missing
        "address": ["Studio Z"],
        "link": "https://example.com/kizomba-workshop",
        "event_id": "serp_event_67890",
        "description": "Learn Kizomba basics."
        # venue, gps_coordinates, ticket_info might be missing
    }

    parsed_event = parse_event_result(sample_serpapi_event)
    print("Parsed Event (Full):")
    for key, value in parsed_event.items():
        print(f"  {key}: {value} (Type: {type(value).__name__})")

    parsed_event_minimal = parse_event_result(sample_serpapi_event_minimal)
    print("\nParsed Event (Minimal):")
    for key, value in parsed_event_minimal.items():
        print(f"  {key}: {value} (Type: {type(value).__name__})")

    # Example for lat/lng extraction from link
    link_with_coords1 = "https://www.google.com/maps/search/?api=1&query=40.712776,-74.005974"
    link_with_coords2 = "https://maps.google.com/maps?ll=34.052235,-118.243683&z=15&q=Dodger+Stadium" # q doesn't have coords
    link_with_coords3 = "https://www.google.com/maps/@34.052235,-118.243683,15z"
    print(f"\nCoords from link1: {extract_lat_lng_from_link(link_with_coords1)}")
    print(f"Coords from link2: {extract_lat_lng_from_link(link_with_coords2)}") # Should be None
    print(f"Coords from link3: {extract_lat_lng_from_link(link_with_coords3)}")
    
    # Example for date parsing
    print(f"\nDate parse ('October 16'): {parse_datetime_from_string('October 16')}")
    print(f"Date parse ('Oct 16'): {parse_datetime_from_string('Oct 16')}")
    print(f"Date parse ('2024-07-22'): {parse_datetime_from_string('2024-07-22')}")
    print(f"Date parse ('Invalid Date'): {parse_datetime_from_string('Invalid Date')}")
    print(f"Date parse ('Feb 29'): {parse_datetime_from_string('Feb 29')}") # Test Feb 29 