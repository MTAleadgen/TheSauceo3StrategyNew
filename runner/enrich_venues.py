import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
import requests
import json

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not GOOGLE_PLACES_API_KEY:
    print("Missing SUPABASE_URL, SUPABASE_KEY, or GOOGLE_PLACES_API_KEY in environment.")
    sys.exit(1)

# Use SUPABASE_URL for both Supabase client and psycopg2 connection
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Google Places API endpoint
PLACES_API_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
DETAILS_API_URL = "https://maps.googleapis.com/maps/api/place/details/json"

def get_cached_place(query):
    response = supabase.table("places_cache").select("*").eq("query", query).execute()
    return response.data[0] if hasattr(response, 'data') and response.data else None

def cache_place(place_id, query, venue, address, lat, lng, types, business_status):
    supabase.table("places_cache").upsert({
        "place_id": place_id,
        "query": query,
        "venue": venue,
        "address": address,
        "lat": lat,
        "lng": lng,
        "types": types,
        "business_status": business_status,
        "cached_at": "now()"
    }).execute()

def get_all_events():
    response = supabase.table('events').select("id, address, venue").execute()
    return response.data if hasattr(response, 'data') else []

def find_venue_from_address(address):
    params = {
        "input": address,
        "inputtype": "textquery",
        "fields": "place_id",
        "key": GOOGLE_PLACES_API_KEY
    }
    resp = requests.get(PLACES_API_URL, params=params)
    data = resp.json()
    if data.get("status") == "OK" and data.get("candidates"):
        return data["candidates"][0]["place_id"]
    return None

def get_place_details(place_id):
    params = {
        "place_id": place_id,
        "fields": "place_id,name,formatted_address,geometry,types,business_status",
        "key": GOOGLE_PLACES_API_KEY
    }
    resp = requests.get(DETAILS_API_URL, params=params)
    data = resp.json()
    if data.get("status") == "OK" and data.get("result"):
        result = data["result"]
        logger.info(f"Google API result for place_id {place_id}: {json.dumps(result)}")
        return {
            "place_id": result.get("place_id"),
            "venue": result.get("name"),
            "address": result.get("formatted_address"),
            "lat": (result.get("geometry") or {}).get("location", {}).get("lat"),
            "lng": (result.get("geometry") or {}).get("location", {}).get("lng"),
            "types": result.get("types"),
            "business_status": result.get("business_status"),
        }
    return None

def update_event_venue_and_address(event_id, venue, address, lat, lng):
    update_data = {
        "venue": venue,
        "address": address
    }
    # Only include lat and lng in the update if they are not None
    if lat is not None and lng is not None:
        update_data["lat"] = lat
        update_data["lng"] = lng
    
    response = supabase.table('events').update(update_data).eq("id", event_id).execute()
    return hasattr(response, 'data') and response.data

def is_missing_venue(venue):
    return not venue or str(venue).strip() == "" or venue == "__VENUE_UNKNOWN__"

def main():
    events = get_all_events()
    logger.info(f"Found {len(events)} events to process.")
    for event in events:
        event_id = event["id"]
        original_serp_address = event["address"]
        event_venue = event["venue"]

        logger.info(f"Processing event {event_id}: Venue='{event_venue}', Address='{original_serp_address}'")

        if not original_serp_address:
            logger.warning(f"Skipping event {event_id} due to missing address.")
            continue

        # 1. Check places_cache first
        cached = get_cached_place(original_serp_address)
        if cached:
            logger.info(f"Cache hit for address: {original_serp_address}")
            final_venue = cached["venue"] if event_venue == "__VENUE_UNKNOWN__" else event_venue
            updated = update_event_venue_and_address(event_id, final_venue, cached["address"], cached["lat"], cached["lng"])
            if updated:
                logger.info(f"Updated event {event_id} from cache: venue='{final_venue}', address='{cached['address']}', lat={cached['lat']}, lng={cached['lng']}")
            else:
                logger.error(f"Failed to update event {event_id} from cache")
            continue

        # 2. Not in cache, call Google Places API
        logger.info(f"Cache miss for address: {original_serp_address}. Calling Google Places API.")
        place_id = find_venue_from_address(original_serp_address)
        if not place_id:
            logger.warning(f"No place_id found via Google Find Place for address: {original_serp_address}")
            continue
        details = get_place_details(place_id)
        if not details:
            logger.warning(f"No details found via Google Place Details for place_id: {place_id} (address: {original_serp_address})")
            continue

        # 3. Decide on final venue
        google_venue = details["venue"]
        final_venue = google_venue if is_missing_venue(event_venue) else event_venue
        google_formatted_address = details["address"]
        google_lat = details["lat"]
        google_lng = details["lng"]

        # 4. Cache the result
        cache_place(
            details["place_id"],
            original_serp_address,
            final_venue,
            google_formatted_address,
            google_lat,
            google_lng,
            details["types"],
            details["business_status"]
        )
        logger.info(f"Cached place for query='{original_serp_address}': venue='{final_venue}', address='{google_formatted_address}'")

        # 5. Update the event
        updated = update_event_venue_and_address(
            event_id,
            final_venue,
            google_formatted_address,
            google_lat,
            google_lng
        )
        if updated:
            logger.info(f"Updated event {event_id} with API details: venue='{final_venue}', address='{google_formatted_address}', lat={google_lat}, lng={google_lng}")
        else:
            logger.error(f"Failed to update event {event_id} with API details")

if __name__ == "__main__":
    main() 