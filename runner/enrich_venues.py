import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not GOOGLE_PLACES_API_KEY:
    print("Missing SUPABASE_URL, SUPABASE_KEY, or GOOGLE_PLACES_API_KEY in environment.")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Google Places API endpoint
PLACES_API_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
DETAILS_API_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Add DB connection for cache
PG_CONN = psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor)
PG_CONN.autocommit = True

def get_cached_place(query):
    with PG_CONN.cursor() as cur:
        cur.execute("SELECT * FROM places_cache WHERE query = %s", (query,))
        return cur.fetchone()

def cache_place(place_id, query, venue, address, lat, lng, types, business_status):
    with PG_CONN.cursor() as cur:
        cur.execute("""
            INSERT INTO places_cache (place_id, query, venue, address, lat, lng, types, business_status, cached_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (place_id) DO UPDATE SET
                query = EXCLUDED.query,
                venue = EXCLUDED.venue,
                address = EXCLUDED.address,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                types = EXCLUDED.types,
                business_status = EXCLUDED.business_status,
                cached_at = now()
        """, (place_id, query, venue, address, lat, lng, types, business_status))

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
    response = supabase.table('events').update({"venue": venue, "address": address, "lat": lat, "lng": lng}).eq("id", event_id).execute()
    return hasattr(response, 'data') and response.data

def main():
    events = get_all_events()
    logger.info(f"Found {len(events)} events to process.")
    for event in events:
        event_id = event["id"]
        address = event["address"]
        logger.info(f"Processing event {event_id} with address: {address}")
        cached = get_cached_place(address)
        if cached:
            logger.info(f"Cache hit for address: {address}")
            updated = update_event_venue_and_address(event_id, cached["venue"], cached["address"], cached["lat"], cached["lng"])
            if updated:
                logger.info(f"Updated event {event_id} from cache: venue='{cached['venue']}', address='{cached['address']}', lat={cached['lat']}, lng={cached['lng']}")
            else:
                logger.error(f"Failed to update event {event_id} from cache")
            continue
        place_id = find_venue_from_address(address)
        if not place_id:
            logger.warning(f"No place found for address: {address}")
            continue
        details = get_place_details(place_id)
        if not details:
            logger.warning(f"No details found for place_id: {place_id}")
            continue
        cache_place(details["place_id"], address, details["venue"], details["address"], details["lat"], details["lng"], details["types"], details["business_status"])
        updated = update_event_venue_and_address(event_id, details["venue"], details["address"], details["lat"], details["lng"])
        if updated:
            logger.info(f"Updated event {event_id}: venue='{details['venue']}', address='{details['address']}', lat={details['lat']}, lng={details['lng']}")
        else:
            logger.error(f"Failed to update event {event_id}")

if __name__ == "__main__":
    main() 