import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
import requests

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


def get_events_with_unknown_venue():
    response = supabase.table('events').select("id, address, venue").eq("venue", "__VENUE_UNKNOWN__").execute()
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
        "fields": "name,formatted_address",
        "key": GOOGLE_PLACES_API_KEY
    }
    resp = requests.get(DETAILS_API_URL, params=params)
    data = resp.json()
    if data.get("status") == "OK" and data.get("result"):
        return data["result"].get("name"), data["result"].get("formatted_address")
    return None, None

def update_event_venue_and_address(event_id, venue, address):
    response = supabase.table('events').update({"venue": venue, "address": address}).eq("id", event_id).execute()
    return hasattr(response, 'data') and response.data

def main():
    events = get_events_with_unknown_venue()
    logger.info(f"Found {len(events)} events with __VENUE_UNKNOWN__.")
    for event in events:
        event_id = event["id"]
        address = event["address"]
        logger.info(f"Processing event {event_id} with address: {address}")
        place_id = find_venue_from_address(address)
        if not place_id:
            logger.warning(f"No place found for address: {address}")
            continue
        venue, formatted_address = get_place_details(place_id)
        if not venue or not formatted_address:
            logger.warning(f"No details found for place_id: {place_id}")
            continue
        updated = update_event_venue_and_address(event_id, venue, formatted_address)
        if updated:
            logger.info(f"Updated event {event_id}: venue='{venue}', address='{formatted_address}'")
        else:
            logger.error(f"Failed to update event {event_id}")

if __name__ == "__main__":
    main() 