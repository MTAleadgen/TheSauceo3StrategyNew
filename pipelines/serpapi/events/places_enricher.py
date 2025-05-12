import os
import requests
from dotenv import load_dotenv
from typing import Any, Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
PLACES_TEXTSEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Simple in-memory cache for Place Details results
# Key: place_id, Value: Place Details JSON
places_cache: Dict[str, Dict[str, Any]] = {}

def call_text_search(query: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Calls the Google Places Text Search API.
    Returns the first result if successful, otherwise None.
    """
    params = {
        'query': query,
        'key': api_key,
    }
    try:
        response = requests.get(PLACES_TEXTSEARCH_URL, params=params, timeout=10) # 10 second timeout
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        results = response.json()
        if results.get('status') == 'OK' and results.get('results'):
            return results['results'][0] # Return the first result
        elif results.get('status') in ['ZERO_RESULTS', 'OVER_QUERY_LIMIT', 'REQUEST_DENIED', 'INVALID_REQUEST']:
            logging.warning(f"Text Search API Error: Status {results.get('status')}, Query: {query}")
            return None
        else:
            logging.warning(f"Text Search API Unknown Status: {results.get('status')}, Query: {query}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f'Text Search request failed for query "{query}": {e}')
        return None
    except Exception as e:
        logging.error(f'Error processing Text Search response for query "{query}": {e}')
        return None

def call_place_details(place_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Calls the Google Places Details API.
    Specifies fields to retrieve to manage costs.
    Returns the result if successful, otherwise None.
    """
    # Define the fields you absolutely need to minimize costs
    fields = "place_id,name,formatted_address,geometry/location/lat,geometry/location/lng"
    params = {
        'place_id': place_id,
        'fields': fields,
        'key': api_key,
    }
    try:
        response = requests.get(PLACES_DETAILS_URL, params=params, timeout=10)
        response.raise_for_status()
        results = response.json()
        if results.get('status') == 'OK' and results.get('result'):
            return results['result']
        elif results.get('status') in ['ZERO_RESULTS', 'NOT_FOUND', 'OVER_QUERY_LIMIT', 'REQUEST_DENIED', 'INVALID_REQUEST']:
            logging.warning(f"Place Details API Error: Status {results.get('status')}, Place ID: {place_id}")
            return None
        else:
            logging.warning(f"Place Details API Unknown Status: {results.get('status')}, Place ID: {place_id}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f'Place Details request failed for place ID "{place_id}": {e}')
        return None
    except Exception as e:
        logging.error(f'Error processing Place Details response for place ID "{place_id}": {e}')
        return None

def enrich_with_places(event: dict[str, Any]) -> dict[str, Any]:
    """
    If event['address'] is null/empty OR event lacks lat/lng,
    attempts to enrich the event using Google Places Text Search and Details APIs.
    Uses an in-memory cache for Place Details results.
    """
    if not GOOGLE_PLACES_API_KEY:
        logging.warning("Google Places API key not configured. Skipping enrichment.")
        return event

    # Check if enrichment is needed
    needs_enrichment = not event.get('address') or event.get('lat') is None or event.get('lng') is None

    if not needs_enrichment:
        return event

    # Try to build a meaningful query
    query_parts = [event.get('name'), event.get('venue')] # Add city/country later if available
    query = " ".join(filter(None, query_parts))

    if not query:
        logging.info(f"Cannot build query for Places enrichment (missing name/venue) for event: {event.get('source_id')}")
        return event # Cannot search without a query

    logging.info(f"Attempting Places enrichment for event: {event.get('source_id') or event.get('name')}")

    place_details = None
    text_search_result = call_text_search(query, GOOGLE_PLACES_API_KEY)

    if text_search_result and text_search_result.get('place_id'):
        place_id = text_search_result['place_id']
        
        # Check cache first
        if place_id in places_cache:
            logging.info(f"Using cached Place Details for place_id: {place_id}")
            place_details = places_cache[place_id]
        else:
            logging.info(f"Calling Place Details API for place_id: {place_id}")
            place_details = call_place_details(place_id, GOOGLE_PLACES_API_KEY)
            if place_details:
                places_cache[place_id] = place_details # Store in cache
            else:
                logging.warning(f"Failed to get Place Details for place_id: {place_id}")
    else:
        logging.info(f'Text Search yielded no results for query: "{query}"')

    # Update event if details were found
    if place_details:
        logging.info(f"Enriching event {event.get('source_id') or event.get('name')} with Place Details.")
        # Update address only if currently missing
        if not event.get('address') and place_details.get('formatted_address'):
            event['address'] = place_details['formatted_address']
        
        # Update lat/lng only if currently missing
        location = place_details.get('geometry', {}).get('location')
        if isinstance(location, dict):
            if event.get('lat') is None and location.get('lat') is not None:
                event['lat'] = location['lat']
            if event.get('lng') is None and location.get('lng') is not None:
                event['lng'] = location['lng']
        
        # Optional: Update venue name if it seems more accurate/complete?
        # if place_details.get('name'):
        #     event['venue'] = place_details['name'] # Be careful with overwriting

    return event

# Example usage:
if __name__ == '__main__':
    # Ensure you have a .env file with GOOGLE_PLACES_API_KEY
    if not GOOGLE_PLACES_API_KEY:
        print("Error: GOOGLE_PLACES_API_KEY not found in environment variables.")
        print("Create a .env file with your key to run this example.")
    else:
        print(f"Google Places API Key loaded successfully.")

        # Example event needing enrichment (missing address, lat, lng)
        event_to_enrich = {
            'source_platform': 'serpapi_google_events',
            'source_id': 'test_event_123',
            'name': 'Bachata Social',
            'description': 'Come dance bachata!',
            'venue': 'Empire State Building', # Use a famous landmark for testing
            'address': None,
            'city': None,
            'country': None,
            'lat': None,
            'lng': None,
            'dance_styles': None,
            'price': '$10',
            'start_time': 'Sat, Nov 11, 9 PM',
            'end_time': None,
            'event_day': '2023-11-11',
            'live_band': None,
            'class_before': None
        }

        # Example event not needing enrichment
        event_complete = {
            'source_platform': 'serpapi_google_events',
            'source_id': 'test_event_456',
            'name': 'Salsa Congress',
            'description': 'Annual salsa event.',
            'venue': 'Hotel California',
            'address': '123 Fake St, Los Angeles, CA',
            'city': 'Los Angeles', # Usually not parsed, but for example
            'country': 'US',
            'lat': 34.0522,
            'lng': -118.2437,
            'dance_styles': ['salsa'],
            'price': '$100',
            'start_time': 'Fri, Dec 1, 10 AM',
            'end_time': 'Sun, Dec 3, 5 PM',
            'event_day': '2023-12-01',
            'live_band': True,
            'class_before': True
        }
        
        print("\n--- Enriching Event 1 (Needs Enrichment) ---")
        enriched_event = enrich_with_places(event_to_enrich.copy()) # Use copy to see changes
        print("Original:", event_to_enrich)
        print("Enriched:", enriched_event)

        print("\n--- Enriching Event 2 (Complete) ---")
        not_enriched_event = enrich_with_places(event_complete.copy())
        print("Original:", event_complete)
        print("Enriched:", not_enriched_event)
        
        # Test caching (run enrichment again for the first event)
        print("\n--- Enriching Event 1 Again (Testing Cache) ---")
        enriched_event_again = enrich_with_places(event_to_enrich.copy())
        # Output should indicate cache usage if the first call succeeded
        print("Enriched Again:", enriched_event_again) 