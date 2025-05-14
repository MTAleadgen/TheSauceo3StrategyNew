import os
import base64
import time
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
# Use load_dotenv() without path assuming .env is in the root or handled by execution environment
load_dotenv()

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")

if not SERPAPI_API_KEY:
    raise ValueError("SERPAPI_API_KEY not found in environment variables.")

# Define the search query string
# Note: Double quotes within the string need to be escaped
EVENTS_SEARCH_QUERY = 'bachata OR kizomba OR salsa OR "coast swing" OR chacha OR "cha cha" OR ballroom OR hustle OR "house dance" OR rumba OR samba OR zouk OR lambada OR semba OR cumbia OR forro OR "social dance" OR dancing OR dance'

DEFAULT_PARAMS = {
    "engine": "google_events",
    "api_key": SERPAPI_API_KEY,
    "q": EVENTS_SEARCH_QUERY,
    "no_cache": "true",  # Force fetching fresh results
    "google_domain": "google.com", # Keep google_domain default
}

def _generate_uule_v2(latitude: float, longitude: float) -> str:
    """Generates a Google UULE v2 string from latitude and longitude."""
    lat_e7 = int(latitude * 10**7)
    lon_e7 = int(longitude * 10**7)
    timestamp_us = int(time.time() * 10**6) # Microseconds

    uule_inner_string = (
        f"role:1\n"
        f"producer:12\n"
        f"provenance:6\n"
        f"timestamp:{timestamp_us}\n"
        f"latlng{{\n"
        f"latitude_e7:{lat_e7}\n"
        f"longitude_e7:{lon_e7}\n"
        f"}}\n"
        f"radius:-1"
    )
    encoded_bytes = base64.b64encode(uule_inner_string.encode('ascii'))
    encoded_string = encoded_bytes.decode('ascii')
    return f"a+{encoded_string}"

def build_params(city_row: dict) -> dict:
    """
    Builds the parameter dictionary for a SerpApi Google Events request
    using UULE based on latitude and longitude.

    Args:
        city_row (dict): A dictionary containing city information,
                          must include 'latitude', 'longitude', 'hl', 'gl'.

    Returns:
        dict: The dictionary of parameters for the SerpApi request.
    """
    required_keys = ['latitude', 'longitude', 'hl', 'gl']
    if not all(k in city_row and city_row[k] is not None and str(city_row[k]).strip() for k in required_keys):
        missing_keys = [key for key in required_keys if key not in city_row or city_row[key] is None or not str(city_row[key]).strip()]
        city_identifier = city_row.get('name') or city_row.get('geonameid') or 'Unknown'
        raise ValueError(f"Missing required or empty keys ({required_keys}) for request build for city: {city_identifier}. Missing: {missing_keys}")

    try:
        lat = float(city_row['latitude'])
        lon = float(city_row['longitude'])
    except ValueError as e:
        city_identifier = city_row.get('name') or city_row.get('geonameid') or 'Unknown'
        raise ValueError(f"Error converting lat/lon to float for city {city_identifier}: {e}") from e

    # Generate UULE string from lat/lon using our new function
    uule_string = _generate_uule_v2(latitude=lat, longitude=lon)

    params = DEFAULT_PARAMS.copy()
    params.update({
        "uule": uule_string,
        "hl": str(city_row['hl']),
        "gl": str(city_row['gl']),
        # "q" and "api_key" are already in DEFAULT_PARAMS
        # start/num will be added by the runner for pagination
    })

    return params

# Example Usage (for testing)
if __name__ == "__main__":
    # Example for New York City (approximate coordinates)
    nyc_data = {
        'name': 'New York City',
        'latitude': 40.7128,
        'longitude': -74.0060,
        'hl': 'en',
        'gl': 'us'
    }
    try:
        request_params = build_params(nyc_data)
        print(f"Generated request params for NYC:\n{request_params}")
    except ValueError as e:
        print(f"Error (NYC): {e}")

    # Example for São Paulo (approximate coordinates)
    sao_paulo_data = {
        'name': 'São Paulo',
        'latitude': -23.5505,
        'longitude': -46.6333,
        'hl': 'pt',
        'gl': 'br'
    }
    try:
        request_params_sp = build_params(sao_paulo_data)
        print(f"\nGenerated request params for São Paulo:\n{request_params_sp}")
    except ValueError as e:
        print(f"Error (São Paulo): {e}") 