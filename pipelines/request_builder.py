import os
import base64
import time
from dotenv import load_dotenv
# import pyuule # Removed

# Load environment variables from .env file
# Assumes .env is in the parent directory of 'pipelines'
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")

if not SERPAPI_API_KEY:
    raise ValueError("SERPAPI_API_KEY not found in environment variables.")

DEFAULT_PARAMS = {
    "engine": "google_events",
    "api_key": SERPAPI_API_KEY,
    "no_cache": "true",  # Force fetching fresh results
}

def _generate_uule_v2(latitude: float, longitude: float) -> str:
    """Generates a Google UULE v2 string from latitude and longitude."""
    lat_e7 = int(latitude * 10**7)
    lon_e7 = int(longitude * 10**7)
    timestamp_us = int(time.time() * 10**6) # Microseconds

    # Based on https://valentin.app/uule.html Version 2 format
    # Using role:1, producer:12, provenance:6 as defaults seen in examples
    # Using radius:-1 for user-specified exact coordinates
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

    # Base64 encode the ASCII string
    encoded_bytes = base64.b64encode(uule_inner_string.encode('ascii'))
    encoded_string = encoded_bytes.decode('ascii')

    # Prepend 'a+'
    return f"a+{encoded_string}"

def build_serpapi_request(city_data: dict) -> dict:
    """
    Builds the parameter dictionary for a SerpApi Google Events request
    using UULE based on latitude and longitude.

    Args:
        city_data (dict): A dictionary containing city information,
                          must include 'latitude' and 'longitude'.

    Returns:
        dict: The dictionary of parameters for the SerpApi request.
    """
    if not all(k in city_data for k in ['latitude', 'longitude']):
        raise ValueError("City data must include 'latitude' and 'longitude' for UULE generation.")

    lat = float(city_data['latitude'])
    lon = float(city_data['longitude'])

    # Generate UULE string from lat/lon
    uule_string = _generate_uule_v2(latitude=lat, longitude=lon)

    params = DEFAULT_PARAMS.copy()
    params.update({
        "q": "events", # Generic query for events
        "uule": uule_string,
        "hl": city_data.get('hl', 'en'), # Use language code from city data or default to 'en'
        "gl": city_data.get('gl', 'us'), # Use geo location code from city data or default to 'us'
    })

    return params

# Example Usage (for testing)
if __name__ == "__main__":
    # Example for New York City (approximate coordinates)
    nyc_data = {
        'latitude': 40.7128,
        'longitude': -74.0060,
        'hl': 'en',
        'gl': 'us'
    }
    request_params = build_serpapi_request(nyc_data)
    print(f"Generated request params for NYC:\n{request_params}")

    # Example for São Paulo (approximate coordinates)
    sao_paulo_data = {
        'latitude': -23.5505,
        'longitude': -46.6333,
        'hl': 'pt',
        'gl': 'br'
    }
    request_params_sp = build_serpapi_request(sao_paulo_data)
    print(f"\nGenerated request params for São Paulo:\n{request_params_sp}") 