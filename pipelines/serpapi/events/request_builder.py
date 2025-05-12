import os
from dotenv import load_dotenv
from pipelines.serpapi.helpers.uule import uule

# Load environment variables from .env file
load_dotenv()

# Define the search query string
# Note: Double quotes within the string need to be escaped
EVENTS_SEARCH_QUERY = 'bachata OR kizomba OR salsa OR "coast swing" OR chacha OR "cha cha" OR ballroom OR hustle OR "house dance" OR rumba OR samba OR zouk OR lambada OR semba OR cumbia OR foro OR "social dance" OR dancing'

def build_params(city_row: dict[str, str]) -> dict[str, str]:
    """
    Given one row from cities_shortlist.csv return a dict of
    SerpAPI params for the *events* engine.

    Uses the uule parameter generated from city name, lat, lng.
    Expected keys in city_row: name, latitude, longitude, hl, gl
    """
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise ValueError("SERPAPI_API_KEY not found in environment variables.")

    # Ensure required keys for uule generation and other params are present
    # Required keys are now name, latitude, longitude, hl, gl
    required_keys = ['name', 'latitude', 'longitude', 'hl', 'gl']
    if not all(key in city_row and city_row[key] is not None and str(city_row[key]).strip() for key in required_keys):
        missing_keys = [key for key in required_keys if key not in city_row or city_row[key] is None or not str(city_row[key]).strip()]
        city_identifier = city_row.get('name') or city_row.get('geonameid') or 'Unknown'
        raise ValueError(f"Missing required or empty keys ({required_keys}) for uule generation for city: {city_identifier}. Missing: {missing_keys}")

    # Generate the uule string using the helper
    # Note: Using the provided simple uule function which encodes only the city name
    try:
        # Ensure latitude and longitude can be converted to float
        latitude = float(city_row['latitude'])
        longitude = float(city_row['longitude'])
        uule_string = uule(str(city_row['name']), latitude, longitude)
    except ValueError as e:
         city_identifier = city_row.get('name') or city_row.get('geonameid') or 'Unknown'
         raise ValueError(f"Error generating UULE for city {city_identifier}: {e}") from e


    # Construct the parameters dictionary using uule
    params = {
        "engine": "google_events",
        "q": EVENTS_SEARCH_QUERY,
        # "location": location_string, # <-- REMOVED
        "google_domain": "google.com", # Added as per user suggestion
        "gl": str(city_row['gl']),
        "hl": str(city_row['hl']),
        "uule": uule_string, # <-- ADDED uule parameter
        "num": 100, # Max results per page (adjust as needed, 100 is often max for events) - changed from 120
        "api_key": api_key,
    }
    return params

# Example usage (assuming you have a sample city row dict):
if __name__ == '__main__':
    # Make sure .env is in the project root or accessible
    # Example row based on the cities_shortlist.csv structure *BEFORE* serpapi_location_string was added
    # We need name, latitude, longitude, hl, gl
    sample_city_sp = {
        'geonameid': '3451190',
        'name': 'São Paulo',
        'asciiname': 'Sao Paulo',
        'latitude': '-23.5475', # Corrected lat/lon
        'longitude': '-46.6361',
        'country_code': 'BR',
        'admin1_code': '27',
        # 'admin1_name': 'São Paulo', # Not needed for uule
        'population': '10021295',
        'timezone': 'America/Sao_Paulo',
        'hl': 'pt',
        'gl': 'BR',
        # 'serpapi_location_string': 'Sao Paulo,State of Sao Paulo,Brazil' # Not needed
    }
    try:
        event_params_sp = build_params(sample_city_sp)
        print("Generated SerpAPI parameters (São Paulo - UULE):")
        print(event_params_sp)
    except ValueError as e:
        print(f"Error: {e}")

    sample_city_nyc = {
        'geonameid': '5128581',
        'name': 'New York City',
        'asciiname': 'New York City',
        'latitude': '40.71427',
        'longitude': '-74.00597',
        'country_code': 'US',
        'admin1_code': 'NY',
        # 'admin1_name': 'New York', # Not needed for uule
        'population': '8175133',
        'timezone': 'America/New_York',
        'hl': 'en',
        'gl': 'US',
        # 'serpapi_location_string': 'New York,New York,United States' # Not needed
    }
    try:
        event_params_nyc = build_params(sample_city_nyc)
        print("\nGenerated SerpAPI parameters (New York City - UULE):")
        print(event_params_nyc)
    except ValueError as e:
        print(f"Error: {e}")

    # Example with missing required key (e.g., latitude)
    sample_city_missing_key = {
        'geonameid': '12345',
        'name': 'Testville',
        # 'latitude': None, # Missing latitude
        'longitude': '10.0',
        'country_code': 'TC',
        'admin1_code': '01',
        'hl': 'en',
        'gl': 'TC',
    }
    try:
        event_params_missing = build_params(sample_city_missing_key)
        print("\nGenerated SerpAPI parameters (missing key):")
        print(event_params_missing)
    except ValueError as e:
        print(f"\nError (missing key): {e}")

    # Example with invalid latitude
    sample_city_invalid_lat = {
        'geonameid': '67890',
        'name': 'Blankberg',
        'latitude': 'not-a-number', # Invalid latitude
        'longitude': '-20.5',
        'country_code': 'BB',
        'admin1_code': '02',
        'hl': 'en',
        'gl': 'BB',
    }
    try:
        event_params_invalid = build_params(sample_city_invalid_lat)
        print("\nGenerated SerpAPI parameters (invalid latitude):")
        print(event_params_invalid)
    except ValueError as e:
        print(f"\nError (invalid latitude): {e}") 