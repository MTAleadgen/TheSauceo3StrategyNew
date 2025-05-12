import os
from dotenv import load_dotenv
import logging # Add logging

# Attempt to import pyuule, provide guidance if missing
try:
    import pyuule
except ImportError:
    logging.error("The 'pyuule' library is required but not installed. Please install it: pip install pyuule")
    # Optionally re-raise or exit if pyuule is absolutely critical
    # raise ImportError("pyuule library not found. Please install it.") 
    pyuule = None # Set to None so later checks fail gracefully

# Load environment variables from .env file
load_dotenv()

# Define the search query string
# Note: Double quotes within the string need to be escaped
EVENTS_SEARCH_QUERY = 'bachata OR kizomba OR salsa OR "coast swing" OR chacha OR "cha cha" OR ballroom OR hustle OR "house dance" OR rumba OR samba OR zouk OR lambada OR semba OR cumbia OR foro OR "social dance" OR dancing'

def build_params(city_row: dict[str, str]) -> dict[str, str]:
    """
    Given one row from cities_shortlist.csv return a dict of base
    SerpAPI params for the *events* engine, using uule for location.

    Pagination params (start, num) will be added later by the runner.
    Expected keys in city_row: name, latitude, longitude, hl, gl
    """
    if pyuule is None:
         raise RuntimeError("pyuule library is not available. Cannot generate UULE parameter.")
         
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise ValueError("SERPAPI_API_KEY not found in environment variables.")

    # Ensure required keys for uule generation and other params are present
    required_keys = ['name', 'latitude', 'longitude', 'hl', 'gl']
    if not all(key in city_row and city_row[key] is not None and str(city_row[key]).strip() for key in required_keys):
        missing_keys = [key for key in required_keys if key not in city_row or city_row[key] is None or not str(city_row[key]).strip()]
        city_identifier = city_row.get('name') or city_row.get('geonameid') or 'Unknown'
        raise ValueError(f"Missing required or empty keys ({required_keys}) for uule generation for city: {city_identifier}. Missing: {missing_keys}")

    # Generate the uule string using the pyuule library
    try:
        latitude = float(city_row['latitude'])
        longitude = float(city_row['longitude'])
        # Generate UULE string based on lat/lon
        # Prepends 'w+' as required by SerpApi documentation for uule parameter
        uule_string = "w+" + pyuule.encode(latitude=latitude, longitude=longitude) 
        # Note: pyuule.encode defaults should be reasonable (role=2, producer=12, provenance=6)
        # Alternative from user hint: pyuule.generate(latitude, longitude, accuracy='city') - Need to check pyuule API
        # Stick with encode for now as it seems standard. Accuracy might be handled by Google based on lat/lon.
        
    except ValueError as e:
         city_identifier = city_row.get('name') or city_row.get('geonameid') or 'Unknown'
         raise ValueError(f"Error converting lat/lon to float for city {city_identifier}: {e}") from e
    except Exception as e:
        city_identifier = city_row.get('name') or city_row.get('geonameid') or 'Unknown'
        raise ValueError(f"Error generating UULE with pyuule for city {city_identifier}: {e}") from e

    # Construct the base parameters dictionary (start/num added later)
    params = {
        "engine": "google_events",
        "q": EVENTS_SEARCH_QUERY,
        "google_domain": "google.com",
        "gl": str(city_row['gl']),
        "hl": str(city_row['hl']),
        "uule": uule_string,
        # "num": 100, # REMOVED - will be set per page by runner
        "api_key": api_key,
    }
    return params

# Example usage (assuming you have a sample city row dict):
if __name__ == '__main__':
    if pyuule is None:
        print("Cannot run example: pyuule library not installed.")
    else:
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
            print("Generated SerpAPI base parameters (São Paulo - pyuule):")
            print(event_params_sp)
        except (ValueError, RuntimeError) as e:
            print(f"Error (São Paulo): {e}")

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
            print("\nGenerated SerpAPI base parameters (New York City - pyuule):")
            print(event_params_nyc)
        except (ValueError, RuntimeError) as e:
            print(f"Error (New York City): {e}")

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
            print("\nGenerated SerpAPI base parameters (missing key):")
            print(event_params_missing)
        except (ValueError, RuntimeError) as e:
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
            print("\nGenerated SerpAPI base parameters (invalid latitude):")
            print(event_params_invalid)
        except (ValueError, RuntimeError) as e:
            print(f"\nError (invalid latitude): {e}") 