import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define the search query string
# Note: Double quotes within the string need to be escaped
EVENTS_SEARCH_QUERY = 'bachata OR kizomba OR salsa OR "coast swing" OR chacha OR "cha cha" OR ballroom OR hustle OR "house dance" OR rumba OR samba OR zouk OR lambada OR semba OR cumbia OR foro OR "social dance" OR dancing'

def build_params(city_row: dict[str, str]) -> dict[str, str]:
    """
    Given one row from cities_shortlist.csv return a dict of
    SerpAPI params for the *events* engine.

    Expected keys in city_row: name, country_code, hl, gl
    (latitude and longitude are not used by the events engine with location param)
    """
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise ValueError("SERPAPI_API_KEY not found in environment variables.")

    # Ensure required keys are present
    required_keys = ['name', 'country_code', 'hl', 'gl']
    if not all(key in city_row for key in required_keys):
        missing_keys = [key for key in required_keys if key not in city_row]
        raise ValueError(f"Missing required keys in city_row: {missing_keys}")

    params = {
        "engine": "google_events",
        "q": EVENTS_SEARCH_QUERY,
        "location": f"{city_row['name']}, {city_row['country_code']}", # Use country_code as per shortlist
        "hl": city_row['hl'],
        "gl": city_row['gl'],
        "api_key": api_key,
    }
    return params

# Example usage (assuming you have a sample city row dict):
if __name__ == '__main__':
    # Make sure .env is in the project root or accessible
    # Example row based on cities_shortlist.csv structure
    sample_city = {
        'geonameid': '5128581',
        'name': 'New York City',
        'asciiname': 'New York City',
        'latitude': '40.71427',
        'longitude': '-74.00597',
        'country_code': 'US',
        'population': '8175133',
        'timezone': 'America/New_York',
        'hl': 'en',
        'gl': 'US'
    }
    try:
        event_params = build_params(sample_city)
        print("Generated SerpAPI parameters:")
        print(event_params)
    except ValueError as e:
        print(f"Error: {e}")

    # Example with missing key
    sample_city_missing = {
        'geonameid': '3451190',
        'name': 'Rio de Janeiro',
        # Missing 'country_code', 'hl', 'gl'
    }
    try:
        event_params_missing = build_params(sample_city_missing)
        print("\nGenerated SerpAPI parameters (missing keys):")
        print(event_params_missing)
    except ValueError as e:
        print(f"\nError (missing keys): {e}") 