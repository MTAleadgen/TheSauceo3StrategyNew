import pandas as pd
# from pathlib import Path # Not used anymore
# import csv # Not used anymore
import os
import requests # Keep for downloading cities/admin files
import logging
# import time # No longer needed for SerpApi rate limiting
from dotenv import load_dotenv

# Load environment variables from .env file (still useful if other env vars are used)
load_dotenv()

# Define North and South American ISO country codes (using UN geoscheme primarily)
NA_SA_CODES = {
    # North America
    'AG', 'BS', 'BB', 'BZ', 'CA', 'CR', 'CU', 'DM', 'DO', 'SV', 'GD', 'GT', 'HT', 'HN', 'JM', 'MX', 'NI', 'PA', 'KN', 'LC', 'VC', 'TT', 'US',
    # North American dependencies/territories
    'AI', 'BM', 'VG', 'KY', 'GL', 'GP', 'MQ', 'MS', 'PR', 'BL', 'MF', 'PM', 'SX', 'TC', 'VI',
    # South America
    'AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'FK', 'GF', 'GY', 'PY', 'PE', 'SR', 'UY', 'VE',
    # South American dependencies/territories
    'BQ'
}

# Define default hl/gl settings per country
COUNTRY_SETTINGS = {
    'US': {'hl': 'en', 'gl': 'US'}, 'CA': {'hl': 'en', 'gl': 'CA'}, 'MX': {'hl': 'es', 'gl': 'MX'},
    'BR': {'hl': 'pt', 'gl': 'BR'}, 'AR': {'hl': 'es', 'gl': 'AR'}, 'CL': {'hl': 'es', 'gl': 'CL'},
    'CO': {'hl': 'es', 'gl': 'CO'}, 'PE': {'hl': 'es', 'gl': 'PE'}, 'VE': {'hl': 'es', 'gl': 'VE'},
    'EC': {'hl': 'es', 'gl': 'EC'}, 'GT': {'hl': 'es', 'gl': 'GT'}, 'CU': {'hl': 'es', 'gl': 'CU'},
    'HT': {'hl': 'fr', 'gl': 'HT'}, 'BO': {'hl': 'es', 'gl': 'BO'}, 'DO': {'hl': 'es', 'gl': 'DO'},
    'HN': {'hl': 'es', 'gl': 'HN'}, 'PY': {'hl': 'es', 'gl': 'PY'}, 'NI': {'hl': 'es', 'gl': 'NI'},
    'SV': {'hl': 'es', 'gl': 'SV'}, 'CR': {'hl': 'es', 'gl': 'CR'}, 'PA': {'hl': 'es', 'gl': 'PA'},
    'PR': {'hl': 'es', 'gl': 'PR'}, 'UY': {'hl': 'es', 'gl': 'UY'}, 'JM': {'hl': 'en', 'gl': 'JM'},
    'TT': {'hl': 'en', 'gl': 'TT'}, 'BZ': {'hl': 'en', 'gl': 'BZ'}, 'GY': {'hl': 'en', 'gl': 'GY'},
    'SR': {'hl': 'nl', 'gl': 'SR'}, 'GP': {'hl': 'fr', 'gl': 'GP'}, 'MQ': {'hl': 'fr', 'gl': 'MQ'},
    'GF': {'hl': 'fr', 'gl': 'GF'}, 'GL': {'hl': 'en', 'gl': 'GL'}, 'PM': {'hl': 'fr', 'gl': 'PM'},
    # Add reasonable defaults for others if needed, using country code as gl
    'AI': {'hl': 'en', 'gl': 'AI'}, 'AG': {'hl': 'en', 'gl': 'AG'}, 'AW': {'hl': 'nl', 'gl': 'AW'}, # Added AW
    'BS': {'hl': 'en', 'gl': 'BS'}, 'BB': {'hl': 'en', 'gl': 'BB'}, 'BM': {'hl': 'en', 'gl': 'BM'},
    'VG': {'hl': 'en', 'gl': 'VG'}, 'KY': {'hl': 'en', 'gl': 'KY'}, 'CW': {'hl': 'nl', 'gl': 'CW'}, # Added CW
    'DM': {'hl': 'en', 'gl': 'DM'}, 'GD': {'hl': 'en', 'gl': 'GD'}, 'KN': {'hl': 'en', 'gl': 'KN'},
    'LC': {'hl': 'en', 'gl': 'LC'}, 'MF': {'hl': 'fr', 'gl': 'MF'}, 'MS': {'hl': 'en', 'gl': 'MS'},
    'SX': {'hl': 'nl', 'gl': 'SX'}, 'TC': {'hl': 'en', 'gl': 'TC'}, 'VI': {'hl': 'en', 'gl': 'VI'},
    'FK': {'hl': 'en', 'gl': 'FK'}, 'BQ': {'hl': 'nl', 'gl': 'BQ'},
}

# Define city-specific overrides
CITY_OVERRIDES = {
    'Montréal': {'hl': 'fr'},
    'Québec': {'hl': 'fr'},    # Assuming name in file is 'Québec' for Québec City
    'Paramaribo': {'hl': 'nl'}, # Matches default for SR, but explicit override is fine
}

# Define the URL for the GeoNames cities file
CITIES_FILE_URL = "https://download.geonames.org/export/dump/cities15000.zip"
CITIES_FILE_ZIP = "data/cities15000.zip"
CITIES_FILE_TXT = "data/cities15000.txt"

# Define the URL for the GeoNames admin1 codes file
ADMIN1_FILE_URL = "https://download.geonames.org/export/dump/admin1CodesASCII.txt"
ADMIN1_FILE_TXT = "data/admin1CodesASCII.txt"

# Define the output file path
OUTPUT_CSV_FILE = "data/cities_shortlist.csv"

# SerpApi Locations API endpoint -- NO LONGER USED
# SERPAPI_LOCATIONS_URL = "https://serpapi.com/locations.json"

# Define column names for cities15000.txt based on GeoNames format
COLUMN_NAMES = [
    'geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude',
    'feature_class', 'feature_code', 'country_code', 'cc2', 'admin1_code',
    'admin2_code', 'admin3_code', 'admin4_code', 'population', 'elevation',
    'dem', 'timezone', 'modification_date'
]

# Define column names for admin1CodesASCII.txt
ADMIN1_COLUMN_NAMES = ['code', 'name', 'name_ascii', 'geonameid_admin1']

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_file(url, local_path):
    """Downloads a file from a URL to a local path if it doesn't exist."""
    if not os.path.exists(local_path):
        logging.info(f"Downloading {os.path.basename(local_path)} from {url}...")
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for bad status codes
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded {os.path.basename(local_path)}.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading {url}: {e}")
            if os.path.exists(local_path):
                os.remove(local_path) # Clean up partially downloaded file
            raise # Re-raise the exception to halt execution
    else:
        logging.info(f"{os.path.basename(local_path)} already exists locally.")

# --- REMOVED get_serpapi_canonical_location function ---
# def get_serpapi_canonical_location(city_name, country_code, api_key):
#     ...

def build_shortlist():
    """Filters cities15000, adds hl/gl, merges admin1 names, writes to CSV, deletes source."""
    # 0. Get SerpAPI Key -- NO LONGER NEEDED FOR THIS SCRIPT
    # serpapi_key = os.getenv("SERPAPI_API_KEY")
    # if not serpapi_key:
    #     logging.error("SERPAPI_API_KEY environment variable not set. Cannot fetch canonical locations or run pipeline.")
    #     return

    # 1. Download required files
    download_file(CITIES_FILE_URL, CITIES_FILE_ZIP)
    download_file(ADMIN1_FILE_URL, ADMIN1_FILE_TXT)

    # 2. Unzip the cities file (if not already unzipped)
    if not os.path.exists(CITIES_FILE_TXT):
        import zipfile
        logging.info(f"Unzipping {CITIES_FILE_ZIP}...")
        try:
            with zipfile.ZipFile(CITIES_FILE_ZIP, 'r') as zip_ref:
                zip_ref.extractall("data/")
            logging.info(f"Successfully unzipped to {CITIES_FILE_TXT}.")
        except zipfile.BadZipFile:
            logging.error(f"Error: {CITIES_FILE_ZIP} is not a valid zip file or is corrupted.")
            if os.path.exists(CITIES_FILE_TXT):
                 os.remove(CITIES_FILE_TXT)
            os.remove(CITIES_FILE_ZIP)
            logging.info("Removed potentially corrupted zip and text files. Please re-run the script.")
            return
        except Exception as e:
            logging.error(f"An unexpected error occurred during unzipping: {e}")
            return
    else:
        logging.info(f"{CITIES_FILE_TXT} already exists.")

    # 3. Load Admin1 Names
    logging.info(f"Loading admin1 names from {ADMIN1_FILE_TXT}...")
    try:
        df_admin1 = pd.read_csv(
            ADMIN1_FILE_TXT,
            sep='\t',
            header=None,
            names=ADMIN1_COLUMN_NAMES,
            usecols=['code', 'name'],
            encoding='utf-8',
            on_bad_lines='warn'
        )
        df_admin1['country_admin1_key'] = df_admin1['code']
        df_admin1 = df_admin1[['country_admin1_key', 'name']].rename(columns={'name': 'admin1_name'})
        admin1_map = df_admin1.set_index('country_admin1_key')['admin1_name'].to_dict()
        logging.info(f"Loaded {len(admin1_map)} admin1 names.")
    except FileNotFoundError:
        logging.error(f"Error: {ADMIN1_FILE_TXT} not found. Cannot proceed.")
        return
    except Exception as e:
        logging.error(f"Error loading admin1 names: {e}")
        return

    # 4. Load and Process Cities Data
    logging.info(f"Loading cities data from {CITIES_FILE_TXT}...")
    try:
        df = pd.read_csv(
            CITIES_FILE_TXT,
            sep='\t',
            header=None,
            names=COLUMN_NAMES,
            # Make sure latitude and longitude are loaded!
            usecols=['name', 'asciiname', 'latitude', 'longitude', 'country_code', 'admin1_code', 'population', 'timezone', 'geonameid'],
            encoding='utf-8',
            low_memory=False,
            on_bad_lines='warn'
        )
    except FileNotFoundError:
        logging.error(f"Error: {CITIES_FILE_TXT} not found. Cannot proceed.")
        return
    except Exception as e:
        logging.error(f"Error loading cities data: {e}")
        return

    logging.info(f"Processing {len(df)} cities...")

    df_americas = df[df['country_code'].isin(NA_SA_CODES)].copy()
    logging.info(f"Filtered to {len(df_americas)} cities in the Americas.")

    df_americas['population'] = pd.to_numeric(df_americas['population'], errors='coerce').fillna(0)
    df_sorted = df_americas.sort_values(by='population', ascending=False)

    df_limited = df_sorted.head(1250).copy()
    logging.info(f"Limited to top {len(df_limited)} cities by population.")

    # Apply country settings for hl/gl
    df_limited['hl'] = df_limited['country_code'].map(lambda cc: COUNTRY_SETTINGS.get(cc, {'hl': 'en'}).get('hl'))
    df_limited['gl'] = df_limited['country_code'].map(lambda cc: COUNTRY_SETTINGS.get(cc, {'gl': cc}).get('gl'))

    # Apply specific overrides
    for city_name, overrides in CITY_OVERRIDES.items():
        for col, val in overrides.items():
            df_limited.loc[df_limited['name'] == city_name, col] = val
    logging.info("Applied hl/gl logic and overrides.")

    # 5. Merge Admin1 Names
    df_limited['admin1_code'] = df_limited['admin1_code'].astype(str)
    df_limited['country_admin1_key'] = df_limited['country_code'] + '.' + df_limited['admin1_code']
    df_limited['admin1_name'] = df_limited['country_admin1_key'].map(admin1_map)
    logging.info("Merged admin1 names into city data.")
    missing_admin_count = df_limited['admin1_name'].isnull().sum()
    if missing_admin_count > 0:
        logging.warning(f"{missing_admin_count} cities did not have a matching admin1 name.")

    # --- REMOVED STEP 6: Get Canonical SerpApi Location String ---
    # logging.info("Fetching canonical location strings from SerpApi Locations API...")
    # canonical_locations = []
    # ... (loop removed)
    # df_limited['serpapi_location_string'] = canonical_locations
    # ...

    # 7. Prepare and Write Output
    # Select and order columns for output
    # REMOVED 'serpapi_location_string'
    output_columns = ['geonameid', 'name', 'asciiname', 'latitude', 'longitude', 'country_code', 'admin1_code', 'admin1_name', 'population', 'timezone', 'hl', 'gl']
    df_output = df_limited[output_columns]

    logging.info(f"Writing shortlist to {OUTPUT_CSV_FILE}...")
    try:
        df_output.to_csv(
            OUTPUT_CSV_FILE,
            index=False,
            encoding='utf-8',
            quoting=1 # QUOTE_ALL
        )
        logging.info(f"Successfully wrote shortlist to {OUTPUT_CSV_FILE}.")
    except Exception as e:
        logging.error(f"Error writing CSV file: {e}")
        return

    # 8. Clean up source files
    try:
        logging.info(f"Deleting source files: {CITIES_FILE_TXT}, {CITIES_FILE_ZIP}")
        if os.path.exists(CITIES_FILE_TXT):
            os.remove(CITIES_FILE_TXT)
        if os.path.exists(CITIES_FILE_ZIP):
            os.remove(CITIES_FILE_ZIP)
        # Keep admin1CodesASCII.txt for future runs
        logging.info("Cleaned up source files.")
    except OSError as e:
        logging.warning(f"Could not delete source file(s): {e}")

if __name__ == "__main__":
    build_shortlist()
