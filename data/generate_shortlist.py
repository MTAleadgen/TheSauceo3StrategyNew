import pandas as pd
from pathlib import Path
import csv

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

# Define column names for cities15000.txt based on GeoNames format
COLUMN_NAMES = [
    'geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude',
    'feature_class', 'feature_code', 'country_code', 'cc2', 'admin1_code',
    'admin2_code', 'admin3_code', 'admin4_code', 'population', 'elevation',
    'dem', 'timezone', 'modification_date'
]

def build_shortlist(
    infile: Path = Path("data/cities15000.txt"),
    outfile: Path = Path("data/cities_shortlist.csv"),
    limit: int = 1250,
) -> None:
    """
    Reads cities15000.txt, filters for North/South America, sorts by population,
    limits the results, adds hl/gl columns, writes to CSV, and deletes the input file.
    """
    if not infile.exists():
        print(f"Error: Input file not found at {infile}")
        return

    try:
        # Read the tab-separated file
        df = pd.read_csv(
            infile,
            sep='\\t',
            header=None,
            names=COLUMN_NAMES,
            encoding='utf-8',
            engine='python', # Needed because default 'c' engine doesn't handle \\t well sometimes
            quoting=csv.QUOTE_NONE # Important for GeoNames format
        )

        # Filter by North & South American country codes
        df_filtered = df[df['country_code'].isin(NA_SA_CODES)].copy()

        # Convert population to numeric, coerce errors to NaN, and drop rows with invalid population
        df_filtered['population'] = pd.to_numeric(df_filtered['population'], errors='coerce')
        df_filtered.dropna(subset=['population'], inplace=True)
        df_filtered['population'] = df_filtered['population'].astype(int) # Convert to integer

        # Sort by population descending
        df_sorted = df_filtered.sort_values(by='population', ascending=False)

        # Keep top N entries
        df_limited = df_sorted.head(limit).copy() # Use copy to avoid SettingWithCopyWarning

        # Add hl and gl columns
        def get_setting(row, setting_type):
            country = row['country_code']
            city = row['name']

            # Check city overrides first
            if city in CITY_OVERRIDES and setting_type in CITY_OVERRIDES[city]:
                return CITY_OVERRIDES[city][setting_type]

            # Check country settings
            if country in COUNTRY_SETTINGS:
                return COUNTRY_SETTINGS[country][setting_type]

            # Fallback defaults (e.g., 'en' for hl, country code for gl)
            return 'en' if setting_type == 'hl' else country

        df_limited['hl'] = df_limited.apply(lambda row: get_setting(row, 'hl'), axis=1)
        df_limited['gl'] = df_limited.apply(lambda row: get_setting(row, 'gl'), axis=1)

        # Select and order columns for output (optional, but good practice)
        output_columns = ['geonameid', 'name', 'asciiname', 'latitude', 'longitude', 'country_code', 'population', 'timezone', 'hl', 'gl']
        df_output = df_limited[output_columns]

        # Write to comma-separated file with quoting
        df_output.to_csv(
            outfile,
            index=False,
            encoding='utf-8',
            quoting=csv.QUOTE_ALL # Ensure all fields are quoted
        )
        print(f"Successfully created shortlist at {outfile}")

        # Delete the original input file
        try:
            infile.unlink()
            print(f"Successfully deleted input file {infile}")
        except OSError as e:
            print(f"Error deleting input file {infile}: {e}")

    except Exception as e:
        print(f"An error occurred during processing: {e}")


if __name__ == "__main__":
    # Make sure the script resolves paths relative to its own location if run directly
    script_dir = Path(__file__).parent
    default_infile = script_dir / "cities15000.txt"
    default_outfile = script_dir / "cities_shortlist.csv"

    # You might want to add command-line argument parsing here later
    # For now, it uses the defaults or paths relative to the script dir
    build_shortlist(infile=default_infile, outfile=default_outfile)
