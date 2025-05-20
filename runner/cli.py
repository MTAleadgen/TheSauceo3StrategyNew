import argparse
import os
import sys
import time
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Dict, Any, List, Optional, Tuple
import json
import math # For pagination
import backoff # For retries
from requests.exceptions import RequestException # Specific exception for backoff
from datetime import datetime, timedelta
import string
from unidecode import unidecode

# Dynamically adjust path to import pipeline modules
# Assuming runner/cli.py is run from the project root (e.g., python -m runner.cli)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import pipeline components (adjust paths if needed based on execution context)
try:
    from pipelines.serpapi.events.request_builder import build_params
    from pipelines.serpapi.events.parser import parse_event_result
    from pipelines.serpapi.events.places_enricher import enrich_with_places
except ImportError as e:
    print(f"Error importing pipeline modules: {e}")
    print("Ensure the script is run correctly relative to the project structure, e.g., using 'python -m runner.cli'")
    sys.exit(1)

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)
# Configure logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/clean_events.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_MAX_EVENTS_PER_CITY = 100
DEFAULT_DAYS_FORWARD = 21
SERPAPI_TIMEOUT = 60 # seconds for SerpAPI request (Increased from 30)
SERPAPI_MAX_RETRIES = 5 # Increased from 3
SERPAPI_BACKOFF_FACTOR = 3 # seconds (Increased from 2 to 3)
SERPAPI_RESULTS_PER_PAGE = 10 # Standard for Google Events API via SerpApi, typically 10 results per page increment

# --- Helper Functions ---

def load_cities(filepath: str, max_cities: Optional[int]) -> List[Dict[str, Any]]:
    """Loads cities from CSV into a list of dictionaries."""
    try:
        df = pd.read_csv(filepath, encoding='utf-8')
        # Ensure required columns are present (adjust based on actual usage)
        required_cols = ['name', 'country_code', 'hl', 'gl']
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            raise ValueError(f"Missing required columns in cities file: {missing}")
        
        if max_cities is not None and max_cities > 0:
            df = df.head(max_cities)
            logging.info(f"Limiting run to {max_cities} cities.")
            
        # Convert to list of dicts
        cities = df.to_dict('records')
        logging.info(f"Loaded {len(cities)} cities from {filepath}")
        return cities
    except FileNotFoundError:
        logging.error(f"Cities file not found: {filepath}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading cities file {filepath}: {e}")
        sys.exit(1)

def call_serpapi_with_retry(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Calls SerpAPI with exponential backoff for retries."""
    retries = 0
    wait_time = SERPAPI_BACKOFF_FACTOR # Initial wait time
    serpapi_search_url = "https://serpapi.com/search.json"

    # Loop for retries. Initial attempt is retries=0.
    # Loop will run for retries = 0, 1, ..., SERPAPI_MAX_RETRIES - 1
    # This means SERPAPI_MAX_RETRIES number of retry attempts after the first failed one.
    # Total attempts = 1 (initial) + SERPAPI_MAX_RETRIES
    while retries <= SERPAPI_MAX_RETRIES: # Changed < to <= to ensure we try exactly SERPAPI_MAX_RETRIES times
        try:
            response = requests.get(serpapi_search_url, params=params, timeout=SERPAPI_TIMEOUT)
            
            if response.status_code == 429: # Rate limit hit
                # Use retries + 1 for logging because retries is 0-indexed
                logging.warning(f"SerpAPI rate limit hit (429). Retrying in {wait_time} seconds... ({retries + 1}/{SERPAPI_MAX_RETRIES})")
                time.sleep(wait_time)
                wait_time *= SERPAPI_BACKOFF_FACTOR 
                retries += 1
                continue 
            
            response.raise_for_status() 
            
            result_json = response.json()
            if "error" in result_json:
                logging.error(f"SerpAPI returned an error: {result_json['error']}. Params: {params}")
                return None 

            return result_json 

        except requests.exceptions.Timeout:
            # Use retries + 1 for logging
            logging.warning(f"SerpAPI request timed out. Retrying... ({retries + 1}/{SERPAPI_MAX_RETRIES})")
            time.sleep(wait_time) 
            wait_time *= SERPAPI_BACKOFF_FACTOR
            retries += 1
        except requests.exceptions.ConnectionError as e:
            # Connection errors like ConnectionResetError need to be retried
            logging.warning(f"SerpAPI connection error: {e}. Retrying in {wait_time} seconds... ({retries + 1}/{SERPAPI_MAX_RETRIES})")
            time.sleep(wait_time)
            wait_time *= SERPAPI_BACKOFF_FACTOR
            retries += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"SerpAPI request failed: {e}. Params: {params}")
            return None 
        except Exception as e:
            logging.error(f"An unexpected error occurred during SerpAPI call: {e}")
            return None
            
    # This log will now be accurate: if loop finishes, all SERPAPI_MAX_RETRIES have been used.
    logging.error(f"SerpAPI call failed after initial attempt and {SERPAPI_MAX_RETRIES} retries. Params: {params}")
    return None

def upsert_event(supabase: Client, event_data: Dict[str, Any]) -> bool:
    """Upserts a single event into the Supabase 'events' table."""
    try:
        # Removed check: Ensure conflict keys are present for upsert
        # conflict_keys = ['event_day', 'venue', 'name']
        # if any(event_data.get(k) is None for k in conflict_keys):
        #     logging.warning(f"Skipping upsert due to missing conflict key(s) (event_day, venue, name) for event: {event_data.get('source_id') or event_data.get('name')}")
        #     return False # Cannot perform upsert without conflict keys

        # Supabase client upsert with specified conflict columns
        response = supabase.table('events').upsert(
            event_data,
            on_conflict='event_day,venue,name' # Specify conflict columns as a comma-separated string
        ).execute()
        
        # Check response (supabase-py v1+ returns APIResponse)
        if hasattr(response, 'data') and response.data:
             # Basic check, might need more specific checks based on response structure
            logging.debug(f"Upsert successful for event: {event_data.get('source_id') or event_data.get('name')}")
            return True
        else:
             # Log error details if available in response
            error_info = getattr(response, 'error', 'No error details')
            logging.error(f"Supabase upsert failed for event {event_data.get('source_id')}. Error: {error_info}")
            return False
            
    except Exception as e:
        logging.error(f"Error during Supabase upsert for event {event_data.get('source_id')}: {e}")
        logging.debug(f"Event data causing error: {event_data}")
        return False

# --- Placeholder for Email Function ---
def send_email(summary_data: Dict[str, Any]):
    """Placeholder function to send email summary. Implement later."""
    logging.info("Placeholder: send_email function called. Email sending not implemented.")
    # Example: print JSON to log for now
    logging.debug(f"Email Summary Data:\n{json.dumps(summary_data, indent=2, default=str)}")
    pass

def delete_past_events(supabase: Client):
    """Delete events from both 'events' and 'events_clean' tables where event_day is before today."""
    today = datetime.utcnow().date().isoformat()
    for table in ['events', 'events_clean']:
        try:
            response = supabase.table(table).delete().lt('event_day', today).execute()
            deleted_count = len(response.data) if hasattr(response, 'data') and response.data else 0
            logger.info(f"Deleted {deleted_count} past events from {table}.")
        except Exception as e:
            logger.error(f"Error deleting past events from {table}: {e}")

def canon(txt):
    if not txt:
        return None
    txt = unidecode(txt.lower()).translate(str.maketrans('', '', string.punctuation))
    return " ".join(txt.split())

def should_fetch_next_page(events, max_events=10, days_window=7, threshold=0.8):
    """Return True if next page should be fetched: page is full and most events are within the next week."""
    if len(events) < max_events:
        return False
    now = datetime.now()
    week_later = now + timedelta(days=days_window)
    in_window = [
        e for e in events
        if 'event_day' in e and e['event_day']
        and now <= datetime.strptime(e['event_day'], '%Y-%m-%d') <= week_later
    ]
    return len(in_window) / len(events) >= threshold

# --- Main Execution --- 

def main():
    start_time = time.time()
    # Revert to standard load_dotenv
    load_dotenv() 
    # # Explicitly provide path to .env file in current directory
    # dotenv_path = os.path.join(os.getcwd(), '.env') 
    # # Set override=True in case system env vars conflict (optional, but can help)
    # loaded = load_dotenv(dotenv_path=dotenv_path, override=True) 
    # if not loaded:
    #     print(f"--- DIAGNOSTIC: Failed to load .env file from {dotenv_path} ---")
    # # load_dotenv()
    # # --- DIAGNOSTIC PRINT ---
    # print("--- DIAGNOSTIC: Checking Environment Variables ---")
    # print(f"SUPABASE_URL: {os.getenv('SUPABASE_URL')}")
    # print(f"SUPABASE_KEY: {os.getenv('SUPABASE_KEY')}")
    # print(f"SERPAPI_API_KEY: {os.getenv('SERPAPI_API_KEY')}")
    # print(f"GOOGLE_PLACES_API_KEY: {os.getenv('GOOGLE_PLACES_API_KEY')}")
    # print(f"LAMBDA_API_KEY: {os.getenv('LAMBDA_API_KEY')}")
    # print("--- END DIAGNOSTIC ---")
    
    parser = argparse.ArgumentParser(description="Run data pipeline for TheSauceo3.")
    parser.add_argument("--mode", required=True, choices=["serpapi_events"], help="Pipeline mode to run.")
    parser.add_argument("--cities", required=True, help="Path to the cities CSV file.")
    parser.add_argument("--max-cities", type=int, default=None, help="Maximum number of cities to process.")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of cities to process in each batch (currently processes one city at a time).") # Batching might be re-evaluated
    # NEW ARGUMENTS
    parser.add_argument("--max-events", type=int, default=DEFAULT_MAX_EVENTS_PER_CITY, help=f"Maximum events to fetch per city (default: {DEFAULT_MAX_EVENTS_PER_CITY}).")
    parser.add_argument("--days-forward", type=int, default=DEFAULT_DAYS_FORWARD, help=f"How many days into the future to include events for (default: {DEFAULT_DAYS_FORWARD}).")
    
    args = parser.parse_args()

    # --- Argument Validation and Setup ---
    logging.info(f"Starting pipeline run with mode: {args.mode}")
    if args.mode != "serpapi_events":
        logging.error(f"Mode '{args.mode}' is not yet implemented. Only 'serpapi_events' is supported.")
        sys.exit(1)

    # Load cities
    cities = load_cities(args.cities, args.max_cities)
    if not cities:
        sys.exit(1)

    # Supabase Client Setup
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        logging.error("SUPABASE_URL and SUPABASE_KEY must be set in the environment or .env file.")
        sys.exit(1)
    try:
        supabase: Client = create_client(supabase_url, supabase_key)
        logging.info("Supabase client initialized.")
    except Exception as e:
        logging.error(f"Failed to initialize Supabase client: {e}")
        sys.exit(1)

    # --- Pipeline Execution ---
    total_cities = len(cities)
    # batch_size = args.batch_size # Batching not fully implemented, process one city at a time
    # num_batches = (total_cities + batch_size - 1) // batch_size

    summary = {
        "total_cities_processed": 0,
        "total_serpapi_requests": 0,
        "total_serpapi_credits_used": 0,
        "events_found": 0,
        "events_upserted_success": 0,
        "events_upserted_failure": 0,
        "enrichment_attempts": 0,
        "rewrite_attempts": 0,
        "serpapi_api_errors": 0,
        "database_errors": 0,
        "runtime_seconds": 0,
    }
    
    # Delete past events before starting new run
    delete_past_events(supabase)

    # Process each city
    for city_index, city_info in enumerate(cities):
        logging.info(f"Processing city {city_index + 1}/{total_cities}: {city_info['name']}")
        summary["total_cities_processed"] += 1
        
        current_page = 0
        events_fetched_for_city = 0
        max_pages = math.ceil(args.max_events / SERPAPI_RESULTS_PER_PAGE) if args.max_events > 0 else float('inf')

        while events_fetched_for_city < args.max_events and current_page < max_pages:
            # Calculate 'start' parameter for pagination
            # SerpAPI's 'start' is 0-indexed for the first page, then 10, 20, etc.
            start_index = current_page * SERPAPI_RESULTS_PER_PAGE
            
            params = build_params(city_info)
            params["start"] = start_index
            params["num"] = SERPAPI_RESULTS_PER_PAGE
            logging.debug(f"SerpAPI params for {city_info['name']}, page {current_page + 1}: {params}")
            
            result_json = call_serpapi_with_retry(params)
            summary["total_serpapi_requests"] += 1

            if not result_json:
                summary["serpapi_api_errors"] += 1
                logging.error(f"No result from SerpAPI for {city_info['name']}, page {current_page + 1}. Skipping to next city or page.")
                break # Break from while loop (pagination for this city)

            # Increment credits used if API call was successful and returned results
            # Assuming 1 credit per successful API call with results
            if "events_results" in result_json and result_json["events_results"]:
                 summary["total_serpapi_credits_used"] += 1


            events_on_page = result_json.get("events_results", [])
            if not events_on_page:
                logging.info(f"No more events found for {city_info['name']} on page {current_page + 1}.")
                break # No more events for this city

            logging.info(f"Found {len(events_on_page)} events on page {current_page + 1} for {city_info['name']}.")

            for event_item in events_on_page:
                if events_fetched_for_city >= args.max_events:
                    logging.info(f"Reached max events ({args.max_events}) for city {city_info['name']}.")
                    break # Break from inner for loop

                parsed_event_data = parse_event_result(event_item, city_info=city_info)
                if not parsed_event_data:
                    logging.warning(f"Could not parse event item: {event_item.get('title', 'Unknown Event')}")
                    continue

                # --- NEW: Extract venue name from SerpAPI result ---
                venue_name_from_serp = event_item.get("venue", {}).get("name")
                parsed_event_data["venue"] = venue_name_from_serp # Add/overwrite venue

                # --- NEW: Ensure address is a string ---
                serp_address_list = event_item.get("address", [])
                if isinstance(serp_address_list, list):
                    parsed_event_data["address"] = ", ".join(serp_address_list)
                elif isinstance(serp_address_list, str): # Should not happen based on example, but good practice
                     parsed_event_data["address"] = serp_address_list
                else:
                    parsed_event_data["address"] = None # Or some default if address is critical

                # Add raw_when for potential LLM processing later (if not already in parse_event_result)
                if "raw_when" not in parsed_event_data and "date" in event_item and "when" in event_item["date"]:
                    parsed_event_data["raw_when"] = event_item["date"]["when"]
                
                # Add ticket_info_raw (if not already in parse_event_result)
                if "ticket_info_raw" not in parsed_event_data and "ticket_info" in event_item:
                     parsed_event_data["ticket_info_raw"] = json.dumps(event_item["ticket_info"])

                # Canonicalize venue, address, and name before upsert
                if 'venue' in parsed_event_data:
                    parsed_event_data['venue'] = canon(parsed_event_data['venue'])
                if 'address' in parsed_event_data:
                    parsed_event_data['address'] = canon(parsed_event_data['address'])
                if 'name' in parsed_event_data:
                    parsed_event_data['name'] = canon(parsed_event_data['name'])

                # Remove any fields not in the events table schema before upsert
                allowed_fields = {
                    'source_id', 'source_url', 'source_platform', 'retrieved_at', 'name', 'description',
                    'venue', 'address', 'city', 'country', 'lat', 'lng', 'event_day', 'raw_when'
                }
                parsed_event_data = {k: v for k, v in parsed_event_data.items() if k in allowed_fields}

                logging.debug(f"Attempting to upsert event: {parsed_event_data.get('name')}")
                if upsert_event(supabase, parsed_event_data):
                    summary["events_upserted_success"] += 1
                else:
                    summary["events_upserted_failure"] += 1
                    summary["database_errors"] += 1 
                
                events_fetched_for_city += 1
                summary["events_found"] += 1
            
            if events_fetched_for_city >= args.max_events:
                break # Break from while loop (pagination)

            # --- SMART PAGINATION: Only fetch next page if most events are soon ---
            if not should_fetch_next_page([parse_event_result(e, city_info=city_info) for e in events_on_page]):
                logging.info(f"Smart pagination: Not fetching next page for {city_info['name']} (not enough near-term events).")
                break

            # Check if there are more pages
            pagination_info = result_json.get("serpapi_pagination", result_json.get("pagination")) # check both keys
            if pagination_info and "next" in pagination_info:
                current_page += 1
                logging.info(f"Advancing to next page ({current_page + 1}) for {city_info['name']}.")
                # Small delay before next paginated request for the same city
                time.sleep(1) 
            else:
                logging.info(f"No more pages indicated for {city_info['name']}.")
                break # No more pages

        logging.info(f"Finished processing city: {city_info['name']}. Fetched {events_fetched_for_city} events.")
        # Optional: Longer delay between different cities if needed
        # time.sleep(2) 

    # --- Final Summary & Cleanup ---
    summary["runtime_seconds"] = round(time.time() - start_time, 2)
    logging.info("Pipeline run finished.")
    logging.info(f"Summary:\n{json.dumps(summary, indent=2, default=str)}")
    
    # Placeholder for sending email with summary
    # send_email(summary)

if __name__ == "__main__":
    main()
