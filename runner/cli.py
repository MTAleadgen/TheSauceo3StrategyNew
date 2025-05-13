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
from pipelines.serpapi.events.qwen_cleaner import rewrite_description

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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
DEFAULT_MAX_EVENTS_PER_CITY = 100
DEFAULT_DAYS_FORWARD = 14
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
    batch_size = args.batch_size
    num_batches = (total_cities + batch_size - 1) // batch_size

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

    for i in range(num_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, total_cities)
        batch_cities = cities[start_index:end_index]
        
        logging.info(f"Processing batch {i + 1}/{num_batches} (Cities {start_index + 1}-{end_index})...")

        for city_row in batch_cities:
            summary["total_cities_processed"] += 1
            logging.info(f"Processing city: {city_row.get('name')}, {city_row.get('country_code')}")
            
            all_city_events_raw = []
            current_page = 0
            MAX_PAGES_TO_FETCH = 2 # Max 2 pages * 10 results/page = 20 events max

            # 1. Build initial Params
            try:
                serpapi_params = build_params(city_row)
            except ValueError as e:
                logging.error(f"Skipping city {city_row.get('name')} due to param build error: {e}")
                continue # Skip to next city
            except Exception as e:
                 logging.error(f"Unexpected error building params for city {city_row.get('name')}: {e}")
                 continue

            # Fetch pages (up to MAX_PAGES_TO_FETCH)
            while current_page < MAX_PAGES_TO_FETCH:
                # For subsequent pages, update start param
                if current_page > 0:
                    serpapi_params['start'] = current_page * SERPAPI_RESULTS_PER_PAGE
                    # Add a short delay between page requests to avoid connection issues
                    time.sleep(2)
                
                # 2. Call SerpAPI
                try:
                    serpapi_results = call_serpapi_with_retry(serpapi_params)
                    if not serpapi_results:
                        logging.warning(f"No results returned for page {current_page+1}")
                        break
                        
                    summary["total_serpapi_requests"] += 1
                    
                    # 2a. Extract events list
                    events_results = serpapi_results.get("events_results", [])
                    if events_results:
                        all_city_events_raw.extend(events_results)
                        logging.info(f"Page {current_page+1}: Retrieved {len(events_results)} events for {city_row.get('name')}")
                    else:
                        logging.info(f"No events found on page {current_page+1} for {city_row.get('name')}")
                        break  # No events on this page, so stop requesting more
                    
                    # 2b. Check if there are still more events to fetch
                    has_more_events = len(events_results) >= SERPAPI_RESULTS_PER_PAGE
                    
                    # Advance to next page if there are potentially more results
                    current_page += 1
                    
                    # Break if we got fewer results than expected (no more pages)
                    if not has_more_events:
                        logging.info(f"No more events for {city_row.get('name')} after page {current_page}")
                        break
                        
                except Exception as e:
                    logging.error(f"Error fetching events for {city_row.get('name')}: {e}")
                    summary["serpapi_api_errors"] += 1  # Increment error counter
                    break  # Skip to next city on error

            # Process all events for this city
            summary["events_found"] += len(all_city_events_raw)
            logging.info(f"Found {len(all_city_events_raw)} total potential events across all pages for {city_row.get('name')}")

            for event_idx, event_raw in enumerate(all_city_events_raw, 1):
                parsed_event = None
                try:
                    # 3a. Parse
                    parsed_event = parse_event_result(event_raw, args.days_forward, city_row)
                    
                    # If parsing failed or event was filtered out, skip to next event_raw
                    if parsed_event is None:
                        # source_id might not exist if parsing failed very early
                        event_identifier = event_raw.get("link") or event_raw.get("title", "UNKNOWN_EVENT")
                        logging.warning(f"Skipping event processing for {event_identifier} as parsing returned None.")
                        continue

                    if not parsed_event.get('source_id'): # This check might be redundant if None is handled above
                        logging.warning("Parsed event missing source_id, skipping. Event data: " + str(parsed_event))
                        continue

                    # 3b. Enrich (if needed)
                    enrichment_needed = not parsed_event.get('address') or parsed_event.get('lat') is None
                    if enrichment_needed:
                        summary["enrichment_attempts"] += 1
                        parsed_event = enrich_with_places(parsed_event)
                    
                    # 3c. Clean Description
                    # No rewriting or extra fields at this stage; just keep the raw description
                    # Remove fields not needed at this stage
                    for field in ["live_band", "class_before", "rewritten_description", "dance_styles"]:
                        if field in parsed_event:
                            del parsed_event[field]

                    # Handle missing values that could cause DB errors
                    if parsed_event.get('venue') is None:
                        parsed_event['venue'] = "__VENUE_UNKNOWN__"  # Must have a venue for conflict checks
                    
                    # Fix start_time format - Supabase expects timestamp values, not just the time
                    # If we have start_time but not event_day, set start_time to null
                    if parsed_event.get('start_time') and not parsed_event.get('event_day'):
                        parsed_event['start_time'] = None
                    # If we have both event_day and start_time, format as timestamp
                    elif parsed_event.get('start_time') and parsed_event.get('event_day'):
                        # Convert to proper timestamp format: "2023-05-16T14:30:00"
                        parsed_event['start_time'] = f"{parsed_event['event_day']}T{parsed_event['start_time']}:00"

                    # 3d. Upsert to Supabase
                    if upsert_event(supabase, parsed_event):
                        summary["events_upserted_success"] += 1
                    else:
                        summary["events_upserted_failure"] += 1
                        summary["database_errors"] += 1 # Increment general DB error counter

                except Exception as e:
                    # Log the specific event_raw that caused the error if possible
                    failed_event_id = event_raw.get("link") or event_raw.get("title", "UNKNOWN_RAW_EVENT")
                    logging.error(f"Failed processing event item {failed_event_id} for city {city_row.get('name')}: {e}")
                    # Log the parsed_event if it exists and parsing didn't fail catastrophically
                    if parsed_event:
                        logging.error(f"State of parsed_event at time of failure: {parsed_event}")
                    summary["events_upserted_failure"] += 1 # Count as failure if processing fails

        logging.info(f"Finished batch {i + 1}/{num_batches}. Sleeping briefly...")
        time.sleep(2) # Small delay between batches

    # --- Final Summary --- 
    end_time = time.time()
    summary["runtime_seconds"] = round(end_time - start_time, 2)

    # Email Stub / Output
    if os.getenv("SMTP_HOST"):
        logging.info("SMTP_HOST is set. Attempting to send email summary.")
        send_email(summary)        # implement later
    else:
        logging.info("SMTP_HOST not set. Logging run summary as JSON.")
        # Use logging instead of print for consistency
        logging.info("Run summary:\n%s", json.dumps(summary, indent=2, default=str))

if __name__ == "__main__":
    main()
