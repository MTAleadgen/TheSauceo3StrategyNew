import argparse
import os
import sys
import time
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Dict, Any, List, Optional

# Dynamically adjust path to import pipeline modules
# Assuming runner/cli.py is run from the project root (e.g., python -m runner.cli)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import pipeline components (adjust paths if needed based on execution context)
try:
    from pipelines.serpapi.events.request_builder import build_params
    from pipelines.serpapi.events.parser import parse_event_result
    from pipelines.serpapi.events.places_enricher import enrich_with_places
    from pipelines.serpapi.events.qwen_cleaner import rewrite_description
except ImportError as e:
    print(f"Error importing pipeline modules: {e}")
    print("Ensure the script is run correctly relative to the project structure, e.g., using 'python -m runner.cli'")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
SERPAPI_TIMEOUT = 30 # seconds for SerpAPI request
SERPAPI_MAX_RETRIES = 3
SERPAPI_BACKOFF_FACTOR = 2 # seconds

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
    wait_time = SERPAPI_BACKOFF_FACTOR
    serpapi_search_url = "https://serpapi.com/search.json"

    while retries <= SERPAPI_MAX_RETRIES:
        try:
            response = requests.get(serpapi_search_url, params=params, timeout=SERPAPI_TIMEOUT)
            
            if response.status_code == 429: # Rate limit hit
                logging.warning(f"SerpAPI rate limit hit (429). Retrying in {wait_time} seconds... ({retries + 1}/{SERPAPI_MAX_RETRIES})")
                time.sleep(wait_time)
                wait_time *= SERPAPI_BACKOFF_FACTOR # Exponential backoff
                retries += 1
                continue # Retry the request
            
            response.raise_for_status() # Raise HTTPError for other bad responses (4xx or 5xx)
            
            # Check for errors indicated in the response body (SerpAPI specific)
            result_json = response.json()
            if "error" in result_json:
                logging.error(f"SerpAPI returned an error: {result_json['error']}. Params: {params}")
                return None # Treat API-level error as failure for this call

            return result_json # Success

        except requests.exceptions.Timeout:
            logging.warning(f"SerpAPI request timed out. Retrying... ({retries + 1}/{SERPAPI_MAX_RETRIES})")
            time.sleep(wait_time) # Wait before retry on timeout too
            wait_time *= SERPAPI_BACKOFF_FACTOR
            retries += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"SerpAPI request failed: {e}. Params: {params}")
            return None # Non-retryable request error
        except Exception as e:
            logging.error(f"An unexpected error occurred during SerpAPI call: {e}")
            return None # Unexpected error
            
    logging.error(f"SerpAPI call failed after {SERPAPI_MAX_RETRIES} retries. Params: {params}")
    return None

def upsert_event(supabase: Client, event_data: Dict[str, Any]) -> bool:
    """Upserts a single event into the Supabase 'events' table."""
    try:
        # Ensure conflict keys are present for upsert
        conflict_keys = ['event_day', 'venue', 'name']
        if any(event_data.get(k) is None for k in conflict_keys):
            logging.warning(f"Skipping upsert due to missing conflict key(s) (event_day, venue, name) for event: {event_data.get('source_id') or event_data.get('name')}")
            return False # Cannot perform upsert without conflict keys

        # Supabase client upsert
        # Note: Supabase `upsert` uses `on_conflict` with primary key by default unless specified.
        # For a UNIQUE constraint like `uniq_event` (event_day, venue, name), 
        # you might need to specify it if it's not the primary key or handle it differently.
        # Let's assume for now the default behavior or a trigger handles the specific conflict.
        # If `uniq_event` needs explicit handling, the `on_conflict` parameter is needed.
        # Check supabase-py documentation for exact `on_conflict` syntax if required.
        response = supabase.table('events').upsert(event_data).execute()
        
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

# --- Main Execution --- 

def main():
    start_time = time.time()
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Run data pipeline for TheSauceo3.")
    parser.add_argument("--mode", default="serpapi_events", help="Pipeline mode (e.g., pipelines.serpapi.events)")
    parser.add_argument("--cities", default="data/cities_shortlist.csv", help="Path to cities shortlist CSV file.")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of SerpAPI requests per processing chunk.")
    parser.add_argument("--max-cities", type=int, default=None, help="Maximum number of cities to process (for testing).")
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
            
            # 1. Build Params
            try:
                serpapi_params = build_params(city_row)
            except ValueError as e:
                logging.error(f"Skipping city {city_row.get('name')} due to param build error: {e}")
                continue # Skip to next city
            except Exception as e:
                 logging.error(f"Unexpected error building params for city {city_row.get('name')}: {e}")
                 continue

            # 2. Call SerpAPI
            summary["total_serpapi_requests"] += 1
            serpapi_result = call_serpapi_with_retry(serpapi_params)

            if not serpapi_result:
                summary["serpapi_api_errors"] += 1
                continue # Skip city if SerpAPI call fails

            # Track credits (assuming structure from SerpAPI docs)
            search_info = serpapi_result.get('search_information', {})
            if isinstance(search_info, dict):
                 summary["total_serpapi_credits_used"] += search_info.get('credits_used', 0)

            # 3. Parse, Enrich, Clean, Upsert Events
            events_results = serpapi_result.get('events_results', [])
            if not events_results:
                logging.info(f"No events found in SerpAPI result for {city_row.get('name')}")
                continue

            summary["events_found"] += len(events_results)
            logging.info(f"Found {len(events_results)} potential events for {city_row.get('name')}")

            for event_raw in events_results:
                try:
                    # 3a. Parse
                    parsed_event = parse_event_result(event_raw)
                    if not parsed_event.get('source_id'):
                        logging.warning("Parsed event missing source_id, skipping.")
                        continue

                    # 3b. Enrich (if needed)
                    enrichment_needed = not parsed_event.get('address') or parsed_event.get('lat') is None
                    if enrichment_needed:
                        summary["enrichment_attempts"] += 1
                        parsed_event = enrich_with_places(parsed_event)
                    
                    # 3c. Clean Description
                    if parsed_event.get('description'):
                        summary["rewrite_attempts"] += 1
                        rewritten_desc = rewrite_description(parsed_event)
                        if rewritten_desc is not None:
                             # Only update if rewrite succeeded and is different?
                             # Maybe check length constraint too?
                             parsed_event['rewritten_description'] = rewritten_desc 
                        else:
                             logging.warning(f"Failed to rewrite description for event: {parsed_event.get('source_id')}")
                             # Keep original description or set rewritten to None/empty?
                             # For now, parsed_event keeps original description if rewrite fails.
                             parsed_event['rewritten_description'] = None # Explicitly set to None if rewrite fails
                    else:
                        parsed_event['rewritten_description'] = None # No original desc to rewrite

                    # 3d. Upsert to Supabase
                    if upsert_event(supabase, parsed_event):
                        summary["events_upserted_success"] += 1
                    else:
                        summary["events_upserted_failure"] += 1
                        summary["database_errors"] += 1 # Increment general DB error counter

                except Exception as e:
                    logging.error(f"Failed processing event item {event_raw.get('event_id', 'UNKNOWN')} for city {city_row.get('name')}: {e}")
                    summary["events_upserted_failure"] += 1 # Count as failure if processing fails

        logging.info(f"Finished batch {i + 1}/{num_batches}. Sleeping briefly...")
        time.sleep(2) # Small delay between batches

    # --- Final Summary --- 
    end_time = time.time()
    summary["runtime_seconds"] = round(end_time - start_time, 2)

    summary_message = f"""
Pipeline Run Summary ({args.mode}):
--------------------------------------
Total Cities Scanned:  {summary['total_cities_processed']}/{total_cities}
SerpAPI Requests:      {summary['total_serpapi_requests']}
SerpAPI Credits Used:  {summary['total_serpapi_credits_used']}
SerpAPI API Errors:    {summary['serpapi_api_errors']}

Events Found:          {summary['events_found']}
Place Enrich Attempts: {summary['enrichment_attempts']}
Desc Rewrite Attempts: {summary['rewrite_attempts']}

DB Upserts Success:    {summary['events_upserted_success']}
DB Upserts Failure:    {summary['events_upserted_failure']}
(DB Errors):         ({summary['database_errors']})

Total Runtime:         {summary['runtime_seconds']} seconds
--------------------------------------
"""

    # Email Stub / Output
    smtp_host = os.getenv("SMTP_HOST")
    if smtp_host:
        logging.info("SMTP_HOST is set. Email sending not implemented yet. Printing summary.")
        # Add email sending logic here later
        print(summary_message)
    else:
        logging.info("SMTP_HOST not set. Printing summary to console.")
        print(summary_message)

if __name__ == "__main__":
    main()
