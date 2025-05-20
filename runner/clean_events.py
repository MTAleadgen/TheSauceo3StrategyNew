import os
import sys
import logging
import json
import time
from dotenv import load_dotenv
from supabase import create_client, Client
from pipelines.serpapi.events.qwen_cleaner import enrich_event_with_llm
from pipelines.cleaner.transformer import transform_event_data

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/clean_events.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
print("DEBUG: LAMBDA_TOKEN =", os.getenv("LAMBDA_TOKEN"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL and SUPABASE_KEY must be set in the environment or .env file.")
    sys.exit(1)

def get_all_events(supabase: Client):
    try:
        response = supabase.table('events').select('*').range(0, 9999).execute()
        if hasattr(response, 'data'):
            return response.data
        return []
    except Exception as e:
        logger.error(f"Error fetching events from Supabase: {e}")
        return []

def validate_event_data(event):
    # Example validation: ensure required fields exist and are of correct type
    required_fields = ['event_id', 'name', 'event_day']
    for field in required_fields:
        if field not in event or event[field] is None:
            return False, f"Missing required field: {field}"
    # Add more validation as needed
    return True, None

def upsert_event_clean_with_retry(supabase: Client, event_clean: dict, max_retries=3, delay=2):
    for attempt in range(1, max_retries + 1):
        try:
            event_clean.pop('end_time', None)
            response = supabase.table('events_clean').upsert(event_clean, on_conflict='event_id').execute()
            if hasattr(response, 'data') and response.data:
                return True, None
            else:
                error_msg = f"Upsert to events_clean failed: {getattr(response, 'error', 'No error details')}"
                if attempt == max_retries:
                    return False, error_msg
                time.sleep(delay)
        except Exception as e:
            if attempt == max_retries:
                return False, f"Exception during upsert: {e}"
            time.sleep(delay)
    return False, "Unknown error after retries"

def save_failed_event(event, error):
    try:
        os.makedirs('logs', exist_ok=True)
        with open('logs/failed_events.jsonl', 'a', encoding='utf-8') as f:
            f.write(json.dumps({'event': event, 'error': error}, ensure_ascii=False) + '\n')
    except Exception as e:
        logger.error(f"Failed to save failed event: {e}")

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    events = get_all_events(supabase)
    logger.info(f"Fetched {len(events)} events from Supabase")
    logger.info(f"Event IDs: {[e.get('id') or e.get('source_id') for e in events]}")
    total_processed = 0
    total_upserted = 0
    total_failed = 0
    filtered_events = []  # To collect filtered (non-dance) events
    logger.info(f"Processing {len(events)} events")
    for event in events:
        try:
            event_id = event.get('id') or event.get('source_id')
            logger.info(f"\n--- Processing Event ---\nID: {event_id}\nName: {event.get('name')}\nDescription: {event.get('description')}\nRaw_when: {event.get('raw_when')}\nTime: {event.get('time')}")
            # Step 1: Enrich event with LLM (description, live_band, class_before, is_dance_event)
            if event.get('description'):
                event_llm = enrich_event_with_llm(event)
                logger.info(f"LLM Output for Event {event_id}: {event_llm}")
                event = event_llm
            # Step 2: Clean/transform event
            cleaned = transform_event_data(event)
            logger.info(f"Cleaned Event for {event_id}: {cleaned}")
            if not cleaned:
                logger.warning(f"Transformer returned None for event: {event_id}. Proceeding to upsert anyway.")
                cleaned = event  # Use the event as-is if transformer returns None
            # Step 2.5: Filter by is_dance_event
            logger.info(f"Checking is_dance_event for {event_id}: {cleaned.get('is_dance_event')}")
            if cleaned.get('is_dance_event') is False:
                logger.info(f"Skipping non-dance event: {event_id} - {event.get('name')}")
                filtered_events.append({'id': event_id, 'name': event.get('name')})
                total_failed += 1
                continue
            # Step 2.6: Validate data before upsert
            is_valid, validation_error = validate_event_data(cleaned)
            if not is_valid:
                logger.error(f"Validation failed for event: {event_id}. Error: {validation_error}. Data: {cleaned}")
                save_failed_event(cleaned, validation_error)
                total_failed += 1
                continue
            # Log if time is missing
            if not cleaned.get('time'):
                logger.warning(f"Event missing time: {event_id} - {event.get('name')}. LLM output: {event}")
            # Step 3: Upsert to events_clean with retry
            logger.info(f"Upserting event: {event_id} - {cleaned.get('name')}")
            success, upsert_error = upsert_event_clean_with_retry(supabase, cleaned)
            if success:
                total_upserted += 1
            else:
                logger.warning(f"Upsert failed for event: {event_id}. Error: {upsert_error}. Data: {cleaned}")
                save_failed_event(cleaned, upsert_error)
                total_failed += 1
            total_processed += 1
        except Exception as e:
            logger.error(f"Failed to process event {event.get('id') or event.get('source_id')}: {e}. Data: {event}")
            save_failed_event(event, str(e))
            total_failed += 1
    # Log a summary of filtered (non-dance) events
    if filtered_events:
        logger.info("\n==== Filtered (Non-Dance) Events Report ====")
        for fe in filtered_events:
            logger.info(f"Filtered out: ID={fe['id']}, Name={fe['name']}")
        logger.info("==== End of Filtered Events Report ====")
    logger.info(f"Done. Processed: {total_processed}, Upserted: {total_upserted}, Failed: {total_failed}")

if __name__ == "__main__":
    main() 