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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f'Current working directory: {os.getcwd()}')

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
        os.makedirs(LOGS_DIR, exist_ok=True)
        failed_events_path = f'{LOGS_DIR}/failed_events.jsonl'
        with open(failed_events_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({'event': event, 'error': error}, ensure_ascii=False) + '\n')
        logger.info(f'Wrote failed event to {failed_events_path}')
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
    failed_events_summary = []  # Collect all failed events and reasons
    logger.info(f"Processing {len(events)} events")
    for event in events:
        try:
            event_id = event.get('id') or event.get('source_id')
            event_name = event.get('name')
            logger.info(f"\n--- Processing Event ---\nID: {event_id}\nName: {event_name}\nDescription: {event.get('description')}\nRaw_when: {event.get('raw_when')}\nTime: {event.get('time')}")
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
            # Always ensure event_id is present before upsert
            cleaned['event_id'] = cleaned.get('event_id') or cleaned.get('id') or cleaned.get('source_id')
            if not cleaned['event_id']:
                logger.warning(f"Event missing event_id: {event_id} - {event_name}. Data: {cleaned}")
            # Log if time is missing
            if not cleaned.get('time'):
                logger.warning(f"Event missing time: {event_id} - {event_name}. LLM output: {event}")
            # Step 3: Upsert to events_clean with retry
            # Ensure is_dance_event is always present (True/False/None)
            if 'is_dance_event' not in cleaned:
                cleaned['is_dance_event'] = None
            logger.info(f"Upserting event: {event_id} - {cleaned.get('name')}")
            success, upsert_error = upsert_event_clean_with_retry(supabase, cleaned)
            if success:
                total_upserted += 1
            else:
                logger.warning(f"Upsert failed for event: {event_id}. Error: {upsert_error}. Data: {cleaned}")
                save_failed_event(cleaned, upsert_error)
                failed_events_summary.append({'id': event_id, 'name': event_name, 'reason': f'Upsert failed: {upsert_error}'})
                total_failed += 1
            total_processed += 1
        except Exception as e:
            logger.error(f"Failed to process event {event.get('id') or event.get('source_id')}: {e}. Data: {event}")
            save_failed_event(event, str(e))
            failed_events_summary.append({'id': event.get('id') or event.get('source_id'), 'name': event.get('name'), 'reason': f'Exception: {e}'})
            total_failed += 1
    # Log a summary of all failed events and reasons
    if failed_events_summary:
        logger.info("\n==== Failed Events Report ====")
        for fe in failed_events_summary:
            logger.info(f"Failed: ID={fe['id']}, Name={fe['name']}, Reason={fe['reason']}")
        logger.info("==== End of Failed Events Report ====")
    logger.info(f"Done. Processed: {total_processed}, Upserted: {total_upserted}, Failed: {total_failed}")
    logger.info("Finalizing and flushing logs to disk...")
    logging.shutdown()

if __name__ == "__main__":
    main() 