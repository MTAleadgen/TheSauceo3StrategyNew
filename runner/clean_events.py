import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from pipelines.serpapi.events.qwen_cleaner import enrich_event_with_llm
from pipelines.cleaner.transformer import transform_event_data

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
print("DEBUG: LAMBDA_TOKEN =", os.getenv("LAMBDA_TOKEN"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL and SUPABASE_KEY must be set in the environment or .env file.")
    sys.exit(1)

BATCH_SIZE = 100

def get_events(supabase: Client, offset: int, limit: int):
    try:
        response = supabase.table('events').select('*').range(offset, offset + limit - 1).execute()
        if hasattr(response, 'data'):
            return response.data
        return []
    except Exception as e:
        logger.error(f"Error fetching events from Supabase: {e}")
        return []

def upsert_event_clean(supabase: Client, event_clean: dict):
    print("UPSERT PAYLOAD:", event_clean)  # Debug print to show payload
    try:
        response = supabase.table('events_clean').upsert(event_clean, on_conflict='event_id').execute()
        if hasattr(response, 'data') and response.data:
            return True
        else:
            logger.error(f"Upsert to events_clean failed: {getattr(response, 'error', 'No error details')}")
            return False
    except Exception as e:
        logger.error(f"Error during upsert to events_clean: {e}")
        return False

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    offset = 0
    total_processed = 0
    total_upserted = 0
    total_failed = 0
    while True:
        events = get_events(supabase, offset, BATCH_SIZE)
        if not events:
            break
        logger.info(f"Processing batch: {offset} - {offset + len(events) - 1}")
        for event in events:
            try:
                # Step 1: Enrich event with LLM (description, live_band, class_before)
                if event.get('description'):
                    event = enrich_event_with_llm(event)
                # Step 2: Clean/transform event
                cleaned = transform_event_data(event)
                if not cleaned:
                    logger.warning(f"Event skipped by transformer: {event.get('id') or event.get('source_id')}")
                    total_failed += 1
                    continue
                # Step 3: Upsert to events_clean
                if upsert_event_clean(supabase, cleaned):
                    total_upserted += 1
                else:
                    total_failed += 1
                total_processed += 1
            except Exception as e:
                logger.error(f"Failed to process event {event.get('id') or event.get('source_id')}: {e}")
                total_failed += 1
        if len(events) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
    logger.info(f"Done. Processed: {total_processed}, Upserted: {total_upserted}, Failed: {total_failed}")

if __name__ == "__main__":
    main() 