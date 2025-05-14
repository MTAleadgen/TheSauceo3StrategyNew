import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from collections import defaultdict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

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

def delete_event(supabase: Client, event_id):
    try:
        response = supabase.table('events').delete().eq('id', event_id).execute()
        if hasattr(response, 'data') and response.data:
            return True
        else:
            logger.error(f"Failed to delete event {event_id}: {getattr(response, 'error', 'No error details')}")
            return False
    except Exception as e:
        logger.error(f"Error deleting event {event_id}: {e}")
        return False

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    offset = 0
    seen = defaultdict(list)  # (event_day, address) -> list of event dicts
    total_deleted = 0
    total_checked = 0
    # First, fetch all events in batches
    while True:
        events = get_events(supabase, offset, BATCH_SIZE)
        if not events:
            break
        for event in events:
            key = (event.get('event_day'), event.get('address'))
            seen[key].append(event)
            total_checked += 1
        if len(events) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
    # Now, for each group with duplicates, keep one and delete the rest
    for key, event_list in seen.items():
        if len(event_list) > 1:
            # Sort by id (or any other logic), keep the first
            event_list = sorted(event_list, key=lambda e: e.get('id'))
            to_delete = event_list[1:]
            for ev in to_delete:
                if delete_event(supabase, ev.get('id')):
                    logger.info(f"Deleted duplicate event {ev.get('id')} for day {key[0]} and address {key[1]}")
                    total_deleted += 1
    logger.info(f"Deduplication complete. Checked: {total_checked}, Deleted: {total_deleted}")

if __name__ == "__main__":
    main() 