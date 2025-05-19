import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from collections import defaultdict
import string
from unidecode import unidecode

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

def canon(s):
    if not s:
        return ''
    return unidecode(s.lower()).translate(str.maketrans('', '', string.punctuation)).strip()

def canonical_key(evt):
    # 1) guaranteed-unique IDs from provider
    if evt.get('source_id'):
        return ('src', evt['source_id'])
    # 2) same URL from different scrapes
    if evt.get('source_url'):
        return ('url', canon(evt['source_url']))
    # 3) fallback composite
    return ('fuzzy', canon(evt.get('name', ''))[:60], canon(evt.get('venue', ''))[:60], evt.get('event_day'))

def choose_master(ev1, ev2):
    # Prefer the one with lat/lng, then earliest retrieved_at
    def has_latlng(ev):
        return bool(ev.get('lat')) and bool(ev.get('lng'))
    if has_latlng(ev1) and not has_latlng(ev2):
        return ev1, ev2
    if has_latlng(ev2) and not has_latlng(ev1):
        return ev2, ev1
    # Otherwise, keep the earliest retrieved_at
    try:
        if ev1.get('retrieved_at') and ev2.get('retrieved_at'):
            if ev1['retrieved_at'] <= ev2['retrieved_at']:
                return ev1, ev2
            else:
                return ev2, ev1
    except Exception:
        pass
    # Default: keep ev1
    return ev1, ev2

def flag_duplicate(supabase: Client, event_id):
    try:
        response = supabase.table('events').update({'is_duplicate': True}).eq('id', event_id).execute()
        if hasattr(response, 'data') and response.data:
            return True
        else:
            logger.warning(f"Failed to flag duplicate event {event_id}: {getattr(response, 'error', 'No error details')}")
            return False
    except Exception as e:
        logger.error(f"Error flagging duplicate event {event_id}: {e}")
        return False

def get_all_events(supabase: Client):
    try:
        response = supabase.table('events').select('*').execute()
        if hasattr(response, 'data'):
            return response.data
        return []
    except Exception as e:
        logger.error(f"Error fetching events from Supabase: {e}")
        return []

def check_is_duplicate_column(supabase: Client):
    # Try to update a dummy row to see if is_duplicate exists
    try:
        response = supabase.table('events').select('id, is_duplicate').limit(1).execute()
        if hasattr(response, 'data') and response.data:
            return True
        else:
            logger.warning("'is_duplicate' column not found in events table. Duplicates will not be flagged.")
            return False
    except Exception as e:
        logger.warning(f"Could not verify 'is_duplicate' column: {e}")
        return False

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    has_is_duplicate = check_is_duplicate_column(supabase)
    seen = dict()  # canonical_key -> master event
    total_flagged = 0
    total_checked = 0
    events = get_all_events(supabase)
    for event in events:
        key = canonical_key(event)
        if key in seen:
            master, dupe = choose_master(seen[key], event)
            if has_is_duplicate:
                if flag_duplicate(supabase, dupe['id']):
                    logger.info(f"Flagged duplicate event {dupe['id']} for key {key}")
                    total_flagged += 1
            else:
                logger.info(f"Would flag duplicate event {dupe['id']} for key {key}")
            seen[key] = master
        else:
            seen[key] = event
        total_checked += 1
    logger.info(f"Deduplication complete. Checked: {total_checked}, Flagged as duplicate: {total_flagged}")

if __name__ == "__main__":
    main() 