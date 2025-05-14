import os, json, time, backoff, requests
import logging # Added for logging within the module

LAMBDA_ENDPOINT = os.getenv("LAMBDA_QWEN_URL")     # e.g. https://a10.api.lambda.ai/v1/chat/completions
LAMBDA_TOKEN    = os.getenv("LAMBDA_TOKEN")        # your "Bearer ..." key
TIMEOUT         = int(os.getenv("LAMBDA_TIMEOUT", 60))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Use module-specific logger

HEADERS = None
if LAMBDA_TOKEN:
    HEADERS = {
        "Authorization": f"Bearer {LAMBDA_TOKEN}",
        "Content-Type": "application/json"
    }
else:
    logger.warning("LAMBDA_TOKEN environment variable not set. LLM Cleaner will be skipped.")


SYSTEM_PROMPT = (
    "You are an editor.  Rewrite the event description so it is:\n"
    "• concise (≤ 120 words)\n"
    "• written in clear conversational English\n"
    "• no ALL-CAPS, no unnecessary emojis\n"
    "Return **only** the rewritten text."
)

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException,),
                      max_tries=5, max_time=120)
def _call_llm(prompt: str) -> str:
    if not LAMBDA_ENDPOINT or not HEADERS:
         raise ValueError("Lambda endpoint URL or token not configured.")
         
    payload = {
        "model": "Qwen3-32B-Chat",        # required by Lambda
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt.strip()}
        ],
        "temperature": 0.7,
        "max_tokens": 256, # Max tokens the model can generate
    }
    logger.info(f"Calling Lambda LLM: {LAMBDA_ENDPOINT} with model {payload['model']}")
    r = requests.post(LAMBDA_ENDPOINT, headers=HEADERS,
                      json=payload, timeout=TIMEOUT)
    
    # Log request details on error for debugging
    if r.status_code >= 400:
        logger.error(f"LLM API Error: Status={r.status_code}, Response={r.text}")
        
    r.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
    data = r.json()
    logger.debug(f"Raw LLM response: {data}")

    # Lambda's schema mirrors OpenAI; adjust if your deployment is different
    try:
        content = data["choices"][0]["message"]["content"].strip()
        # Simple check for empty/placeholder response
        if not content or len(content) < 5: 
             logger.warning(f"LLM returned short or empty content: '{content}'")
             # Decide whether to return empty or raise error
             return "" # Return empty string for now
             
        # Optional: Add length check here based on prompt constraint?
        # The prompt asks for <= 120 words, max_tokens=256 is a fallback.
        # Post-processing could enforce word count if needed.
        return content
        
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Error parsing LLM response: {e}. Response: {data}")
        raise ValueError("Failed to extract text from LLM response.") from e


# ----------------------------------------------------------------------
def rewrite_description(event: dict) -> dict:
    """Return a clone of *event* with `rewritten_description` filled in."""
    
    # Check prerequisites
    if not LAMBDA_ENDPOINT or not HEADERS:
         logger.warning("Skipping LLM rewrite: Endpoint or Token not configured.")
         # Ensure field exists but is None if skipped
         event_copy = event.copy()
         event_copy["rewritten_description"] = None 
         return event_copy
         
    original_description = event.get("description")
    if not original_description or not isinstance(original_description, str) or not original_description.strip():
        logger.debug(f"Skipping LLM rewrite: No valid original description found for event {event.get('source_id') or event.get('name')}")
        # Ensure field exists but is None if skipped
        event_copy = event.copy()
        event_copy["rewritten_description"] = None
        return event_copy

    event_copy = event.copy() # Start with a copy
    try:
        logger.info(f"Rewriting description for event: {event_copy.get('source_id') or event_copy.get('name')}")
        rewritten = _call_llm(original_description)
        
        if rewritten:
            # Only update if we got a non-empty response
            event_copy["rewritten_description"] = rewritten 
            logger.info(f"Successfully rewritten description for event: {event_copy.get('source_id') or event_copy.get('name')}")
        else:
            # If _call_llm returned empty string due to empty response
            event_copy["rewritten_description"] = None
            logger.warning(f"LLM returned empty description for event: {event_copy.get('source_id') or event_copy.get('name')}")

    except ValueError as e: # Catch config or parsing errors from _call_llm
         logger.error(f"Configuration or parsing error during LLM rewrite for event {event_copy.get('source_id')}: {e}")
         event_copy["rewritten_description"] = None # Set to None on failure
    except requests.exceptions.RequestException as e: # Catch network/HTTP errors after retries
        logger.error(f"HTTP/Network error calling LLM for event {event_copy.get('source_id')}: {e}")
        event_copy["rewritten_description"] = None # Set to None on failure
    except Exception as exc: # Catch any other unexpected errors
        # leave the original description, but log for later inspection
        logger.error(f"[qwen_cleaner] LLM call failed unexpectedly for event {event_copy.get('source_id')}: {exc}", exc_info=True)
        event_copy["rewritten_description"] = None # Set to None on failure

    return event_copy

# Example Usage (Optional - requires LAMBDA_QWEN_URL and LAMBDA_TOKEN in env)
if __name__ == '__main__':
    # Load .env file for local testing if available
    from dotenv import load_dotenv
    load_dotenv()
    
    # Reload env vars after dotenv load
    LAMBDA_ENDPOINT = os.getenv("LAMBDA_QWEN_URL")
    LAMBDA_TOKEN    = os.getenv("LAMBDA_TOKEN")
    if LAMBDA_TOKEN:
        HEADERS = {
            "Authorization": f"Bearer {LAMBDA_TOKEN}",
            "Content-Type": "application/json"
        }
    else:
        HEADERS = None

    if not LAMBDA_ENDPOINT or not HEADERS:
         print("LAMBDA_QWEN_URL or LAMBDA_TOKEN not set in environment. Cannot run example.")
    else:
        print(f"LAMBDA_QWEN_URL and LAMBDA_TOKEN found. Running example against: {LAMBDA_ENDPOINT}")
        sample_event = {
            'source_id': 'example1',
            'name': 'Test Event',
            'description': 'Join us for an amazing event this weekend! Lots of fun activities planned. Email contact@example.com or call 555-1234 for details. Location: 123 Main St. Time: 8 PM. COME ONE COME ALL!!! 😊🚀🎉'
        }
        
        sample_event_empty_desc = {
            'source_id': 'example2',
            'name': 'Event with no description',
            'description': ''
        }
        
        sample_event_no_desc_key = {
            'source_id': 'example3',
            'name': 'Event missing description key'
        }


        print("\n--- Rewriting description for sample_event ---")
        start_t = time.time()
        rewritten_event = rewrite_description(sample_event) # No need to copy here, function does it
        end_t = time.time()
        print(f"LLM Call took: {end_t - start_t:.2f}s")
        print("Original Description:", sample_event.get('description'))
        print("Rewritten Description:", rewritten_event.get('rewritten_description'))

        print("\n--- Rewriting description for sample_event_empty_desc ---")
        rewritten_event_empty = rewrite_description(sample_event_empty_desc)
        print("Original Description:", sample_event_empty_desc.get('description'))
        print("Rewritten Description:", rewritten_event_empty.get('rewritten_description'))
        
        print("\n--- Rewriting description for sample_event_no_desc_key ---")
        rewritten_event_no_desc = rewrite_description(sample_event_no_desc_key)
        print("Original Description:", sample_event_no_desc_key.get('description'))
        print("Rewritten Description:", rewritten_event_no_desc.get('rewritten_description'))


        # Example of how an error might be handled (simulating bad endpoint)
        print("\n--- Testing Error Handling (Simulating Bad Endpoint) ---")
        original_endpoint = LAMBDA_ENDPOINT
        LAMBDA_ENDPOINT = "http://invalid-url-that-does-not-exist" 
        try:
             error_event = rewrite_description(sample_event)
             print("Rewritten Description (should be None):", error_event.get('rewritten_description'))
        except Exception as e:
            print(f"Caught expected exception: {e}") # Backoff might retry
        finally:    
            LAMBDA_ENDPOINT = original_endpoint # Restore endpoint
            
        print("\n--- Testing Error Handling (Simulating Bad Token) ---")
        original_headers = HEADERS
        HEADERS = {
            "Authorization": f"Bearer BAD_TOKEN",
            "Content-Type": "application/json"
        }
        try:
             error_event_token = rewrite_description(sample_event)
             print("Rewritten Description (should be None):", error_event_token.get('rewritten_description'))
        except Exception as e:
             print(f"Caught expected exception (might be HTTP 401/403 after retries): {e}")
        finally:
            HEADERS = original_headers # Restore headers 