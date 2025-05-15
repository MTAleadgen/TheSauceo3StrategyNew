import os
from dotenv import load_dotenv
load_dotenv()
print("DEBUG: Using model", os.getenv("LAMBDA_QWEN_MODEL"))
import json, time, backoff, requests
import logging # Added for logging within the module
import re # Added for regular expression operations
print("DEBUG (qwen_cleaner): LAMBDA_TOKEN =", os.getenv("LAMBDA_TOKEN"))

LAMBDA_ENDPOINT = os.getenv("LAMBDA_QWEN_URL")     # e.g. https://a10.api.lambda.ai/v1/chat/completions
LAMBDA_TOKEN    = os.getenv("LAMBDA_TOKEN")        # your "Bearer ..." key
TIMEOUT         = int(os.getenv("LAMBDA_TIMEOUT", 60))

print("DEBUG (qwen_cleaner): LAMBDA_TOKEN (after assignment) =", LAMBDA_TOKEN)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Use module-specific logger

HEADERS = None
if LAMBDA_TOKEN:
    HEADERS = {
        "Authorization": f"Bearer {LAMBDA_TOKEN}",
        "Content-Type": "application/json"
    }
    print("DEBUG (qwen_cleaner): HEADERS =", HEADERS)
else:
    logger.warning("LAMBDA_TOKEN environment variable not set. LLM Cleaner will be skipped.")


SYSTEM_PROMPT = (
    "You are an event data processor. For the event below, do the following:\n"
    "1. Rewrite the event description to be concise (≤ 120 words), clear, and conversational. No ALL-CAPS, no unnecessary emojis.\n"
    "2. Decide if the event features a live band (true/false).\n"
    "3. Decide if there is a class or lesson before the main event (true/false).\n"
    "4. Extract the event price ONLY if it is 'Free' or a number with a $ or other currency symbol in front of it (e.g., '$5', '€10', 'R$20', '£8', or 'Free'). If no such price is found, return null. Do NOT return numbers without a currency symbol.\n"
    "Return your answer as a JSON object with these fields:\n"
    "{\n"
    '  "rewritten_description": "...",\n'
    '  "live_band": true/false,\n'
    '  "class_before": true/false,\n'
    '  "price": "..." // string or null\n'
    "}"
)

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException,),
                      max_tries=5, max_time=120)
def _call_llm(prompt: str) -> str:
    if not LAMBDA_ENDPOINT or not HEADERS:
         raise ValueError("Lambda endpoint URL or token not configured.")
         
    payload = {
        "model": os.getenv("LAMBDA_QWEN_MODEL", "qwen25-coder-32b-instruct"),
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
def enrich_event_with_llm(event: dict) -> dict:
    if not LAMBDA_ENDPOINT or not HEADERS:
        logger.warning("Skipping LLM enrichment: Endpoint or Token not configured.")
        event_copy = event.copy()
        event_copy["description"] = event.get("description")
        event_copy["live_band"] = None
        event_copy["class_before"] = None
        event_copy["name"] = event.get("name")
        event_copy["price"] = event.get("price")
        return event_copy

    user_message = f"Event title: {event.get('name', '')}\nEvent description: {event.get('description', '')}"
    payload = {
        "model": os.getenv("LAMBDA_QWEN_MODEL", "qwen25-coder-32b-instruct"),
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ],
        "temperature": 0.7,
        "max_tokens": 512,
    }
    r = requests.post(LAMBDA_ENDPOINT, headers=HEADERS, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    content = data["choices"][0]["message"]["content"].strip()
    try:
        llm_result = json.loads(content)
        event_copy = event.copy()
        event_copy["description"] = llm_result.get("rewritten_description")
        event_copy["live_band"] = llm_result.get("live_band")
        event_copy["class_before"] = llm_result.get("class_before")
        # Post-process price to ensure only valid values are accepted
        price = llm_result.get("price")
        if isinstance(price, str):
            price = price.strip()
            if not (price.lower() == "free" or re.match(r"^(r\$|us?\$|\$|€|£)\s*\d+", price, re.I)):
                price = None
        elif price is not None:
            price = str(price)
            if not (price.lower() == "free" or re.match(r"^(r\$|us?\$|\$|€|£)\s*\d+", price, re.I)):
                price = None
        event_copy["price"] = price
        event_copy["name"] = event.get("name")
        return event_copy
    except Exception as e:
        logger.error(f"Failed to parse LLM JSON: {e}, content: {content}")
        event_copy = event.copy()
        event_copy["description"] = event.get("description")
        event_copy["live_band"] = None
        event_copy["class_before"] = None
        event_copy["price"] = event.get("price")
        event_copy["name"] = event.get("name")
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
        rewritten_event = enrich_event_with_llm(sample_event) # No need to copy here, function does it
        end_t = time.time()
        print(f"LLM Call took: {end_t - start_t:.2f}s")
        print("Original Description:", sample_event.get('description'))
        print("Rewritten Description:", rewritten_event.get('description'))

        print("\n--- Rewriting description for sample_event_empty_desc ---")
        rewritten_event_empty = enrich_event_with_llm(sample_event_empty_desc)
        print("Original Description:", sample_event_empty_desc.get('description'))
        print("Rewritten Description:", rewritten_event_empty.get('description'))
        
        print("\n--- Rewriting description for sample_event_no_desc_key ---")
        rewritten_event_no_desc = enrich_event_with_llm(sample_event_no_desc_key)
        print("Original Description:", sample_event_no_desc_key.get('description'))
        print("Rewritten Description:", rewritten_event_no_desc.get('description'))


        # Example of how an error might be handled (simulating bad endpoint)
        print("\n--- Testing Error Handling (Simulating Bad Endpoint) ---")
        original_endpoint = LAMBDA_ENDPOINT
        LAMBDA_ENDPOINT = "http://invalid-url-that-does-not-exist" 
        try:
             error_event = enrich_event_with_llm(sample_event)
             print("Rewritten Description (should be None):", error_event.get('description'))
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
             error_event_token = enrich_event_with_llm(sample_event)
             print("Rewritten Description (should be None):", error_event_token.get('description'))
        except Exception as e:
             print(f"Caught expected exception (might be HTTP 401/403 after retries): {e}")
        finally:
            HEADERS = original_headers # Restore headers 