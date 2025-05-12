import os, json, httpx, backoff
from typing import Dict, Any
import logging

LAMBDA_ENDPOINT = "https://api.endpoints.anyscale.com/v1/run"   # example
# Attempt to load API Key, handle missing key gracefully
API_KEY = None
try:
    # Assuming LAMBDA_API_KEY is the correct env var name based on your snippet
    API_KEY = os.environ["LAMBDA_API_KEY"] 
except KeyError:
    logging.warning("LAMBDA_API_KEY environment variable not set. Qwen Cleaner will be skipped.")

PROMPT_TMPL = """
Rewrite the event description for brevity, clarity, and safety.
MAX 240 CHARACTERS. NO PII (emails, phone numbers).

Original Description:
"{description}"

Rewritten Description:
"""

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@backoff.on_exception(backoff.expo, httpx.HTTPError, max_tries=5)
def call_ai_model(prompt: str) -> str:
    if not API_KEY or not LAMBDA_ENDPOINT:
        raise ValueError("API Key or Endpoint not configured for AI model call.")
    
    headers = {"Authorization": f"Bearer {API_KEY}"}
    # Adjust payload based on your actual lambda endpoint requirements
    payload = { 
        "model": "meta-llama/Meta-Llama-3-8B-Instruct", # Example model - adjust if needed
        "messages": [{"role": "user", "content": prompt}], # Example chat structure
        "temperature": 0.7 # Example parameter
    }
    logging.info(f"Calling AI model at {LAMBDA_ENDPOINT}")
    resp = httpx.post(LAMBDA_ENDPOINT, json=payload, headers=headers, timeout=60)
    resp.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
    
    # Parse the response - **adjust based on your lambda's actual return structure**
    response_data = resp.json()
    logging.debug(f"Raw AI response: {response_data}") # Log raw response for debugging
    
    # Example: Accessing text from a common chat completion structure
    try:
        # This structure depends heavily on the Anyscale endpoint's specific response format
        # Check the Anyscale documentation for the exact path to the generated text
        rewritten_text = response_data['choices'][0]['message']['content'] 
        return rewritten_text.strip()
    except (KeyError, IndexError, TypeError) as e:
        logging.error(f"Error parsing AI model response: {e}. Response: {response_data}")
        raise ValueError("Failed to extract rewritten text from AI model response.") from e


def rewrite_description(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Rewrites the event description using an AI model via an endpoint.
    Adds the rewritten description to event['rewritten_description'].
    Skips if the original description is empty or API key is missing.
    """
    original_description = event.get('description')

    if not API_KEY:
        logging.warning("Skipping description rewrite: LAMBDA_API_KEY not set.")
        event['rewritten_description'] = None
        return event
        
    if not original_description:
        logging.info(f"Skipping description rewrite: Original description is empty for event {event.get('source_id') or event.get('name')}")
        event['rewritten_description'] = None # Ensure the field exists, even if null
        return event

    prompt = PROMPT_TMPL.format(description=original_description)
    
    try:
        logging.info(f"Rewriting description for event: {event.get('source_id') or event.get('name')}")
        rewritten_desc = call_ai_model(prompt)
        # Simple post-processing: ensure max length (though prompt requests it)
        event['rewritten_description'] = rewritten_desc[:240] 
        logging.info(f"Successfully rewritten description for event: {event.get('source_id') or event.get('name')}")
    except ValueError as e: # Catch configuration errors or parsing errors from call_ai_model
         logging.error(f"Configuration or parsing error during description rewrite for event {event.get('source_id')}: {e}")
         event['rewritten_description'] = None # Set to None on failure
    except httpx.HTTPError as e:
        logging.error(f"HTTP error calling AI model for event {event.get('source_id')}: {e}")
        event['rewritten_description'] = None # Set to None on failure
    except Exception as e: # Catch other unexpected errors like timeouts after retries
        logging.error(f"Unexpected error during description rewrite for event {event.get('source_id')}: {e}", exc_info=True)
        event['rewritten_description'] = None # Set to None on failure

    return event

# Example Usage (Optional - requires LAMBDA_API_KEY in env)
if __name__ == '__main__':
    # Load .env file for local testing if available
    from dotenv import load_dotenv
    load_dotenv()
    
    # Update API_KEY after loading .env
    API_KEY = os.getenv("LAMBDA_API_KEY") 

    if not API_KEY:
         print("LAMBDA_API_KEY not set in environment. Cannot run example.")
    else:
        print("LAMBDA_API_KEY found. Running example.")
        sample_event = {
            'source_id': 'example1',
            'name': 'Test Event',
            'description': 'Join us for an amazing event this weekend! Lots of fun activities planned. Email contact@example.com or call 555-1234 for details. Location: 123 Main St. Time: 8 PM.'
        }
        
        sample_event_empty_desc = {
            'source_id': 'example2',
            'name': 'Event with no description',
            'description': ''
        }

        print("\n--- Rewriting description for sample_event ---")
        rewritten_event = rewrite_description(sample_event.copy())
        print("Original Description:", sample_event['description'])
        print("Rewritten Description:", rewritten_event.get('rewritten_description'))

        print("\n--- Rewriting description for sample_event_empty_desc ---")
        rewritten_event_empty = rewrite_description(sample_event_empty_desc.copy())
        print("Original Description:", sample_event_empty_desc['description'])
        print("Rewritten Description:", rewritten_event_empty.get('rewritten_description'))

        # Example of how an error might be handled
        print("\n--- Testing Error Handling (Simulating API Key Error) ---")
        original_key = API_KEY
        API_KEY = None # Temporarily unset key
        error_event = rewrite_description(sample_event.copy())
        print("Rewritten Description (should be None):", error_event.get('rewritten_description'))
        API_KEY = original_key # Restore key 