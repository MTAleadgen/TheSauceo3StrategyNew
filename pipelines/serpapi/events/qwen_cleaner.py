import os
import json
import logging
from typing import Any, Dict, Optional
from dotenv import load_dotenv

# --- Placeholder for AI Model Interaction ---
# You will need to replace this section with the actual implementation
# for calling your chosen AI model (Qwen or other).
# This might involve importing specific libraries (e.g., requests, openai)
# and handling authentication (API keys).

# Example placeholder function:
def call_ai_model(prompt: str, temperature: float, max_tokens: int, stop_sequence: str) -> Optional[str]:
    """Placeholder function to simulate calling an AI model."""
    logging.warning("Using placeholder AI model call. Replace with actual implementation.")
    # Simulate a successful response structure for testing:
    # In a real scenario, this would make an API call.
    if "rewrite description" in prompt.lower():
        # Simulate extracting the JSON part based on the stop sequence
        simulated_response_json = json.dumps({"rewritten_description": "Placeholder rewritten description based on input."})
        return simulated_response_json # The raw string response from the model
    return None 
    # Example using requests (replace with actual endpoint/auth):
    # try:
    #     api_key = os.getenv("YOUR_AI_MODEL_API_KEY")
    #     headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
    #     payload = {
    #         'model': 'qwen-some-model', # Replace with actual model ID
    #         'prompt': prompt,
    #         'temperature': temperature,
    #         'max_tokens': max_tokens,
    #         'stop': [stop_sequence]
    #     }
    #     response = requests.post("YOUR_AI_MODEL_ENDPOINT", headers=headers, json=payload, timeout=30)
    #     response.raise_for_status()
    #     # Parse the response according to your AI provider's format
    #     # This example assumes the response JSON has a 'choices' list like OpenAI
    #     result_json = response.json()
    #     if result_json.get('choices') and len(result_json['choices']) > 0:
    #         return result_json['choices'][0].get('text') # Or appropriate key
    #     else:
    #         logging.error(f"AI model response format unexpected: {result_json}")
    #         return None
    # except requests.exceptions.RequestException as e:
    #     logging.error(f"AI model API request failed: {e}")
    #     return None
    # except Exception as e:
    #     logging.error(f"Error calling AI model: {e}")
    #     return None

# --- End Placeholder --- 

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables (potentially for AI API key)
load_dotenv()

PROMPT_TMPL = """<task>
You are a data-cleaning assistant. Given JSON describing a dance event,
rewrite the "description" field so that it
  • removes duplicate information already present in other keys
  • is ≤300 chars
  • is safe for publishing (no personal data, no phone numbers)
Return ONLY valid JSON with the key "rewritten_description".
</task>
<json>{json_blob}</json>
<result>"""

def rewrite_description(event: Dict[str, Any]) -> Optional[str]:
    """
    Rewrites the event description using an AI model based on the provided prompt template.

    Args:
        event: The event dictionary (output from the parser/enricher).

    Returns:
        The rewritten description string, or None if the rewrite fails or the
        original description is empty.
    """
    
    original_description = event.get('description')
    if not original_description:
        logging.info("Original description is empty, skipping rewrite.")
        return None # No description to rewrite

    # Create a JSON blob of the event data for the prompt context
    # Exclude potentially large or sensitive fields if necessary before dumping
    try:
        # Select relevant fields for context (avoid excessively large blobs)
        context_event = {
            k: v for k, v in event.items() 
            if k not in ['rewritten_description'] and v is not None # Exclude previous result and None values
        }
        json_blob = json.dumps(context_event, indent=2, ensure_ascii=False)
    except TypeError as e:
        logging.error(f"Failed to serialize event to JSON for AI prompt: {e}")
        return None

    # Construct the prompt
    prompt = PROMPT_TMPL.format(json_blob=json_blob)

    # AI Model parameters
    temperature = 0.2
    max_tokens = 512 # Sufficient for the short JSON response expected
    stop_sequence = "</result>" # Model should stop generating here

    logging.info(f"Requesting description rewrite for event: {event.get('source_id') or event.get('name')}")
    
    # Call the (placeholder) AI model
    model_response_raw = call_ai_model(
        prompt=prompt, 
        temperature=temperature, 
        max_tokens=max_tokens, 
        stop_sequence=stop_sequence
    )

    if not model_response_raw:
        logging.warning("AI model did not return a response.")
        return None

    # Attempt to parse the JSON response from the model
    try:
        # The model should return only the JSON part, e.g., { "rewritten_description": "..." }
        # Remove potential leading/trailing whitespace or markdown formatting
        cleaned_response = model_response_raw.strip().strip('```json').strip('```').strip()
        
        # Ensure the response starts and ends with curly braces for valid JSON
        if not (cleaned_response.startswith('{') and cleaned_response.endswith('}')):
            logging.warning(f"AI model response is not valid JSON format: {cleaned_response}")
            # Attempt to find JSON within the string if possible (basic heuristic)
            start_index = cleaned_response.find('{')
            end_index = cleaned_response.rfind('}')
            if start_index != -1 and end_index != -1 and start_index < end_index:
                cleaned_response = cleaned_response[start_index : end_index + 1]
            else:
                return None # Cannot reliably parse

        parsed_json = json.loads(cleaned_response)
        
        if isinstance(parsed_json, dict) and 'rewritten_description' in parsed_json:
            rewritten = parsed_json['rewritten_description']
            if isinstance(rewritten, str):
                logging.info("Successfully rewrote description.")
                return rewritten
            else:
                logging.warning(f"'rewritten_description' key found but value is not a string: {rewritten}")
                return None
        else:
            logging.warning(f"Parsed JSON from AI model lacks 'rewritten_description' key: {parsed_json}")
            return None

    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON response from AI model: {e}")
        logging.debug(f"Raw model response: {model_response_raw}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during AI response processing: {e}")
        return None

# Example Usage:
if __name__ == '__main__':
    # Example event (potentially output from parser/enricher)
    sample_event = {
        'source_platform': 'serpapi_google_events',
        'source_id': 'test_event_789',
        'name': 'Weekly Zouk Practice',
        'description': 'Come practice Zouk every Wednesday at The Studio. Address is 123 Dance Ave. Perfect for beginners and intermediate dancers. Call 555-1234 for info. Starts at 8 PM sharp! Venue: The Studio.',
        'venue': 'The Studio',
        'address': '123 Dance Ave, Danceville',
        'city': 'Danceville',
        'country': 'US',
        'lat': 40.123,
        'lng': -74.456,
        'dance_styles': ['zouk'],
        'price': 'Free',
        'start_time': 'Wed, 8 PM',
        'end_time': None,
        'event_day': '2023-11-15',
        'live_band': False,
        'class_before': False
    }

    print("--- Rewriting Description --- (Using Placeholder Model)")
    rewritten_desc = rewrite_description(sample_event)

    if rewritten_desc:
        print(f"Original Description:\n  {sample_event['description']}")
        print(f"Rewritten Description:\n  {rewritten_desc}")
    else:
        print("Failed to rewrite description or original was empty.")

    print("\n--- Rewriting Empty Description --- (Using Placeholder Model)")
    sample_event_no_desc = sample_event.copy()
    sample_event_no_desc['description'] = None
    rewritten_no_desc = rewrite_description(sample_event_no_desc)
    if rewritten_no_desc:
         print(f"Rewritten Description (Should not happen):\n  {rewritten_no_desc}")
    else:
        print("Correctly skipped rewrite for empty original description.") 