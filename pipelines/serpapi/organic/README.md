# SerpAPI Organic Search Pipeline

This pipeline will be responsible for fetching and processing organic search results from SerpAPI for dance-related queries.

## Expected Inputs

- Search query parameters (e.g., keywords, location, language, region).
- City data (similar to `cities_shortlist.csv`) for location-based searches.

## Expected Outputs

- Parsed organic search results (e.g., title, link, snippet, source).
- Data structured for storage in a dedicated Supabase table (e.g., `organic_search_results`).

## Modules (Future)

- `request_builder.py`: Constructs SerpAPI parameters for the Google Organic Search engine.
- `parser.py`: Parses the JSON response from SerpAPI into a structured format.
- `data_cleaner.py`: Cleans and standardizes data (e.g., URLs, text snippets).
- `db_writer.py`: Handles inserting/upserting data into Supabase. 