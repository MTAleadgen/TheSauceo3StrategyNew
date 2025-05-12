# JSON Scrapers Pipeline

This pipeline is intended for scenarios where websites embed event data directly as JSON within their HTML (e.g., in `<script type="application/ld+json">` tags or JavaScript variables).

## Expected Inputs

- Target website URLs.
- Selectors or patterns to locate the JSON data within the page source.
- Schemas or mapping rules for the expected JSON structures.

## Expected Outputs

- Event data parsed from the embedded JSON.
- Data standardized to match the Supabase `events` table schema.

## Modules (Future)

- `page_fetcher.py`: Fetches HTML content from target URLs (using libraries like `requests` or `httpx`).
- `json_extractor.py`: Uses selectors (e.g., `BeautifulSoup`, `lxml`, regex) to find and extract JSON strings from HTML.
- `schema_parser.py`: Parses the extracted JSON based on known schemas (e.g., Schema.org event types) or custom mappings.
- `event_mapper.py`: Maps the parsed JSON data to the standard event schema.
- `db_writer.py`: Writes data to Supabase. 