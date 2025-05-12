# DataForSEO Events Pipeline

This pipeline will leverage the DataForSEO API (specifically endpoints related to events or local search results containing events) to gather dance event information.

## Expected Inputs

- City/location data (`cities_shortlist.csv`).
- Dance-related keywords.
- DataForSEO API credentials.

## Expected Outputs

- Parsed event data (name, description, venue, address, date/time, price, source, etc.).
- Data formatted to match the `events` table schema in Supabase.

## Modules (Future)

- `request_builder.py`: Constructs API requests/tasks for DataForSEO event-related endpoints.
- `response_handler.py`: Handles polling or receiving results from DataForSEO tasks.
- `parser.py`: Parses the JSON response(s) from DataForSEO into the standard event format.
- `places_enricher_dfs.py`: (Optional) Potentially adapt Places enrichment if needed for DataForSEO results.
- `db_writer.py`: Handles inserting/upserting data into the Supabase `events` table. 