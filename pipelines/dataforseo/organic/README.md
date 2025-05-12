# DataForSEO Organic Search Pipeline

This pipeline will use the DataForSEO API to fetch and process organic search engine results (SERPs).

## Expected Inputs

- Keywords relevant to dance events, studios, instructors, etc.
- Location parameters (based on `cities_shortlist.csv`).
- Language/region parameters.
- DataForSEO API credentials.

## Expected Outputs

- Parsed organic search results (title, link, snippet).
- Structured data ready for insertion into a relevant Supabase table (e.g., `organic_search_results`).

## Modules (Future)

- `request_builder.py`: Constructs API requests/tasks for DataForSEO SERP endpoints.
- `response_handler.py`: Manages task submission and result retrieval.
- `parser.py`: Parses the SERP JSON data from DataForSEO.
- `data_cleaner.py`: Cleans URLs and text snippets.
- `db_writer.py`: Handles writing data to Supabase. 