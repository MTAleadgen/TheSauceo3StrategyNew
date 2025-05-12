# RSS Feeds Pipeline

This pipeline will be responsible for fetching and parsing RSS feeds from dance-related websites, blogs, or event aggregators that provide them.

## Expected Inputs

- A list of RSS feed URLs.
- Configuration for parsing common event patterns from feed item content (e.g., using regex or simple HTML parsing).

## Expected Outputs

- Event data extracted from feed items (title, link, description, publication date, potentially event date if parseable).
- Data structured to match the Supabase `events` table, or a separate table for less structured feed items (e.g., `feed_items`).

## Modules (Future)

- `feed_fetcher.py`: Fetches content from a list of RSS feed URLs (using libraries like `feedparser`).
- `content_parser.py`: Attempts to extract structured event information from feed item titles, descriptions, and content.
- `event_mapper.py`: Maps extracted data to the standard event schema.
- `db_writer.py`: Writes data to Supabase. 