# Dedicated Event Platform APIs Pipeline

This directory will contain modules for directly integrating with specific event platform APIs (Eventbrite, Meetup, Ticketmaster, etc.).

## Expected Inputs

- API credentials for each platform.
- Search criteria (keywords like 'salsa', 'bachata', location/radius, date ranges).

## Expected Outputs

- Event data parsed from each API's response.
- Data standardized to match the Supabase `events` table schema.

## Modules (Future)

- `eventbrite.py`: Handles Eventbrite API calls and data parsing.
- `meetup.py`: Handles Meetup API calls and data parsing.
- `ticketmaster.py`: Handles Ticketmaster API calls and data parsing.
- `common.py`: (Optional) Shared utilities or data structures.
- `db_writer.py`: Handles writing standardized event data to Supabase. 