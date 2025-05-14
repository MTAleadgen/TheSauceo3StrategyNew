-- supabase/20240514_add_raw_date_columns.sql
ALTER TABLE events
  ADD COLUMN raw_start_date TEXT,
  ADD COLUMN raw_when TEXT; 