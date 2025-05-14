-- ------------------------------------------------------------
-- 20240513_add_columns_to_events_clean.sql
-- Add venue / address / event_day that we forgot in the first cut
-- ------------------------------------------------------------
alter table public.events_clean
    add column if not exists venue      text,
    add column if not exists address    text,
    add column if not exists event_day  date;

-- keep look-ups fast
create index if not exists idx_events_clean_event_day on public.events_clean(event_day); 