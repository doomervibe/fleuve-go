-- Rollback compression changes
DROP INDEX IF EXISTS idx_stored_events_pushed;
DROP INDEX IF EXISTS idx_stored_events_namespace;
ALTER TABLE stored_events DROP COLUMN IF EXISTS compressed;
ALTER TABLE stored_events DROP COLUMN IF EXISTS compressed_body;
