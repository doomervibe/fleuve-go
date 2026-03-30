-- Add compressed_body column for large payloads (optional optimization)
ALTER TABLE stored_events ADD COLUMN IF NOT EXISTS compressed_body BYTEA;
ALTER TABLE stored_events ADD COLUMN IF NOT EXISTS compressed BOOLEAN DEFAULT FALSE;

-- Add index for namespace queries
CREATE INDEX IF NOT EXISTS idx_stored_events_namespace ON stored_events (namespace) WHERE namespace IS NOT NULL;

-- Add index for pushed events (outbox pattern)
CREATE INDEX IF NOT EXISTS idx_stored_events_pushed ON stored_events (pushed, workflow_type) WHERE pushed = FALSE;
