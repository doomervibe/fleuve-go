-- event_type is used by ActionExecutor recovery and activity tracking.
ALTER TABLE workflow_activities ADD COLUMN IF NOT EXISTS event_type VARCHAR(255) NOT NULL DEFAULT '';
