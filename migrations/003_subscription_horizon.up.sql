-- Subscription horizon: only deliver emitter events with workflow_version > after_emitter_event_no.
-- NULL = legacy behaviour (no horizon filter). Used with runner backfill on subscription_added.

ALTER TABLE subscriptions
    ADD COLUMN IF NOT EXISTS after_emitter_event_no BIGINT NULL;
