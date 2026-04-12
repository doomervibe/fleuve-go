-- Track the global_id of the EvSubscriptionAdded event that created each subscription.
-- Used by findSubscriptions to prevent double-delivery: events with global_id less than
-- subscription_added_global_id are handled by backfill, not the live stream path.
ALTER TABLE subscriptions
    ADD COLUMN IF NOT EXISTS subscription_added_global_id BIGINT NULL;
