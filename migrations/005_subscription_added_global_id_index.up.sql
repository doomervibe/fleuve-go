-- Partial index on subscription_added_global_id to speed up the findSubscriptions
-- filter: (subscription_added_global_id IS NULL OR $N >= subscription_added_global_id).
-- NULL rows are excluded from the index because the IS NULL branch cannot use a B-tree index anyway.
CREATE INDEX IF NOT EXISTS idx_subscriptions_added_global_id
    ON subscriptions (subscription_added_global_id)
    WHERE subscription_added_global_id IS NOT NULL;
