-- Rollback initial schema
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS workflow_search_attributes;
DROP TABLE IF EXISTS workflow_metadata;
DROP TABLE IF EXISTS scaling_operations;
DROP TABLE IF EXISTS snapshots;
DROP TABLE IF EXISTS delay_schedules;
DROP TABLE IF EXISTS activities;
DROP TABLE IF EXISTS external_subscriptions;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS offsets;
DROP TABLE IF EXISTS stored_events;
