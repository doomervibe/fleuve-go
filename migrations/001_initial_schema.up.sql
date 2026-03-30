-- Initial schema for Fleuve
-- Compatible with Python implementation

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Stored events table (primary event store)
CREATE TABLE IF NOT EXISTS stored_events (
    global_id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    workflow_version BIGINT NOT NULL,
    namespace VARCHAR(255),
    event_type VARCHAR(255) NOT NULL,
    workflow_type VARCHAR(255) NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    body JSONB NOT NULL,
    at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    pushed BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT stored_events_workflow_version_unique UNIQUE (workflow_id, workflow_version)
);

CREATE INDEX IF NOT EXISTS idx_stored_events_workflow_id ON stored_events (workflow_id);
CREATE INDEX IF NOT EXISTS idx_stored_events_workflow_type ON stored_events (workflow_type);
CREATE INDEX IF NOT EXISTS idx_stored_events_event_type ON stored_events (event_type);
CREATE INDEX IF NOT EXISTS idx_stored_events_at ON stored_events (at);
CREATE INDEX IF NOT EXISTS idx_stored_events_global_id ON stored_events (global_id);

-- Offsets table (for stream readers)
CREATE TABLE IF NOT EXISTS offsets (
    reader VARCHAR(255) PRIMARY KEY,
    last_read_event_no BIGINT NOT NULL DEFAULT 0,
    namespace VARCHAR(255),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Subscriptions table
CREATE TABLE IF NOT EXISTS subscriptions (
    id SERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    workflow_type VARCHAR(255) NOT NULL,
    subscribed_to_workflow VARCHAR(255) NOT NULL,
    subscribed_to_event_type VARCHAR(255) NOT NULL,
    tags TEXT[] DEFAULT '{}',
    tags_all TEXT[] DEFAULT '{}',
    namespace VARCHAR(255),
    CONSTRAINT subscriptions_unique UNIQUE (workflow_id, subscribed_to_workflow, subscribed_to_event_type)
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_workflow_id ON subscriptions (workflow_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_subscribed_to ON subscriptions (subscribed_to_workflow, subscribed_to_event_type);

-- External subscriptions table
CREATE TABLE IF NOT EXISTS external_subscriptions (
    id SERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    workflow_type VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    CONSTRAINT external_subscriptions_unique UNIQUE (workflow_id, topic)
);

CREATE INDEX IF NOT EXISTS idx_external_subscriptions_topic ON external_subscriptions (topic);

-- Activities table (action execution tracking)
CREATE TABLE IF NOT EXISTS activities (
    id SERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    event_number BIGINT NOT NULL,
    workflow_type VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    checkpoint JSONB DEFAULT '{}',
    retry_policy JSONB DEFAULT '{}',
    error_message TEXT,
    error_type VARCHAR(255),
    result BYTEA,
    runner_id VARCHAR(255),
    CONSTRAINT activities_unique UNIQUE (workflow_id, event_number)
);

CREATE INDEX IF NOT EXISTS idx_activities_workflow_id ON activities (workflow_id);
CREATE INDEX IF NOT EXISTS idx_activities_status ON activities (status);
CREATE INDEX IF NOT EXISTS idx_activities_runner_id ON activities (runner_id);
CREATE INDEX IF NOT EXISTS idx_activities_status_last_attempt ON activities (status, last_attempt_at);

-- Delay schedules table
CREATE TABLE IF NOT EXISTS delay_schedules (
    id SERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    delay_id VARCHAR(255) NOT NULL,
    workflow_type VARCHAR(255) NOT NULL,
    delay_until TIMESTAMP WITH TIME ZONE NOT NULL,
    event_version BIGINT NOT NULL,
    cron_expression VARCHAR(255),
    timezone VARCHAR(64) DEFAULT 'UTC',
    next_command JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT delay_schedules_unique UNIQUE (workflow_id, delay_id)
);

CREATE INDEX IF NOT EXISTS idx_delay_schedules_delay_until ON delay_schedules (delay_until);
CREATE INDEX IF NOT EXISTS idx_delay_schedules_workflow_id ON delay_schedules (workflow_id);

-- Snapshots table (for event truncation support)
CREATE TABLE IF NOT EXISTS snapshots (
    workflow_id VARCHAR(255) PRIMARY KEY,
    workflow_type VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    state JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshots_workflow_type ON snapshots (workflow_type);

-- Scaling operations table
CREATE TABLE IF NOT EXISTS scaling_operations (
    workflow_type VARCHAR(255) PRIMARY KEY,
    target_offset BIGINT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Workflow metadata table (tags, search attributes)
CREATE TABLE IF NOT EXISTS workflow_metadata (
    workflow_id VARCHAR(255) PRIMARY KEY,
    workflow_type VARCHAR(255) NOT NULL,
    tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_metadata_workflow_type ON workflow_metadata (workflow_type);

-- Workflow search attributes table
CREATE TABLE IF NOT EXISTS workflow_search_attributes (
    workflow_id VARCHAR(255) PRIMARY KEY,
    workflow_type VARCHAR(255) NOT NULL,
    attributes JSONB DEFAULT '{}',
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_search_attributes_gin ON workflow_search_attributes USING GIN (attributes);
CREATE INDEX IF NOT EXISTS idx_workflow_search_attributes_workflow_type ON workflow_search_attributes (workflow_type);

-- Outbox table (for external messaging)
CREATE TABLE IF NOT EXISTS outbox (
    id SERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    topic VARCHAR(255) NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_outbox_published ON outbox (published);
CREATE INDEX IF NOT EXISTS idx_outbox_workflow_id ON outbox (workflow_id);
