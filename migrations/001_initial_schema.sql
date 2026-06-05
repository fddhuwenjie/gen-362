CREATE TABLE IF NOT EXISTS workflow_executions (
    id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    workflow_type VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'RUNNING',
    last_event_id BIGINT NOT NULL DEFAULT 0,
    input JSONB NOT NULL DEFAULT '{}',
    result JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE(workflow_id, run_id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions(status);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_updated ON workflow_executions(updated_at);

CREATE TABLE IF NOT EXISTS workflow_events (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL,
    workflow_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attributes JSONB NOT NULL DEFAULT '{}',
    UNIQUE(workflow_id, run_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow ON workflow_events(workflow_id, run_id);
CREATE INDEX IF NOT EXISTS idx_workflow_events_type ON workflow_events(event_type);

CREATE TABLE IF NOT EXISTS pending_activities (
    id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    activity_id VARCHAR(64) NOT NULL,
    activity_type VARCHAR(255) NOT NULL,
    input JSONB NOT NULL DEFAULT '{}',
    attempt INT NOT NULL DEFAULT 1,
    scheduled_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    retry_policy JSONB,
    last_attempt_at TIMESTAMPTZ,
    task_queue VARCHAR(255) NOT NULL DEFAULT 'default',
    UNIQUE(workflow_id, run_id, activity_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_activities_queue ON pending_activities(task_queue);
CREATE INDEX IF NOT EXISTS idx_pending_activities_scheduled ON pending_activities(scheduled_at);

CREATE TABLE IF NOT EXISTS pending_timers (
    id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    timer_id VARCHAR(64) NOT NULL,
    fire_time TIMESTAMPTZ NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    fired BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(workflow_id, run_id, timer_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_timers_fire ON pending_timers(fire_time, fired);

CREATE TABLE IF NOT EXISTS activity_heartbeats (
    id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    activity_id VARCHAR(64) NOT NULL,
    progress JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_activity_heartbeats_activity ON activity_heartbeats(workflow_id, run_id, activity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS signal_queue (
    id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    signal_name VARCHAR(255) NOT NULL,
    input JSONB NOT NULL DEFAULT '{}',
    version VARCHAR(64),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    handled BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_signal_queue_workflow ON signal_queue(workflow_id, run_id, handled);

CREATE TABLE IF NOT EXISTS query_handlers (
    id BIGSERIAL PRIMARY KEY,
    workflow_type VARCHAR(255) NOT NULL,
    query_name VARCHAR(255) NOT NULL,
    handler_version VARCHAR(64) NOT NULL,
    handler_code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workflow_type, query_name, handler_version)
);
