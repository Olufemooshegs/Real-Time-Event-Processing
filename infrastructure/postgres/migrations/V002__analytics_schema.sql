-- Step 7 real sink schema. This migration replaces the Step 1 connectivity-only shell.
-- It is intentionally idempotent so a fresh initialization and a manual migration run have
-- the same result. Only the Step 1 connectivity shell is replaced. A real events table is
-- preserved so rerunning this migration cannot erase verified data.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'transactions' AND table_name = 'events'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'transactions' AND table_name = 'events' AND column_name = 'event_id'
    ) THEN
        DROP TABLE transactions.events;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS transactions.events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL,
    amount BIGINT NOT NULL,
    currency TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    schema_version INTEGER NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS events_user_event_time_idx
    ON transactions.events (user_id, event_time);

CREATE TABLE IF NOT EXISTS transactions.window_aggregates (
    user_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_volume BIGINT NOT NULL,
    average_transaction_value DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, window_start)
);

CREATE TABLE IF NOT EXISTS transactions.anomalies (
    anomaly_id BIGSERIAL PRIMARY KEY,
    anomaly_type TEXT NOT NULL CHECK (anomaly_type IN ('ANOMALY_VELOCITY', 'ANOMALY_AMOUNT')),
    event_id UUID NOT NULL,
    user_id TEXT NOT NULL,
    transaction_count BIGINT,
    velocity_window_seconds INTEGER,
    velocity_threshold BIGINT,
    amount BIGINT,
    rolling_99th_percentile DOUBLE PRECISION,
    history_size INTEGER,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS anomalies_user_detected_at_idx
    ON transactions.anomalies (user_id, detected_at);
