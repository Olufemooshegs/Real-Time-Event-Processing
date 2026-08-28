-- Migration bootstrap. PostgreSQL runs this on first initialization only.
CREATE SCHEMA IF NOT EXISTS transactions;
CREATE TABLE IF NOT EXISTS transactions.schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- V002 is kept as a separate file for review and reproducibility. The psql \ir directive
-- executes it from the mounted /docker-entrypoint-initdb.d directory.
\ir migrations/V002__analytics_schema.sql

INSERT INTO transactions.schema_migrations (version)
VALUES ('V002__analytics_schema.sql')
ON CONFLICT (version) DO NOTHING;
