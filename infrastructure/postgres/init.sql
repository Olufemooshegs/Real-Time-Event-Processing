-- Step 1 connectivity shell only. The analytics schema is introduced in Step 7.
CREATE SCHEMA IF NOT EXISTS transactions;

CREATE TABLE IF NOT EXISTS transactions.events (
    id BIGSERIAL PRIMARY KEY
);
