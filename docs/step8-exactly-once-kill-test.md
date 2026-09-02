# Step 8: exactly-once kill test

This is a repeatable experiment, not a pre-recorded success claim. It kills the
TaskManager with `SIGKILL` while a counted producer run is active, then reconciles the
raw Kafka records against the dead-letter topic and Postgres event IDs.

## Run

Run from the repository root in Codespaces/Linux after Steps 1-7 are running:

```bash
cp .env.example .env   # only if .env does not exist; set a real password
make up
make flink-up
make postgres-migrate  # only for an existing Step 1 Postgres volume, if available
EVENTS_TARGET=100000 RATE=1000 KILL_AFTER=20 bash scripts/step8-kill-test.sh
```

The default target is approximately 100,000 records at 1,000 records/sec. This is large
enough to span several 10-second checkpoints and make a mid-stream kill observable while
remaining reasonable for the 4-core/8-GB development host. The producer's final
`summary sent=...` line is the authoritative produced count; the target is only a pacing
configuration because the async generator's achieved rate is measured, not assumed.

The script writes an evidence directory under `step8-results/<run-id>/` containing:

- producer and Flink submission logs;
- Kafka partition offsets before and after the run;
- raw and dead-letter records from precisely that offset range;
- Postgres event IDs and before/after row counts;
- `reconciliation.txt`, including `unaccounted_ids`.

`unaccounted_ids` is the lost-event measure: each unique event ID observed in the raw
range that appears in neither `transactions.events` nor a dead-letter record. Raw offset
delta and producer `sent` are reported separately because producer retries create repeated
records with one event ID. Structural malformed records are expected in dead-letter; valid
semantic anomalies remain valid events at this stage and should land in Postgres.

After the script drains, inspect the JobManager UI and TaskManager logs. Confirm that the
job returned to `RUNNING`, the restarted TaskManager has no continuing failed vertex
attempts, and the checkpoint directory remained writable. A clean CLI submission message
alone is not evidence of recovery.

Compose configures a bounded fixed-delay restart strategy (10 attempts, 5 seconds apart)
so a TaskManager loss has an opportunity to recover without creating an unbounded
crash-loop. The test must still record whether those attempts were actually used.

## Downstream failure check

This is separate from a worker kill. Start a sustained producer run, then stop Postgres for
roughly 15 seconds while the Flink job is running:

```bash
docker compose stop postgres
sleep 15
docker compose start postgres
docker compose exec -T postgres sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Record the TaskManager log and JobManager vertex state during the outage. The direct
`psycopg2` sinks do not contain an application-level reconnect loop: a connection or
statement exception can fail the operator, after which Flink's configured restart/recovery
behavior is the thing being tested. Query Postgres after the producer drains and reconcile
IDs again. Do not
classify this as exactly-once merely because retries eventually succeed: the evidence must
show zero unaccounted IDs and zero duplicate primary-key rows, with any anomaly append
duplicates called out separately.

## Interpretation

Flink checkpointing provides exactly-once consistency for internal keyed state (dedup,
windows, and anomaly state) across TaskManager recovery. The Postgres event and aggregate
upserts make replayed writes idempotent, providing end-to-end duplicate protection for
those tables. The test must still report the measured result; it does not prove exactly-once
for diagnostic Kafka topics or append-only anomaly rows, where retry duplicates are an
accepted tradeoff.
