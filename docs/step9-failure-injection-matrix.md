# Step 9: failure-injection matrix

These procedures are rerunnable and do not change pipeline code. Run them from the
repository root in Codespaces/Linux with `.env` loaded and Kafka, Postgres, and Flink
healthy. Capture each scenario under `step9-results/<scenario>-<UTC timestamp>/`.

## Shared evidence rules

Before each run, record `docker compose ps`, recent JobManager/TaskManager/Postgres/Kafka
logs, the Flink job ID, Kafka consumer-group offsets, and baseline Postgres counts. After
the injected failure, poll until the relevant metric is stable, not for a fixed sleep:

```bash
while true; do
  date -u +%FT%TZ
  docker compose exec -T jobmanager flink list
  docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
    --describe --group flink-validation-dedup-v1 || true
  docker compose exec -T postgres sh -c \
    'psql -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select count(*) from transactions.events"'
  sleep 5
done
```

Stop polling only after the producer has exited, raw-topic end offsets stop changing, and
the Postgres event count is unchanged across at least three polls. Reconcile unique raw
event IDs against `transactions.events` and `transactions.deadletter` as Step 8 did; do
not infer loss from a fixed elapsed time.

For every scenario record failure time, detection time, recovery time, raw records
produced, events replayed, events landed, dead-lettered events, unaccounted IDs, duplicate
event IDs, duplicate window keys, and final Flink vertex state.

## 1. Kafka broker failure

Start a bounded producer and the detached Flink job. Once the job is `RUNNING` and raw
offsets are increasing, kill the sole broker:

```bash
docker compose kill -s SIGKILL kafka
date -u +%FT%TZ > "$OUT/kafka-killed-at"
```

Codespaces does not reliably restart a SIGKILLed container under `restart: unless-stopped`.
Record the observed `Exited (137)` state, then explicitly restore it:

```bash
docker compose up -d kafka
```

Poll Kafka health, Flink state, source offsets, and Postgres counts to stability. This
single-broker RF=1 setup cannot demonstrate Kafka fault tolerance because no replica can
serve reads or writes while the broker is down. Report whether Flink retries and recovers
after the broker returns or fails permanently; neither outcome is Kafka HA.

## 2. Producer failure and restart

```bash
python3 producers/transaction_generator/main.py --bootstrap-servers localhost:9092 \
  --topic transactions.raw --rate 500 --duration 180 --duplicate-rate 0.05 \
  > "$OUT/producer-1.log" 2>&1 &
producer_pid=$!
sleep 20
kill -9 "$producer_pid"
date -u +%FT%TZ > "$OUT/producer-killed-at"
python3 producers/transaction_generator/main.py --bootstrap-servers localhost:9092 \
  --topic transactions.raw --rate 500 --duration 90 --duplicate-rate 0.05 \
  > "$OUT/producer-2.log" 2>&1
```

Compare both producer summaries with raw offset deltas and reconciled Postgres IDs. The
expected distinction is a producer-side gap while it is dead, not consumer loss. Records
published before the kill should drain, and new records should resume after restart.

## 3. Network interruption between Flink and Kafka

Record the Compose network name with `docker network ls`, then disconnect the TaskManager
while a producer is active:

```bash
NETWORK=real-time-event-processing_default
docker network disconnect "$NETWORK" real-time-flink-taskmanager
date -u +%FT%TZ > "$OUT/network-disconnected-at"
sleep 15
docker network connect "$NETWORK" real-time-flink-taskmanager
date -u +%FT%TZ > "$OUT/network-reconnected-at"
```

This Docker method also interrupts TaskManager-to-JobManager RPC; record that limitation.
If available, prefer a temporary firewall OUTPUT block to Kafka's container IP and record
the exact rule. Poll consumer lag, job state, and Postgres counts to stability, then
reconcile IDs and report whether resubmission was required.

## 4. Postgres unavailability during sink writes

With sustained input and a `RUNNING` job, stop only Postgres:

```bash
docker compose stop postgres
date -u +%FT%TZ > "$OUT/postgres-stopped-at"
sleep 15
docker compose start postgres
docker compose exec -T postgres sh -c \
  'until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do sleep 2; done'
date -u +%FT%TZ > "$OUT/postgres-ready-at"
```

The Step 7 sinks are direct `psycopg2` `ProcessFunction`s with no application-level
reconnect loop. A connection exception may fail the operator; record whether Flink's
restart strategy recovers it and whether checkpoint replay drains the backlog. Poll until
raw offsets and Postgres counts stabilize, then perform the Step 8 ID reconciliation.
Report event-table duplicates, window-key duplicates, and append-only anomaly duplicates
separately.

## 5. Consumer restart and offset-reset behavior

Record current group offsets and the job ID, then cancel cleanly:

```bash
docker compose exec -T jobmanager flink list
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --describe --group flink-validation-dedup-v1
docker compose exec -T jobmanager flink cancel <JOB_ID>
```

Submit the same job again with the same `flink-validation-dedup-v1` group and compare
starting offsets with the committed offsets. Then submit a second copy with a new group,
for example `step9-earliest-<timestamp>`, and verify that
`KafkaOffsetsInitializer.earliest()` reprocesses retained raw data. The same-group run
should resume; the new-group run is an intentional replay. Reconcile both runs and report
event-upsert uniqueness, window updates, and diagnostic append duplicates separately.

## Results status

This document defines the procedures and evidence fields. It does not claim recovery,
loss, or duplicate counts until each scenario has been run against the live Codespaces
stack and its Kafka/Postgres evidence reviewed.
