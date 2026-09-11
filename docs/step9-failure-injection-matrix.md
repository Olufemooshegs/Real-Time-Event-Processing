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

## Results

### Scenario 1: Kafka broker failure — closed

Run against the live Codespaces stack on 2026-09-11, three attempts, evidence in
`step9-results/kafka-broker-failure-*`.

**Pre-existing bug found and fixed before this scenario could produce a meaningful result.**
Flink 1.19's `config.yaml` uses a hierarchical schema. The `docker-compose.yml`
`FLINK_PROPERTIES` block set both a bare `restart-strategy: fixed-delay` scalar and nested
`restart-strategy.fixed-delay.attempts` / `.delay` keys. Under the new schema these collide:
`restart-strategy` cannot simultaneously be a plain string and a map. The loader kept the
scalar and silently dropped the nested attempts/delay, with no warning in any log. The job
ran with Flink's hardcoded library default (`FixedDelayRestartBackoffTimeStrategy`,
1 attempt, 1000ms delay) instead of the intended 10 attempts / 5s delay, confirmed by the
exact parameters named in the `Recovery is suppressed by...` exception on the first kill
test. Every restart-strategy assumption made before this point, including the Section 5
design doc's ~50-second retry budget, was never actually in effect.

**Fix:** `restart-strategy: fixed-delay` changed to `restart-strategy.type: fixed-delay` in
both `jobmanager` and `taskmanager` `FLINK_PROPERTIES` blocks, so nothing collides with the
nested keys. Confirmed via `config.yaml` producing a proper nested block
(`restart-strategy: { type: fixed-delay, fixed-delay: { attempts: 10, delay: 5s } }`) and,
later, via the exact exception on a second forced failure naming
`maxNumberRestartAttempts=10, backoffTimeMS=5000`.

**Finding 1 — Kafka-outage recovery is data-dependent, not deterministic.** With only a
5% duplicate rate and no late/malformed traffic, killing the broker for several minutes
never produced a single task failure. The job stayed `RUNNING` throughout, its `KafkaSource`
consumer silently absorbed the outage via its own internal client reconnect logic, and the
Postgres event count resumed climbing automatically once the broker came back (129,162 ->
135,336 across the recovery window), with zero restarts and zero manual intervention.

**Finding 2 — with dead-letter/late-topic writes forced (`--late-rate 0.3
--malformed-rate 0.2`), the outage does eventually cause a hard failure, but only after a
~120-second delay**, not immediately. This lines up with Kafka producer's default
`delivery.timeout.ms` of 120,000ms: the dead-letter/late sink's producer connection,
already open before the kill, only surfaced a hard error once its internal delivery
timeout was exhausted (`No resolvable bootstrap urls given in bootstrap.servers`,
raised while attempting to (re)construct the producer). First `RESTARTING` state observed
122 seconds after the kill.

**Finding 3 — the corrected 10-attempt/5s restart strategy gives the job roughly 50
seconds of retry budget before giving up permanently.** Timestamps confirm this precisely:
`RESTARTING` first observed at 08:47:09 UTC, terminal `FAILED` at 08:48:00 UTC, a 51-second
gap matching 10 x 5s almost exactly. Kafka was down far longer than that in this test, so
the retry budget was always going to be exhausted; this defines the actual survivable
outage window for this pipeline as configured: **under a minute, self-healing; over a
minute, requires manual resubmission.**

**Finding 4 — a terminally `FAILED` job is not resupervised.** Flink does not restart a
job that has exhausted its configured restart attempts; it stays down indefinitely until
someone runs `flink run` again. There is no external supervisor (no Kubernetes restart
policy, no `restart: unless-stopped`-equivalent at the Flink job level) in this setup.

**Finding 5 — end-to-end exactly-once across this outage was delivered by the Postgres
idempotent upsert, not by Flink's checkpoint recovery.** The resubmission after the
terminal failure was a plain `flink run -py ...` with no `-s <savepoint path>`, so it
started with completely empty deduplication state, not a resumed checkpoint. Despite that,
`select count(*), count(distinct event_id) from transactions.events` returned
`162350 | 162350` after resubmission and full backlog drain: zero duplicate rows. This
confirms the design doc's Section 5 claim in practice, and sharpens it: Flink's
`EXACTLY_ONCE` checkpointing protects internal state across a task failure *within* a
running job's lifetime. It provides no protection across a full job termination and
manual cold resubmission. The absence of duplicates here is entirely attributable to the
Step 7 upsert-on-`event_id` sink, independent of Flink's own recovery mechanism.

RF=1 still means no Kafka-level fault tolerance was or could be demonstrated by this test;
the finding here is about Flink's and the pipeline's behavior around a broker outage, not
about Kafka surviving one.

### Scenarios 2-5

Not yet run. Procedures above remain accurate and unchanged.
