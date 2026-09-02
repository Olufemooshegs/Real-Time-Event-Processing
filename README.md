# Real-Time Event Processing & Analytics Platform

Status: **in progress — Step 8 experiment procedure ready; runtime results pending independent verification (see docs/architecture-design-doc.md for the full plan)**

This README is updated after each step with what's actually running and verified, not what's
planned. If something isn't listed under "What's running" below, it doesn't exist yet.

---

## What's running right now

- **Kafka**, single broker, KRaft mode (no Zookeeper), topic `transactions.raw` with 6
  partitions, replication factor 1.
- **Postgres**, with the Step 7 versioned schema for validated events, window aggregates,
  and anomaly records.
- **Flink**, with validation, deduplication, event-time windows, deterministic anomalies,
  and direct Postgres sinks.
- **Async transaction producer** (`producers/transaction_generator/main.py`, `aiokafka`),
  running as a plain local Python process, not containerized (deliberate choice for this
  phase — see "Decisions" below).

## What's explicitly NOT built yet

- FastAPI analytics API
- ClickHouse
- Prometheus / Grafana
- Failure injection tooling
- Benchmarking harness

---

## Why KRaft instead of Zookeeper

Simpler single-broker dev setup. No operational reason to introduce Zookeeper at this scale.

## Known gaps (carried forward deliberately, not oversights)

- **Replication factor is 1.** Fine for a single-broker dev environment; a real deployment
  would need ≥3 brokers and RF ≥3. Not addressed yet, and not pretended otherwise.
- **`.env` is not auto-validated.** `make up` will silently start Postgres with blank
  credentials if `.env` hasn't been copied from `.env.example` first, which caused a real
  restart-loop failure during Step 1 verification. Not yet fixed with a precondition check —
  flagged for Codex, not yet actioned.
- **Producer runs as a local process, not a container**, for now. Faster to iterate on while
  actively tuning injection rates (duplicate/late/out-of-order/malformed) against Flink's
  eventual watermark design in Step 5. Containerizing it is deferred, possibly to the
  failure-injection step (Step 9), where "kill the producer's container" becomes a
  meaningful test case.
- **Hot-partition skew is real, not just theorized.** See the dedicated write-up in
  `docs/architecture-design-doc.md`, Section 2. Measured at Step 2: a ~600-1000 distinct
  `user_id` pool of sequential zero-padded IDs left 2 of 6 partitions untouched at 2,000
  events even under nominal uniform distribution. Root cause identified as murmur2 hash
  behavior on a narrow structured key space, not a code bug — producer send-side logic was
  checked directly and ruled out. No mitigation decided yet (larger pool size vs.
  non-sequential IDs vs. accepting the skew as realistic).

---

## Verified so far (real terminal output, not assumptions)

### Step 1 — Infrastructure skeleton
- Kafka broker accepts connections; `transactions.raw` topic created and described correctly
  at 6 partitions / RF 1.
- Postgres accepts connections; `transactions.events` table confirmed present after tracing
  down a stale-volume issue that was silently skipping `init.sql` on restart (see git history
  for the debugging trail — root cause was an already-initialized data volume surviving a
  `down` without a matching, correctly-named `volume rm`).
- Clean restart (`down` + `up`, no manual fixes) confirmed working after the above was
  resolved.

### Step 2 — Producer (closed)
- Duplicate injection confirmed real: repeated `event_id` values observed in the raw topic
  with identical payloads, consistent with a producer-level retry rather than a fresh event.
- Late-event injection confirmed real: observed `event_time` trailing `ingest_time` by up to
  ~28 seconds in a single sample run.
- Structural malformed events confirmed: string-typed `amount`, missing required fields
  (`amount`, `merchant_id`, `currency`, `event_time` each seen missing at least once).
- Semantic malformed events confirmed: negative amounts (7 examples, range -10,424 to
  -287,997), invalid currency codes (`INVALID`, `ZZZ`), invalid transaction types
  (`chargeback_test`, `unknown_type`).
- Partition-key correctness confirmed: same `user_id` consistently mapped to a single
  partition across a full fresh sample, checked directly via a distinct
  partition-per-key pipeline (not just distribution evenness).
- Within-partition out-of-order events confirmed: genuine `event_time` inversions between
  adjacent messages on the same partition, observed at ~20ms scale with
  `--out-of-order-rate 0.15`. Notably smaller scale than late-event delays (~28s) — see
  Section 3 of the design doc for why these two numbers need to be tuned separately in
  Step 5, not derived from one setting.
- Hot-partition skew investigated and found to be real even under nominal uniform user
  distribution, traced to murmur2 hash behavior on a narrow, sequential, zero-padded
  `user_id` key space — not a producer code bug. Full write-up and mitigation options
  in the design doc, Section 2. No mitigation decided yet.

---

### Step 3 — Kafka topic hardening & formal configuration (closed)
- Topic definitions moved from ad-hoc CLI commands to declarative config
  (`infrastructure/kafka/topics.yml`), covering all four topics from the design doc:
  `transactions.raw`, `transactions.deadletter`, `transactions.late`,
  `analytics.aggregates`.
- `scripts/apply-topics.sh` (via `make topics-apply`) creates/reconciles all four topics
  idempotently. Confirmed via real re-run: second execution reported `OK:` for every
  topic (not `Created topic`), no errors, no unintended changes.
- A partition-count parsing bug was found and fixed during verification: the script's
  `sed` pattern expected `PartitionCount:6` (no space) but Kafka's actual `--describe`
  output is `PartitionCount: 6` (with a space), causing every run to fail immediately
  after processing the first topic. Fixed by tolerating optional whitespace in the pattern.
- All four topics confirmed via `kafka-configs --describe` to match the manifest exactly:
  - `transactions.raw`: 6 partitions, RF 1, retention 48h (172800000ms), snappy
  - `transactions.deadletter`: 6 partitions, RF 1, retention 14d (1209600000ms), snappy
  - `transactions.late`: 6 partitions, RF 1, retention 14d (1209600000ms), snappy
  - `analytics.aggregates`: 6 partitions, RF 1, retention 7d (604800000ms), snappy
- Hot-key mitigation decided: switch to non-sequential `user_id` generation (format —
  full UUID vs. partial-readability suffix — still to be finalized), as a dedicated
  follow-up step. Not applied to the Step 2 producer in this step, to avoid silently
  modifying already-verified code.

### Step 4 — Minimal Flink job: validation, dedup (closed)
- Flink cluster (JobManager + TaskManager, single TaskManager, 2 slots) added to
  docker-compose.yml, memory sized deliberately (jobmanager 1024m process size / 1200m
  container limit, taskmanager 1536m / 1536m) after the default 512m proved mathematically
  insufficient for Flink's own JVM overhead + off-heap accounting.
- PyFlink job (`flink/jobs/validation_dedup_job.py`) consumes `transactions.raw`, performs
  structural validation only (JSON shape and field types — semantic rules like negative
  amounts deliberately pass through, per design, and are deferred to Step 6), routes
  structural failures to `transactions.deadletter` with a reason code, and deduplicates
  valid events by `event_id` using keyed state with a 10-minute TTL.
- Confirmed via the durable dead-letter topic (not just print logs): 134 dead-lettered
  records across a full test run, spanning 5 correct reason codes (`invalid_type:amount`,
  `missing_field:amount`, `missing_field:currency`, `missing_field:event_time`,
  `missing_field:merchant_id`).
- Dedup confirmed via log counts tracking sensibly against producer-reported duplicates
  (42 DEDUPLICATED_DROP vs 49 duplicates injected; gap explained by duplicate events that
  were also structurally malformed and dead-lettered before reaching the dedup stage).
- Checkpointing enabled (10s interval, EXACTLY_ONCE mode, durable file-based storage);
  job sustained continuous RUNNING state across multiple verification runs.

**Nine distinct, unrelated bug classes were found and fixed to get here** — worth recording
plainly, since this step took far longer than Steps 1-3 combined and each issue would have
been costly to rediscover blind in a later step:
1. `taskmanager` service missing its own `build:` block in docker-compose.yml (was trying
   to pull a nonexistent public image instead of building locally).
2. Kafka's healthcheck tested `localhost:29092` instead of `kafka:29092` — passed manually
   the whole time, never passed automated health checks, because the internal listener is
   bound to the `kafka` hostname, not loopback.
3. Kafka's healthcheck timeout (5s) too tight for a cold JVM CLI invocation
   (`kafka-topics`) — not a resource problem, just inherent JVM startup cost.
4. Two near-identical healthcheck blocks in the compose file (Kafka's and JobManager's)
   caused repeated edits to land on the wrong block during debugging.
5. JobManager's `jobmanager.memory.process.size: 512m` was mathematically too small —
   Flink's own JVM overhead minimum (192MB) plus default off-heap requirement (128MB)
   couldn't fit in 512MB total.
6. `curl` was listed in the Dockerfile's install step but wasn't actually present in the
   built image at runtime — broke both Flink healthchecks until switched to
   dependency-free checks (`bash`'s `/dev/tcp`, `pgrep`).
7. PyFlink job submission requires the Python interpreter path explicitly
   (`-pyclientexec`, `-pyexec`) since the image only has `python3`, not a plain `python`
   binary on PATH.
8. `flink-connector-kafka` (thin JAR) was used instead of `flink-sql-connector-kafka`
   (shaded/fat JAR) — the thin connector doesn't bundle Kafka's client library, causing a
   `NoClassDefFoundError` at Kafka source construction.
9. The `flink-checkpoints`/`flink-savepoints` named Docker volumes were owned by root by
   default while the container runs as the `flink` user — job startup failed until
   ownership was corrected.
10. PyFlink API mismatches specific to this installed version (1.19.3): `WatermarkStrategy`
    lives at `pyflink.common`, not `pyflink.datastream.watermark_strategy`;
    `set_checkpoint_storage()` requires a `FileSystemCheckpointStorage` object, not a raw
    path string; `print(..., flush=True)` isn't supported by PyFlink's `CustomPrint`; and
    side outputs are emitted by yielding an `(OutputTag, value)` tuple, not calling
    `ctx.output(tag, value)` as the Java API allows.

Any file edit under `flink/jobs/` requires `docker compose build jobmanager taskmanager`
before it takes effect — the job file is baked into the image at build time, not mounted
live. This was rediscovered the hard way more than once during this step.

### Step 5 — Event-time processing: watermarks, allowed lateness, windowing (closed)
- Bounded-out-of-orderness watermark (50ms bound, justified against Step 2's ~20ms measured
  jitter) with 30s idleness detection to prevent quiet/skewed Kafka partitions from stalling
  watermark progress.
- Allowed lateness set to 45s, justified against Step 2's ~28s measured maximum late-event
  delay, deliberately kept far larger than the watermark's own out-of-orderness bound per
  the design doc's Section 3 finding that these are separate phenomena at different scales.
- 10-second tumbling windows keyed by `user_id`, computing count/total volume/average
  transaction value. Verified mathematically correct via direct spot-checks against log
  output (e.g. `usr_00234: count=3, total=112148, average=37382.67` — exact).
- Watermark advancement independently confirmed live and correct via Flink's REST metrics
  API on both parallel subtasks (ruling out a hot-key/partition-stall watermark issue).

**Finding: `WindowedStream.side_output_late_data()` does not work in this PyFlink 1.19.3
environment.** Despite correct configuration (confirmed via source inspection of the
PyFlink library itself — `allowed_lateness()`, `side_output_late_data()`, and
`_get_result_data_stream()` all correctly wire the late-data tag into the underlying Java
operator), zero late events ever reached the side output across multiple clean test runs
with up to 168 genuinely late events per run (some delayed up to 90s, comfortably past the
45s threshold). Ruled out as causes, in order of investigation: watermark not advancing
(disproven — confirmed live via REST metrics), timestamp assignment producing wrong values
(disproven — verified via direct calculation), hot-key partition stall on the windowing
operator (disproven — both subtasks showed identical, current watermarks),
`aggregate()`-specific issue (disproven — swapping to `reduce()` produced the same zero
result). Root cause presumed to be a PyFlink Python-binding-specific limitation of this
built-in feature, not a configuration or design error.

**Resolution:** built-in late-data side output replaced with a manual `LatenessRouter`
(`ProcessFunction` comparing `ctx.timestamp()` against `ctx.timer_service().current_watermark()`
directly, run before windowing). Confirmed working via both log output (725
`LATE_EVENT_ROUTED` prints in one test) and durable topic content (741 real records
confirmed in `transactions.late` via direct consumption, not just logs).

Also fixed during this step: `TimestampAssigner` import path
(`pyflink.common.watermark_strategy`, not `pyflink.common`), `allowed_lateness()` requires
a plain int in milliseconds (not a `Time` object), and a class-defined-after-use ordering
bug (Python executes top-to-bottom; a class referenced inside `main()` must be defined
before the `if __name__ == "__main__":` block that calls `main()`, not after it).

### Step 6 — Stateful anomaly detection (closed)
Two deterministic rules, additive off the Step 5 event-time stream, no changes to
validation/dedup/windowing:
- **Velocity:** >10 transactions/user within a 60-second event-time horizon (keyed state
  counter).
- **Amount:** current transaction exceeds the user's own rolling 99th-percentile amount,
  computed over a bounded reservoir of the user's last 256 amounts, with a 20-record
  warmup period to avoid false positives on new users with too little history.

Verified with real traffic, not just log presence:
- Velocity rule confirmed correctly escalating under a real burst (`--user-pool-size 5`,
  50/sec): e.g. `usr_00005` fired repeatedly as its count climbed past threshold
  (11, 12, 13...17), each with correct `transaction_count`/`threshold` values.
- Amount rule confirmed with real percentile math, e.g. `usr_00004`: amount 680,762 vs.
  rolling 99th percentile 302,955 (genuine large outlier); `usr_00005`: two separate
  correct triggers (227,375 vs. 220,529; 527,965 vs. 224,737), with `history_size: 256`
  confirming the reservoir cap holds as designed.
- Confirmed zero false positives under normal, low-volume, full-pool traffic (5/sec,
  default user pool) — no anomalies fired.

### Step 7 — Postgres sinks: events, aggregates, anomalies (closed)
Versioned migration (`infrastructure/postgres/migrations/V002__analytics_schema.sql`)
replacing the Step 1 placeholder, adding `transactions.events`,
`transactions.window_aggregates`, `transactions.anomalies`.

**Finding: PyFlink 1.19.3's `JdbcSink.sink()` is unusable with any current
`flink-connector-jdbc` release.** The Python wrapper does Java reflection to find a static
method `createRowJdbcStatementBuilder(int[])` on `JdbcOutputFormat`. Inspecting the actual
JAR bytecode (`javap` unavailable in this image; inspected via `zipfile` + a raw string
scan instead) confirmed that method doesn't exist in `flink-connector-jdbc-3.2.0-1.19.jar`
— the connector was refactored to a `StatementExecutorFactory` pattern. Further research
showed this refactor predates Flink 1.19 itself (present as of 1.17-SNAPSHOT), meaning no
current `flink-connector-jdbc` release for 1.19 restores the old method PyFlink 1.19.3
expects. Not a version-pinning problem — a genuine incompatibility between PyFlink's
Python `JdbcSink` helper and every available connector release.

**Resolution:** bypassed `JdbcSink.sink()` entirely. Three sinks
(`PostgresEventsSink`, `PostgresAggregatesSink`, `PostgresAnomaliesSink`) implemented as
plain `ProcessFunction`s using `psycopg2` directly, each opening its own connection in
`open()`/closing in `close()`. Same reasoning as Step 5's `LatenessRouter`: work around a
confirmed-broken built-in rather than keep chasing connector version compatibility.

Verified against real duplicate/burst traffic, not just absence of errors:
- Idempotent upsert on `event_id` confirmed: zero duplicate rows in `transactions.events`
  after a run with 15% injected duplicate rate (314 real rows landed, zero duplicates).
- Idempotent upsert on `(user_id, window_start)` confirmed: zero duplicate window rows.
- Anomaly sink confirmed with real burst traffic: 628 `ANOMALY_VELOCITY` + 20
  `ANOMALY_AMOUNT` rows landed correctly.

```
cp .env.example .env
# edit .env: set a real (non-empty, non-placeholder) POSTGRES_PASSWORD
make up
make health
make topic-create
make topic-describe
```

Expected passing health check output:
```
PASS: Kafka broker is accepting connections
PASS: Kafka topic creation and description succeeded
PASS: Postgres is accepting connections
```

Producer (local process, from `producers/transaction_generator/`):
```
pip install -r requirements.txt
python main.py --rate 50 --duration 30 --duplicate-rate 0.05 --late-rate 0.1 \
  --out-of-order-rate 0.1 --malformed-rate 0.05 --malformed-mode structural
```
See `producers/transaction_generator/README.md` for the full flag reference.

---

## Step 8 exactly-once kill test

The repeatable experiment is documented in [`docs/step8-exactly-once-kill-test.md`](docs/step8-exactly-once-kill-test.md)
and automated by `scripts/step8-kill-test.sh`. It defaults to approximately 100,000
records, kills the TaskManager with `SIGKILL` mid-stream, waits for recovery, and records
Kafka offsets, producer counts, Postgres IDs, and unaccounted event IDs. No result is
considered verified until the generated evidence is checked against the JobManager state
and direct Kafka/Postgres queries.

---

## Development workflow notes

- Commit and push after local generation, before switching into the Codespace.
- Run `git pull` as the first action every time a Codespace session starts.
- Don't mark a step "done" on Codex's word alone — every step requires independent
  verification against real terminal output before moving to the next one.
