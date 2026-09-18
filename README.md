# Real-Time Event Processing and Analytics Platform

A distributed, fault-tolerant transaction processing pipeline built to
demonstrate defensible, verified understanding of real system design
concepts: exactly-once semantics, event-time watermarks, backpressure, fault
tolerance, concurrency, and latency under load. Every step in this build was
verified against real terminal output before being marked complete. Nothing
here is a demo that only ran once.

## Architecture

```
Producer (aiokafka) -> Kafka (KRaft, 6 partitions) -> Flink (PyFlink 1.19.3)
    -> Postgres (idempotent upserts) -> FastAPI (read layer)
```

- **Producer**: async Python, injects controlled duplicates, late events,
  out-of-order events, and structurally malformed events at configurable
  rates, for realistic fault and edge-case testing.
- **Kafka**: single-broker KRaft mode, 4 topics (`transactions.raw`,
  `transactions.deadletter`, `transactions.late`, `analytics.aggregates`),
  6 partitions each.
- **Flink**: PyFlink job handling event-time validation, deduplication,
  windowed aggregation, anomaly detection, and direct Postgres sinks.
- **Postgres**: system of record, with `event_id`-based idempotent upserts
  providing the pipeline's real exactly-once guarantee.
- **FastAPI**: read-only analytics layer over Postgres.

## Tech stack

Kafka (KRaft), Apache Flink / PyFlink 1.19.3, Postgres, FastAPI, Docker
Compose, Python (aiokafka, psycopg2). Development environment is GitHub
Codespaces; Docker cannot be run locally for this project. Code is generated
locally on Windows via Codex, pushed to GitHub, then pulled and verified
inside the Codespace.

## Quickstart

```
git pull
make down
docker volume rm $(docker volume ls -q | grep real-time-event-processing)
make up
make health
make topic-create
make flink-job-submit
docker compose exec jobmanager flink list
```

If `flink-job-submit` fails with a checkpoint directory permission error
immediately after a fresh volume wipe, this is a known gap (see below), fixed
with:

```
docker compose exec -u root jobmanager chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints
docker compose exec -u root taskmanager chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints
make flink-job-submit
```

## Build status: 12-step plan

| Step | Scope | Status |
|---|---|---|
| 1-3 | Base infrastructure setup (Kafka, initial producer, initial Flink job scaffolding) | Complete |
| 4 | Bug fixing and infrastructure hardening pass | Complete, see Known Issues below |
| 5-6 | Event-time processing, watermarking, and anomaly detection logic | Complete |
| 7 | Validation, deduplication, anomaly detection, and Postgres sinks in one job | Complete |
| 8 | (internal build step) | Complete |
| 9 | Fault tolerance: 4 failure-injection scenarios | Complete. See [docs/step-09-fault-tolerance.md](docs/step-09-fault-tolerance.md) |
| 10 | FastAPI analytics API, 5 endpoints | Complete. See [docs/step-10-analytics-api.md](docs/step-10-analytics-api.md) |
| 11 | Benchmark harness, 1k to 100k requested events/sec | Complete. See [docs/step-11-benchmark-harness.md](docs/step-11-benchmark-harness.md) |
| 12 | Full end-to-end integration test | Complete. See [docs/step-12-e2e-test.md](docs/step-12-e2e-test.md) |

Steps 1-3, 5, 6, and 8 do not have dedicated write-ups in this repo's history;
their outcomes are folded into the Known Issues section below where they
produced a confirmed, reusable finding.

## Key findings across the project

- **The pipeline's real exactly-once guarantee comes from Postgres's
  idempotent upsert on `event_id`, not from Flink checkpoint recovery alone.**
  This was proven repeatedly across Step 9's scenarios: even when a producer
  was killed mid-stream or a Flink job died and needed manual resubmission,
  reconciliation against Kafka offsets and Postgres counts showed zero data
  loss and zero double-processing every time.
- **Flink self-heals from Kafka-side failures (broker loss, network
  partitions) but not from a sustained downstream Postgres outage.** Past
  roughly a 50-second retry budget, a job failure becomes terminal and needs
  manual resubmission. This is the single most important architectural
  finding from Step 9: a production deployment needs external supervision to
  handle sustained database outages, since Flink's own fault tolerance
  doesn't cover that case.
- **The producer, not the downstream pipeline, is the binding constraint at
  lower load levels**, per Step 11's benchmark. A sharp capacity cliff appears
  at higher requested rates, past which the system does not degrade
  gracefully.
- **Step 12's full reconciliation closes the loop**: a single, clean,
  end-to-end run showed every stage (producer, Kafka, dead-lettering,
  deduplication, Postgres, API) accounting for events exactly, with no
  unexplained gaps.

## Known issues and workarounds

Confirmed, reusable findings from bug fixing and hardening (Step 4) and
general project work, kept here because they would otherwise need to be
rediscovered:

- **Flink 1.19 restart strategy config**: the hierarchical key
  `restart-strategy.type: fixed-delay` is required; the flat key
  `restart-strategy: fixed-delay` silently disables the strategy with no
  error.
- **`WindowedStream.side_output_late_data()` is non-functional** in PyFlink
  1.19.3, confirmed via JAR inspection and A/B testing. A manual
  `LatenessRouter` implementation is required instead.
- **`JdbcSink.sink()` is unusable** with current `flink-connector-jdbc`
  releases due to a removed Java reflection target. Direct `psycopg2` sink
  classes are used instead, which is why Postgres connection failures during
  an outage surface inside a `ProcessFunction` rather than through a
  framework-managed sink with its own retry logic (see Step 9, Scenario 4).
- **`restart: unless-stopped` in Docker Compose is non-functional** in
  Codespaces' nested container runtime; `RestartCount` stays at 0 after a
  `SIGKILL`. Explicit `docker compose up -d` is required instead.
- **Freshly created Docker volumes are root-owned**, but the Flink containers
  run as the `flink` user. Every full `docker volume rm` and rebuild requires
  reapplying `chown -R flink:flink` on the checkpoint and savepoint
  directories before job submission will succeed. This is not yet scripted
  into `make up` and should be, to avoid the manual step on every clean
  environment reset.
- **`curl` is not present inside the Flink images** despite being referenced
  in earlier healthcheck scripts. Since the JobManager's REST API port
  (`8081`) is exposed to the host, `curl` calls against Flink's REST API
  should be run from the host shell, not `docker compose exec`.
- **`*.txt` gitignore rules can silently exclude `requirements.txt`**, causing
  Docker build failures with no obvious cause. Always check gitignore scope
  when a build fails to find a dependency file that is visibly present in the
  working directory.
- **Kafka's retry budget is approximately 50 seconds** before a terminal
  failure; this figure reappears consistently in both Scenario 1 (Kafka
  broker failure) and Scenario 4 (Postgres outage) and reflects Flink's fixed
  restart strategy budget in this deployment, not a Kafka-specific limit.
- **Fixed drain waits produce false recovery results** in fault-tolerance
  testing. Always poll until counts stabilize rather than waiting a fixed
  number of seconds and assuming recovery is complete.

## Testing methodology

Every verification in this project followed the same discipline: no step was
marked complete without real terminal output, and no discrepancy was
hand-waved away. When a producer's own counters proved unreliable (a hard
kill, a truncated log from a concurrent process), the fix was always to
reconcile backward from Kafka's raw offset deltas and Postgres's landed
counts, never forward from self-reported application stats. This pattern
surfaced independently in three of Step 9's four scenarios and is the single
most reusable testing lesson from this project.