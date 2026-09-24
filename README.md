# Real-Time Event Processing and Analytics Platform

A distributed, fault-tolerant transaction processing pipeline built to demonstrate real-world event streaming and system design concepts: event-time processing, watermarks, deduplication, exactly-once effects, backpressure, fault tolerance, concurrency, and performance under load.

The project was developed as an evidence-driven engineering build: each major capability was validated with controlled failure injection, reconciliation, and benchmark runs rather than a single successful demo.

## Architecture

```text
Async Producer (aiokafka)
        |
        v
Kafka (KRaft, 6 partitions)
        |
        v
Apache Flink / PyFlink 1.19.3
        |
        +--> Dead-letter events
        +--> Late events
        +--> Windowed analytics
        +--> Anomaly detection
        |
        v
PostgreSQL (idempotent upserts)
        |
        v
FastAPI (read-only analytics API)
```

### Components

| Component      | Responsibility                                                                                                                           |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **Producer**   | Generates transaction events asynchronously and injects controlled duplicates, late events, out-of-order events, and malformed payloads. |
| **Kafka**      | Durable event transport using KRaft with 6 partitions per topic.                                                                         |
| **Flink**      | Performs validation, event-time processing, deduplication, watermarking, windowed aggregation, anomaly detection, and routing.           |
| **PostgreSQL** | Acts as the system of record and uses `event_id`-based idempotent upserts to make final writes safe against reprocessing.                |
| **FastAPI**    | Exposes read-only analytics endpoints backed by PostgreSQL.                                                                              |

### Kafka Topics

* `transactions.raw`
* `transactions.deadletter`
* `transactions.late`
* `analytics.aggregates`

## Tech Stack

* **Apache Kafka** with KRaft
* **Apache Flink / PyFlink 1.19.3**
* **PostgreSQL**
* **FastAPI**
* **Python**
* **aiokafka**
* **psycopg2**
* **Docker Compose**

## Core Engineering Concepts

### Event-Time Processing

Events are processed using their event timestamps rather than relying only on ingestion time. Watermarks are used to reason about out-of-order and late-arriving events.

### Deduplication and Exactly-Once Effects

The pipeline is designed to tolerate duplicate delivery and reprocessing. Flink checkpoint recovery alone does not guarantee exactly-once effects at the database boundary.

The final protection comes from PostgreSQL's idempotent `event_id` upserts, ensuring repeated processing does not create duplicate records in the system of record.

### Fault Tolerance

The system includes controlled failure injection for both infrastructure and application components, including Kafka failures, network disruption, producer termination, Flink failure, and PostgreSQL outages.

### Backpressure and Load Handling

The benchmark harness tests the pipeline under increasing requested event rates and observes where throughput, latency, and recovery behavior begin to degrade.

### Reconciliation-Based Testing

When application-level counters become unreliable during failure scenarios, correctness is established from independent sources such as Kafka offset deltas and PostgreSQL landed counts.

This prevents recovery claims from depending on the component that may have failed.

## Quickstart

```bash
git pull
make down

docker volume rm $(docker volume ls -q | grep real-time-event-processing)

make up
make health
make topic-create
make flink-job-submit

docker compose exec jobmanager flink list
```

After submission, verify the Flink job is running and use the project test and benchmark commands to exercise the pipeline.

## Build Status

| Step | Scope                                                              | Status   |
| ---- | ------------------------------------------------------------------ | -------- |
| 1-3  | Base infrastructure, Kafka, producer, and Flink scaffolding        | Complete |
| 4    | Bug fixing and infrastructure hardening                            | Complete |
| 5-6  | Event-time processing, watermarking, and anomaly detection         | Complete |
| 7    | Validation, deduplication, anomaly detection, and PostgreSQL sinks | Complete |
| 8    | Internal build step                                                | Complete |
| 9    | Fault tolerance and failure-injection testing                      | Complete |
| 10   | FastAPI analytics API                                              | Complete |
| 11   | Benchmark harness: 1k-100k requested events/sec                    | Complete |
| 12   | Full end-to-end integration test                                   | Complete |

### Detailed Verification Reports

* [Step 9: Fault Tolerance](docs/step-09-fault-tolerance.md)
* [Step 10: Analytics API](docs/step-10-analytics-api.md)
* [Step 11: Benchmark Harness](docs/step-11-benchmark-harness.md)
* [Step 12: End-to-End Test](docs/step-12-e2e-test.md)

## Verified Findings

### 1. Database Idempotency Is the Final Exactly-Once Boundary

Repeated failure scenarios showed that PostgreSQL `event_id` upserts prevent duplicate records even when processing is retried or a failed Flink job is restarted manually.

Reconciliation across Kafka and PostgreSQL showed no unexplained data loss or duplicate database writes in the tested scenarios.

### 2. Flink Recovers Differently Depending on the Failed Dependency

Kafka-side failures, including broker loss and network disruption, were recoverable through the configured Flink restart strategy.

A sustained PostgreSQL outage behaved differently: after the configured retry budget was exhausted, the Flink job entered a terminal failure state and required resubmission.

This exposes an important production-design boundary: **stream processor fault tolerance does not remove the need for external supervision of persistent downstream failures.**

### 3. Producer Throughput Becomes a Constraint at Lower Load Levels

Benchmarking showed that the producer is the limiting component at lower requested rates.

At higher requested rates, the system reaches a sharp capacity cliff rather than degrading smoothly.

This makes the benchmark useful not only for reporting throughput, but for identifying where the architecture needs scaling or redesign.

### 4. End-to-End Reconciliation Closes the Correctness Loop

The final integration test reconciled the major stages of the pipeline:

```text
Producer
   -> Kafka
   -> Validation / Routing
   -> Deduplication
   -> Aggregation
   -> PostgreSQL
   -> API
```

The run accounted for the expected events across the pipeline with no unexplained gaps.

## Known Implementation Constraints

These are important design findings discovered during development:

* **Flink restart strategy configuration** must use the hierarchical `restart-strategy.type: fixed-delay` configuration.
* **PyFlink 1.19.3 late-event side output behavior** required a custom routing implementation rather than relying on `WindowedStream.side_output_late_data()`.
* **The JDBC sink path used by the project was incompatible with the connector version in use**, so PostgreSQL writes are handled through direct `psycopg2` sink logic.
* **The configured failure-recovery budget is approximately 50 seconds** in the current deployment. This is a property of the configured Flink restart strategy, not a Kafka limit.
* **Recovery tests use polling and count stabilization**, rather than fixed sleep durations, to avoid false positives.

## Testing Methodology

Every major milestone was validated from observable system behavior rather than assumed from application logs.

For fault-tolerance tests, the project deliberately introduces failures and then checks recovery using independent evidence:

1. Inject a controlled failure.
2. Allow the system to recover or reach its terminal state.
3. Reconcile Kafka offsets and downstream PostgreSQL counts.
4. Check for lost, duplicated, or unexplained events.
5. Repeat across different failure modes.

This approach makes the project useful as a systems-engineering exercise rather than a simple Kafka/Flink demo.

## Project Focus

The project is intentionally centered on the hard parts of real-time systems:

* Event-time correctness
* Late and out-of-order data
* Idempotent processing
* Fault recovery
* Backpressure
* Failure injection
* End-to-end reconciliation
* Throughput and latency under load
* Clear failure boundaries between distributed components
