# Step 11 benchmark harness

## Procedure

Run from the repository root in Codespaces/Linux after Kafka, Postgres, Flink, and the
producer prerequisites are healthy:

```bash
make up
make flink-up
make step11-benchmark
```

The script accepts a space-separated rate list, duration per level, and output directory:

```bash
bash scripts/step11-benchmark.sh "1000 5000" 120 step11-results/<run-id>
```

It captures hardware and environment data before the first level, confirms the Kafka image
provides `kafka-get-offsets`, discovers Flink lag/backlog metrics through the JobManager
REST API, and records five-second snapshots. Consumer-group CURRENT-OFFSET/LAG is used;
the FLIP-27 source's lack of an active member is not treated as a failure.

Each level writes raw offsets, producer logs, poll snapshots, checkpoint history, latency
inputs, ID reconciliation, and `level-<rate>.json`. Achieved throughput comes from raw
offset deltas. Drain requires unchanged raw offsets and Postgres event count across three
polls, or records a timeout. The recovery run uses the highest level that stabilized and
explicitly restores a SIGKILLed TaskManager.

## Results

Results are pending. This document intentionally contains no benchmark numbers or example
output. Review generated JSON and polling artifacts after running against the live stack;
do not substitute requested rates for measured rates.
