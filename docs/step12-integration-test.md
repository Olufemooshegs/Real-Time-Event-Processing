# Step 12: end-to-end integration test

## Procedure

Run from the repository root in Codespaces/Linux after the Compose prerequisites are
available:

```bash
make step12-integration-test
```

The runner first calls `scripts/step12-reset-kafka-backlog.sh`. That script stops Compose
without `-v`, identifies the volume bearing the Compose `kafka-data` label, removes only
that volume, brings the stack up, reapplies declarative topics, and requires all six
`transactions.raw` partitions to report offset zero. It deliberately leaves Postgres and
Flink checkpoint/savepoint volumes intact.

It then submits the Flink job and runs the producer with this fixed correctness profile:

```text
--rate 100 --duration 60 --user-pool-size 5 --merchant-pool-size 10
--duplicate-rate 0.10 --late-rate 0.10 --late-max-delay-seconds 30
--out-of-order-rate 0.10 --malformed-rate 0.10 --malformed-mode structural
```

The script waits for three consecutive five-second polls with unchanged raw offsets and
Postgres event count. It exports the precise raw and dead-letter offset ranges, reconciles
unique raw IDs against Postgres and dead-letter IDs using `scripts/reconcile_event_ids.py`,
and validates the live API against direct Postgres rows. API validation covers transactions,
aggregates, per-user anomalies, global anomalies, cursor page boundaries, malformed
cursors, invalid anomaly types, and one sampled transaction payload.

Before the benchmark isolation sweep, the script performs the Kafka-only reset a second
time. It invokes the existing Step 11 harness directly with `1000 10000 50000` and its
120-second duration. `SKIP_RECOVERY=true` prevents a new kill test because Step 12 analyzes
the existing Step 11 recovery artifact instead. `scripts/step12-analyze.py` writes the
fresh-vs-prior comparison and reports whether the checked-in Step 8 evidence includes a
measured recovery duration suitable for comparison.

Each run writes `step12-results/<UTC timestamp>/` with producer/Flink logs, offset ranges,
polls, reconciliation, API validation, the fresh benchmark artifacts, and JSON analyses.

## Results

Results are pending. No result is recorded here until this script has run against the live
stack and its generated JSON artifacts have been reviewed.
