# Flink validation and deduplication job

Step 4 runs one PyFlink JobManager and one TaskManager. The JobManager is limited to 512 MiB and the TaskManager to 1536 MiB, including Flink process memory. This is deliberately conservative on the 4-core, 8 GB Codespace so Kafka, Postgres, Docker, and the operating system retain headroom. The TaskManager has two task slots and 256 MiB managed memory.

The job checkpoints every 10 seconds to the `flink-checkpoints` named Docker volume. A named volume survives container restarts and normal `docker compose down`, unlike in-memory checkpoint state. Ten seconds is short enough to retain recent deduplication state for this development job without spending a disproportionate amount of time checkpointing its small state.

## Start and submit

```bash
make flink-up
make flink-job-submit
make flink-logs
```

The JobManager web UI is available at `http://localhost:8081`; `make flink-up` waits for its `/overview` endpoint. Use `make flink-health` to check it again later.

The job reads `transactions.raw` with consumer group `flink-validation-dedup-v1`, starting from the earliest offset on its first run. To test it from new data with the existing group, either start the producer after submission or use a new group through the direct `flink run -py` command and `--consumer-group`.

## What appears in the TaskManager log

- `VALIDATED_PASS event_id=...` and `VALIDATED_PASS_OUTPUT {...}`: structurally valid, first-seen records. Semantic malformed values intentionally also pass in Step 4.
- `DEADLETTERED reason=missing_field:amount`, `invalid_type:amount`, or `malformed_json`: structurally invalid records. A JSON envelope containing `reason_code` and `raw_record` is sent to `transactions.deadletter`.
- `DEDUPLICATED_DROP event_id=...`: a repeated `event_id` was dropped by keyed state.

The deduplication TTL is 10 minutes. Step 2 verified exact-payload producer retry duplicates, not delayed retries with changed timestamps, so a ten-minute window gives ample retry coverage without retaining event IDs indefinitely. Revisit this TTL if later failure testing demonstrates a longer retry or recovery window.

## Caveats

This job intentionally has no watermarks, windows, late-event routing, semantic validation, anomaly detection, Postgres sink, or aggregate Kafka sink. Those belong to later steps.

PyFlink process functions and Python serialization add overhead relative to a Java implementation. That tradeoff is appropriate for consistency with the Python stack and this minimal job, but it must be measured during the later throughput benchmark rather than treated as production-scale evidence.
