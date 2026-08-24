# Flink validation, deduplication, and event-time windows

Step 4 runs one PyFlink JobManager and one TaskManager. The JobManager is limited to 512 MiB and the TaskManager to 1536 MiB, including Flink process memory. This is deliberately conservative on the 4-core, 8 GB Codespace so Kafka, Postgres, Docker, and the operating system retain headroom. The TaskManager has two task slots and 256 MiB managed memory.

The job checkpoints every 10 seconds to the `flink-checkpoints` named Docker volume. A named volume survives container restarts and normal `docker compose down`, unlike in-memory checkpoint state. Ten seconds is short enough to retain recent deduplication state for this development job without spending a disproportionate amount of time checkpointing its small state.

## Event-time behavior

The job assigns timestamps from the event's ISO-8601 `event_time`, then uses a bounded-out-of-orderness watermark of **50 ms**. Step 2 measured within-partition event-time inversions of roughly 20 ms, so 50 ms provides 2.5x measured headroom without turning ordinary ordering jitter into seconds of window-output delay. A **30-second source-idleness timeout** prevents quiet Kafka partitions from blocking watermark progress, which matters with the already measured partition skew.

Per-user event-time windows tumble every **10 seconds**. They allow **45 seconds** of lateness. The observed Step 2 late-event delay was about 28 seconds, so 45 seconds preserves a 17-second margin. This is intentionally much larger than the 50 ms watermark bound: the first covers small within-partition ordering jitter, while the latter retains already-closed windows for genuinely late arrivals.

Records arriving after `watermark > window_end + 45 seconds` are captured by the window operator's `side_output_late_data` mechanism and published unchanged to `transactions.late`. They are also logged with `LATE_EVENT_OUTPUT`. Late arrivals within the 45-second allowance update their original window and can produce a revised aggregate output.

## Start and submit

```bash
make flink-up
make flink-api-check
make flink-job-submit
make flink-logs
```

The JobManager web UI is available at `http://localhost:8081`; `make flink-up` waits for its `/overview` endpoint. Use `make flink-health` to check it again later.

Run `make flink-api-check` after rebuilding and before submitting when validating the installed PyFlink 1.19.3 image. It prints the in-container help for every Step 5 API call that is sensitive to PyFlink version: the watermark builder and timestamp assigner, tumbling window constructor, allowed-lateness setup, late-data side output, and incremental aggregate signature.

If `docker compose down -v` recreated the checkpoint or savepoint volumes, restore their ownership before submitting: run `chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints` as root in both the JobManager and TaskManager containers. Otherwise checkpointing can fail on a permission error.

The job reads `transactions.raw` with consumer group `flink-validation-dedup-v1`, starting from the earliest offset on its first run. To test it from new data with the existing group, either start the producer after submission or use a new group through the direct `flink run -py` command and `--consumer-group`.

## What appears in the TaskManager log

- `VALIDATED_PASS event_id=...` and `VALIDATED_PASS_OUTPUT {...}`: structurally valid, first-seen records. Semantic malformed values intentionally also pass at this stage.
- `DEADLETTERED reason=missing_field:amount`, `invalid_type:amount`, or `malformed_json`: structurally invalid records. A JSON envelope containing `reason_code` and `raw_record` is sent to `transactions.deadletter`.
- `DEDUPLICATED_DROP event_id=...`: a repeated `event_id` was dropped by keyed state.
- `USER_WINDOW_AGGREGATE {...}`: a per-user 10-second event-time result with count, total minor-unit volume, and average transaction value.
- `LATE_EVENT_OUTPUT {...}`: a validated, deduplicated record that arrived after the window's 45-second allowed-lateness limit and was routed to `transactions.late`.

The deduplication TTL is 10 minutes. Step 2 verified exact-payload producer retry duplicates, not delayed retries with changed timestamps, so a ten-minute window gives ample retry coverage without retaining event IDs indefinitely. Revisit this TTL if later failure testing demonstrates a longer retry or recovery window.

## Caveats

This job intentionally has no merchant windows, semantic validation, anomaly detection, Postgres sink, or aggregate Kafka sink. Those belong to later steps.

PyFlink process functions and Python serialization add overhead relative to a Java implementation. That tradeoff is appropriate for consistency with the Python stack and this minimal job, but it must be measured during the later throughput benchmark rather than treated as production-scale evidence.
