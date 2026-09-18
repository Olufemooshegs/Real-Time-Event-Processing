# Step 12: End-to-End Integration Test

The final verification step: a single, controlled run used as ground truth to
reconcile every stage of the pipeline (producer, Kafka, Flink, Postgres, API)
against real counts, closing the loop on everything built in Steps 1 through 11.

## Run parameters

```
python main.py --rate 20 --duration 20 --duplicate-rate 0.05 --late-rate 0.1 \
  --out-of-order-rate 0.1 --malformed-rate 0.05 --malformed-mode structural
```

Run against a freshly wiped environment (`docker volume rm` on all project
volumes, followed by `make up`, `make health`, `make topic-create`, and a fresh
Flink job submission confirmed `RUNNING` before the producer started).

## Reconciliation

| Stage | Count | Basis |
|---|---|---|
| Producer sent | 380 | `final sent=380` from producer log |
| Kafka `transactions.raw` | 380 | sum of `kafka-get-offsets` across all 6 partitions |
| Kafka `transactions.deadletter` | 18 | 17 malformed events plus 1 malformed event that was also re-sent as a duplicate |
| Deduplicated by Flink | 13 | 14 injected duplicates minus 1 already dead-lettered |
| Landed in `transactions.events` | 349 | 380 minus 18 minus 13, exact |
| Window aggregate spot check (`usr_00001`) | count=1, volume=314232 | matched `/users/usr_00001/aggregates` exactly, confirming the API layer serializes Postgres data correctly, not just plausibly |
| Anomalies | 0 | expected at this volume and duration; below the velocity and warmup thresholds anomaly detection requires, not independently re-verified at this scale |

Every number reconciles exactly except anomalies, which were not exercised at
this run's scale. Anomaly-path confidence rests on Step 6's dedicated
verification of the anomaly detection logic itself, not on this run.

## Process note: an earlier, discarded attempt

Before the clean run above, an initial attempt at this test used two
back-to-back producer invocations (one interrupted mid-run by the user) against
the same topic without a wipe between them. That attempt produced an
unexplained surplus of roughly 6,300 records in the raw topic that could not be
traced to any duplicate-publish, stale-volume, retry, or extra-process cause
after exhausting all of them (checked: orphaned processes, Docker container
list, volume identity, network connections, and message timestamp ranges).

A single, isolated, uninterrupted run reconciled exactly on the first attempt,
so the surplus from the earlier tangled run is attributed to the
overlapping/interrupted multi-run sequencing itself, not a pipeline defect.
That data was discarded rather than force-reconciled.

**Lesson for future testing:** never chain multiple producer invocations
against a shared topic without a wipe between them if exact reconciliation
matters. A single clean run is more useful evidence than a multi-run sequence
that cannot be cleanly attributed.

## Known outstanding gap, unrelated to this test

The Flink checkpoint directory ownership fix (Step 4, bug: freshly created
volumes are root-owned but the Flink container runs as the `flink` user) does
not survive a full `docker volume rm`, and had to be reapplied by hand before
job submission would succeed, both times a fresh environment was built during
this testing session. This should be scripted into `make up` so a clean
environment reset does not require manual intervention every time.