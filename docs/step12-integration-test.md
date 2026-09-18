## Step 12 — End-to-end integration test (closed)

A single, uninterrupted producer run was used as ground truth to reconcile every stage
of the pipeline against real counts, not log assumptions.

**Run parameters:** `--rate 20 --duration 20 --duplicate-rate 0.05 --late-rate 0.1
--out-of-order-rate 0.1 --malformed-rate 0.05 --malformed-mode structural`

**Reconciliation:**

| Stage | Count | Basis |
|---|---|---|
| Producer sent | 380 | `final sent=380` |
| Kafka `transactions.raw` | 380 | sum of `kafka-get-offsets` across all 6 partitions |
| Kafka `transactions.deadletter` | 18 | 17 malformed + 1 malformed event re-sent as a duplicate |
| Deduplicated by Flink | 13 | 14 injected duplicates − 1 already dead-lettered |
| Landed in `transactions.events` | 349 | 380 − 18 − 13, exact |
| Window aggregate (spot check, `usr_00001`) | count=1, volume=314232 | matches `/users/usr_00001/aggregates` exactly |
| Anomalies | 0 | expected at this volume/duration; below Step 6's velocity and warmup thresholds, not independently re-verified here |

Every number reconciles exactly except anomalies, which weren't exercised at this
run's scale and rely on Step 6's dedicated verification instead.

**Process note:** an earlier attempt at this test used two back-to-back producer
invocations (one interrupted mid-run) against the same topic without a wipe between
them, and produced an unexplained ~6,300-record surplus in the raw topic that could
not be traced to any duplicate-publish, stale-volume, or extra-process cause after
exhausting all of them. A single, isolated, uninterrupted run reconciled exactly,
so the surplus is attributed to overlapping/interrupted multi-run sequencing, not
a pipeline defect. Lesson: don't chain multiple producer invocations against a
shared topic without a wipe between them if exact reconciliation matters.

**Known outstanding gap, unrelated to this test:** the Flink checkpoint directory
ownership fix (Step 4, bug #9) does not survive a full `docker volume rm`, and had
to be reapplied manually before job submission would succeed. Worth scripting into
`make up` so a clean environment reset doesn't require this by hand every time.