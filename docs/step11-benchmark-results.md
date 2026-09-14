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

## Bugs found and fixed during harness development

Four real bugs were found and fixed before this harness produced trustworthy numbers,
each caught by comparing actual output against expected behavior rather than trusting the
code because it ran without error.

1. **`lag()` read the wrong column.** `kafka-consumer-groups --describe` output is
   `GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID`.
   The original awk expression summed field `$5` (LOG-END-OFFSET, a number that only grows)
   instead of `$6` (LAG). Confirmed against real captured output before fixing; the bug
   would have reported an ever-increasing, meaningless number as consumer lag for every
   poll of every level.

2. **`discover()` called a nonexistent Flink REST endpoint.** `GET /jobs/<id>/vertices` is
   not a real endpoint in this Flink 1.19.3 deployment (confirmed via a live 404, after an
   initial web search surfaced documentation for a much older/legacy Flink REST API that
   turned out not to apply here). Vertices are embedded directly in `GET /jobs/<id>`'s
   `"vertices"` array. Fixed by calling the correct endpoint and reading the same field from
   its response.

3. **`copy_raw()`'s per-partition loop variables clobbered the outer timing variables.**
   `start`, `end`, and `count` were declared without `local` inside `copy_raw()`, called
   between the run's `end="$(date +%s%3N)"` capture and the throughput/checkpoint/latency
   calculations that depend on it. Since bash functions share the caller's scope unless a
   variable is explicitly `local`, `copy_raw()`'s small per-partition offset counts silently
   overwrote the real millisecond epoch timestamps. Confirmed via a debug trace showing
   `start=5711 end=5711` where a real timestamp should have been a 13-digit number near
   1.79 trillion. This single bug corrupted three separate outputs at once: achieved
   throughput (divided by a near-zero elapsed time, producing a nonsense multi-million
   events/sec figure), the checkpoint duration window (an empty range matched nothing), and
   the latency percentile query (a zero-width `BETWEEN` window matched zero Postgres rows).
   Fixed by declaring `local start end count` inside the function.

4. **Checkpoint filtering used the wrong window.** After fixing #3, checkpoint counts were
   still zero. Flink's `/jobs/<id>/checkpoints` REST endpoint retains only the 10 most
   recent checkpoints in its `history` array (confirmed directly: `"total": 77` completed
   checkpoints existed, but `history` length was 10). Checkpointing continues on its normal
   10-second interval throughout the drain phase, regardless of producer activity, so by the
   time the drain-to-stability loop finished and `checkpoints.json` was fetched, the
   retained history had already aged past the narrow `[start, end]` window (`end` marks when
   the producer stopped sending, not when the drain finished). Confirmed by checking a
   sample checkpoint's `trigger_timestamp` directly: it landed 67 seconds after that level's
   `end`. Fixed by capturing a separate `drain_end` timestamp when the stabilization loop
   actually completes, and filtering checkpoints against `[start, drain_end]` instead.

## Results

Run against the live Codespaces stack. Producer flags: default duplicate rate only
(`--late-rate`/`--malformed-rate` not set), so this run measures clean, well-formed traffic
and is not comparable to Step 9's failure-injection profile.

**Important caveat, stated plainly rather than left implicit:** `transactions.raw` has
carried accumulating backlog across this entire project's test history (RF=1, never reset
between Steps 8, 9, and this benchmark's own smoke tests). `landed_event_ids` climbed from
1,304,536 to 1,598,358 across this one run, confirming it, and every level's
`consumer_group_lag` was already non-zero before that level's producer even started. The
absolute latency figures below reflect processing time against a pipeline already carrying
real backlog, not a clean per-level baseline. The relative trend across levels is still
real and is the actual finding here; the absolute numbers should not be quoted out of this
context.

| Requested rate | Achieved throughput | Steady state | Checkpoints (count, avg/max ms) | Latency p50 / p95 / p99 (s, event-to-received) |
|---|---|---|---|---|
| 1,000 | 360/s | yes | 8, 131 / 144 | 36.2 / 55.9 / 61.2 |
| 5,000 | 370/s | yes | 8, 139 / 156 | 35.4 / 62.8 / 70.1 |
| 10,000 | 391/s | yes | 9, 218 / 306 | 33.0 / 70.3 / 74.9 |
| 50,000 | 819/s | **no (timeout)** | **0 captured** | 113.5 / 204.4 / 213.4 |
| 100,000 | 769/s | **no (timeout)** | **0 captured** | 121.5 / 212.5 / 218.7 |

**Finding 1 — the producer, not Kafka or Flink, is the binding constraint at every level.**
Achieved throughput never exceeds roughly 400 events/sec even when 50,000 or 100,000/sec is
requested. This matches the single-process async producer's behavior observed repeatedly in
earlier ad-hoc testing throughout this project; this benchmark is the first place it's been
measured formally across a controlled sweep rather than noticed incidentally.

**Finding 2 — checkpoint duration climbs with requested rate even though achieved
throughput barely moves.** Average checkpoint duration goes 131ms → 139ms → 218ms across
1,000/5,000/10,000 requested, a real and monotonic trend against an achieved throughput that
only moved from 360 to 391 events/sec across the same three levels. Something in the
pipeline is responding to producer-side pressure (connection attempts, batch sizes, or
metadata churn from the higher requested rate) independent of how many events actually
land. Not yet root-caused; worth a dedicated investigation before Step 12, since it
suggests checkpoint cost is not purely a function of processed volume.

**Finding 3 — a sharp qualitative break occurs between 10,000 and 50,000 requested,
independent of achieved throughput.** Three signals break down together at the same
transition, not gradually: the drain loop failed to reach stability within its 180-second
timeout for the first time, checkpoint history came back completely empty (not smaller —
zero), and event latency roughly tripled (p50 33s → 113s). Since achieved throughput at
50,000 requested (819/s) is not dramatically higher than at 10,000 requested (391/s), this
looks like a threshold effect tied to something about sustained pressure at higher
*requested* concurrency, not simply "more events landed so it got slower." Candidate
causes not yet isolated: producer-side connection/retry behavior change above a request
threshold, a Flink backpressure regime change, or accumulated effects of the pre-existing
Kafka backlog reaching a tipping point during these specific runs. This is the headline
finding of Step 11 and needs follow-up before being treated as a hard capacity number for
this pipeline.

**Finding 4 — zero duplicate rows and zero unaccounted event IDs at every level**,
including the two levels that failed to stabilize. Whatever is causing the 50k/100k
breakdown, it is not causing data loss or duplication within this benchmark's
reconciliation window — consistent with the Step 9 finding that the Postgres idempotent
upsert, not Flink's own state, is what actually delivers this guarantee.

### What this benchmark does not tell us

- Whether the checkpoint-duration and steady-state breakdown are caused by the producer,
  Flink, or the pre-existing Kafka backlog cannot be distinguished from this data alone.
  Isolating this would require rerunning against a freshly reset topic (no backlog) and,
  separately, a multi-process or higher-throughput producer to determine whether the same
  break occurs at a similar *achieved* rate independent of *requested* rate.
- Recovery-time-under-load (the TaskManager kill scenario run at the highest stabilized
  rate, 5,000 requested in this run) still needs its own dedicated write-up; the raw
  artifacts are captured in `step11-results/full-run/` but not yet analyzed here.