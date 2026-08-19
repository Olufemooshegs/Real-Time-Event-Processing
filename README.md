# Real-Time Event Processing & Analytics Platform

Status: **in progress — Step 2 of 12 complete, moving to Step 3 (see docs/architecture-design-doc.md for the full plan)**

This README is updated after each step with what's actually running and verified, not what's
planned. If something isn't listed under "What's running" below, it doesn't exist yet.

---

## What's running right now

- **Kafka**, single broker, KRaft mode (no Zookeeper), topic `transactions.raw` with 6
  partitions, replication factor 1.
- **Postgres**, with an initial `transactions.events` schema shell (not the real analytics
  schema yet — that comes in Step 7).
- **Async transaction producer** (`producers/transaction_generator/main.py`, `aiokafka`),
  running as a plain local Python process, not containerized (deliberate choice for this
  phase — see "Decisions" below).

## What's explicitly NOT built yet

- Flink (validation, deduplication, watermarks, windowing, stateful anomaly detection)
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

## How to run it

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

## Development workflow notes

- Commit and push after local generation, before switching into the Codespace.
- Run `git pull` as the first action every time a Codespace session starts.
- Don't mark a step "done" on Codex's word alone — every step requires independent
  verification against real terminal output before moving to the next one.