# Transaction generator

This is a plain local Python process. It writes only to Kafka's `transactions.raw` topic and does not modify Postgres or Docker Compose.

## Run

Start the Step 1 infrastructure first, then create and activate a virtual environment:

```bash
cd producers/transaction_generator
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py --duration 30
```

On Windows PowerShell, activate with `.venv\Scripts\Activate.ps1` instead. Use `Ctrl-C` when no `--duration` is supplied.

## Options

- `--bootstrap-servers`: Kafka address, default `localhost:9092`.
- `--topic`: target topic, default `transactions.raw`.
- `--rate`: target total records per second across all tasks, default `100`.
- `--producers`: number of concurrent async producer tasks, default `1`.
- `--duration`: run time in seconds. Omit for continuous streaming.
- `--user-pool-size` and `--merchant-pool-size`: reusable ID pool sizes, defaults `1000` and `100`.
- `--user-distribution`: `uniform` or `skewed`. Skewed uses Zipf-like weighted selection.
- `--zipf-exponent`: skew strength for `skewed` users, default `1.2`; larger values concentrate more traffic in the earliest user IDs.
- `--burst-probability`: probability that a generated record uses burst pacing, default `0`.
- `--burst-multiplier`: rate multiplier for a burst-paced record, default `2`.
- `--duplicate-rate`: probability of resending a cached record with its original `event_id`.
- `--late-rate`: probability of assigning an event time in the past.
- `--late-max-delay-seconds`: maximum late-event delay, default `30` seconds.
- `--out-of-order-rate`: probability of sending a newly generated event before one buffered older event.
- `--malformed-rate`: probability of corrupting a newly generated event.
- `--malformed-mode`: `structural` removes a required field or changes `amount` to a string; `semantic` emits a negative amount, invalid currency, or invalid transaction type.
- `--summary-interval`: stdout summary interval in seconds, default `5`.

Example failure-injection run:

```bash
python main.py --rate 500 --producers 4 --duration 60 \
  --user-distribution skewed --zipf-exponent 1.4 \
  --duplicate-rate 0.02 --late-rate 0.05 --late-max-delay-seconds 45 \
  --out-of-order-rate 0.05 --malformed-rate 0.01 --malformed-mode semantic
```

The producer uses Kafka `acks=all`, idempotence, Snappy compression, and `user_id` as the message key.

## Injection accuracy

Duplicate, late, and malformed counters increment when those behaviors are actually selected for emission. The out-of-order counter increments when a newer event is sent before an older event buffered by the same async task.

At high throughput, target pacing and the observed global ordering are approximate. Async scheduling, broker backpressure, concurrent tasks, retries, and Kafka partitioning can shift the observed rate and interleave records from different tasks. In particular, out-of-order behavior is guaranteed only relative to the local task's buffered event, not as a strict total ordering across all producers or Kafka partitions.
