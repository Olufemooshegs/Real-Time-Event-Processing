"""Generate transaction events and publish them to Kafka asynchronously."""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from aiokafka import AIOKafkaProducer


VALID_CURRENCIES = ("NGN", "USD", "GBP", "EUR")
VALID_TRANSACTION_TYPES = ("purchase", "transfer", "bill_payment", "cash_withdrawal")
UNKNOWN_CURRENCIES = ("ZZZ", "INVALID")
UNKNOWN_TRANSACTION_TYPES = ("chargeback_test", "unknown_type")


@dataclass
class Counters:
    sent: int = 0
    duplicates: int = 0
    late: int = 0
    out_of_order: int = 0
    malformed: int = 0


@dataclass
class EventEnvelope:
    event: dict[str, Any]
    key: bytes


def utc_timestamp(delay_seconds: float = 0.0) -> str:
    value = datetime.now(timezone.utc) - timedelta(seconds=delay_seconds)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish configurable transaction events to Kafka."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions.raw")
    parser.add_argument("--rate", type=float, default=100.0, help="Target records/sec.")
    parser.add_argument("--producers", type=int, default=1, help="Concurrent async tasks.")
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Run time in seconds. Omit for streaming mode until Ctrl-C.",
    )
    parser.add_argument("--user-pool-size", type=int, default=1_000)
    parser.add_argument("--merchant-pool-size", type=int, default=100)
    parser.add_argument(
        "--user-distribution", choices=("uniform", "skewed"), default="uniform"
    )
    parser.add_argument(
        "--zipf-exponent",
        type=float,
        default=1.2,
        help="Skew strength when --user-distribution=skewed (must be > 0).",
    )
    parser.add_argument("--burst-probability", type=float, default=0.0)
    parser.add_argument("--burst-multiplier", type=float, default=2.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.0)
    parser.add_argument("--late-rate", type=float, default=0.0)
    parser.add_argument("--late-max-delay-seconds", type=float, default=30.0)
    parser.add_argument("--out-of-order-rate", type=float, default=0.0)
    parser.add_argument("--malformed-rate", type=float, default=0.0)
    parser.add_argument(
        "--malformed-mode", choices=("structural", "semantic"), default="structural"
    )
    parser.add_argument("--summary-interval", type=float, default=5.0)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.rate <= 0 or args.producers <= 0:
        raise ValueError("--rate and --producers must both be greater than zero")
    if args.duration is not None and args.duration <= 0:
        raise ValueError("--duration must be greater than zero when supplied")
    if args.user_pool_size <= 0 or args.merchant_pool_size <= 0:
        raise ValueError("pool sizes must be greater than zero")
    if args.zipf_exponent <= 0:
        raise ValueError("--zipf-exponent must be greater than zero")
    if args.burst_multiplier < 1:
        raise ValueError("--burst-multiplier must be at least 1")
    if args.late_max_delay_seconds < 0 or args.summary_interval <= 0:
        raise ValueError("delay and summary interval cannot be negative or zero")
    if args.late_rate > 0 and args.late_max_delay_seconds <= 0:
        raise ValueError(
            "--late-max-delay-seconds must be greater than zero when --late-rate is used"
        )
    for name in (
        "burst_probability",
        "duplicate_rate",
        "late_rate",
        "out_of_order_rate",
        "malformed_rate",
    ):
        if not 0 <= getattr(args, name) <= 1:
            raise ValueError(f"--{name.replace('_', '-')} must be between 0 and 1")


class TransactionFactory:
    def __init__(self, args: argparse.Namespace, rng: random.Random) -> None:
        self.args = args
        self.rng = rng
        self.users = [f"usr_{number:05d}" for number in range(1, args.user_pool_size + 1)]
        self.merchants = [
            f"merchant_{number:04d}" for number in range(1, args.merchant_pool_size + 1)
        ]
        self.user_weights: list[float] | None = None
        if args.user_distribution == "skewed":
            self.user_weights = [
                1 / (rank**args.zipf_exponent) for rank in range(1, len(self.users) + 1)
            ]

    def make_event(self, counters: Counters) -> EventEnvelope:
        user_id = (
            self.rng.choices(self.users, weights=self.user_weights, k=1)[0]
            if self.user_weights
            else self.rng.choice(self.users)
        )
        event_time = utc_timestamp()
        if self.rng.random() < self.args.late_rate:
            delay = self.rng.uniform(
                min(1.0, self.args.late_max_delay_seconds),
                self.args.late_max_delay_seconds,
            )
            event_time = utc_timestamp(delay)
            counters.late += 1

        event: dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "merchant_id": self.rng.choice(self.merchants),
            "amount": int(min(max(self.rng.lognormvariate(10.5, 0.9), 100), 5_000_000)),
            "currency": self.rng.choice(VALID_CURRENCIES),
            "transaction_type": self.rng.choice(VALID_TRANSACTION_TYPES),
            "event_time": event_time,
            "ingest_time": utc_timestamp(),
            "schema_version": 1,
        }
        if self.rng.random() < self.args.malformed_rate:
            self._make_malformed(event)
            counters.malformed += 1
        return EventEnvelope(event=event, key=user_id.encode("utf-8"))

    def _make_malformed(self, event: dict[str, Any]) -> None:
        if self.args.malformed_mode == "structural":
            if self.rng.choice((True, False)):
                event.pop(self.rng.choice(("merchant_id", "amount", "currency", "event_time")))
            else:
                event["amount"] = str(event["amount"])
            return

        choice = self.rng.choice(("amount", "currency", "transaction_type"))
        if choice == "amount":
            event["amount"] = -abs(int(event["amount"]))
        elif choice == "currency":
            event["currency"] = self.rng.choice(UNKNOWN_CURRENCIES)
        else:
            event["transaction_type"] = self.rng.choice(UNKNOWN_TRANSACTION_TYPES)


async def send_event(
    producer: AIOKafkaProducer,
    topic: str,
    envelope: EventEnvelope,
    counters: Counters,
    sent_events: deque[EventEnvelope],
) -> None:
    await producer.send_and_wait(topic, value=envelope.event, key=envelope.key)
    counters.sent += 1
    sent_events.append(envelope)


async def worker(
    worker_id: int,
    args: argparse.Namespace,
    producer: AIOKafkaProducer,
    counters: Counters,
    sent_events: deque[EventEnvelope],
    deadline: float | None,
) -> None:
    rng = random.Random()
    factory = TransactionFactory(args, rng)
    per_worker_rate = args.rate / args.producers
    pending: EventEnvelope | None = None

    while deadline is None or time.monotonic() < deadline:
        is_burst = rng.random() < args.burst_probability
        interval = 1 / (per_worker_rate * (args.burst_multiplier if is_burst else 1))

        if sent_events and rng.random() < args.duplicate_rate:
            duplicate = rng.choice(tuple(sent_events))
            await send_event(producer, args.topic, duplicate, counters, sent_events)
            counters.duplicates += 1
        else:
            current = factory.make_event(counters)

            # Keep one event buffered. Sending a newly generated event before the
            # buffered, older event creates a real event-time ordering inversion.
            if pending is None:
                pending = current
            elif rng.random() < args.out_of_order_rate:
                await send_event(producer, args.topic, current, counters, sent_events)
                counters.out_of_order += 1
            else:
                await send_event(producer, args.topic, pending, counters, sent_events)
                pending = current

        await asyncio.sleep(interval)

    if pending is not None:
        await send_event(producer, args.topic, pending, counters, sent_events)


async def reporter(counters: Counters, interval: float) -> None:
    last_sent = 0
    last_time = time.monotonic()
    while True:
        await asyncio.sleep(interval)
        now = time.monotonic()
        rate = (counters.sent - last_sent) / (now - last_time)
        print(
            "summary "
            f"sent={counters.sent} current_rate={rate:.1f}/s "
            f"duplicates={counters.duplicates} late={counters.late} "
            f"out_of_order={counters.out_of_order} malformed={counters.malformed}",
            flush=True,
        )
        last_sent = counters.sent
        last_time = now


async def run(args: argparse.Namespace) -> None:
    counters = Counters()
    sent_events: deque[EventEnvelope] = deque(maxlen=10_000)
    deadline = time.monotonic() + args.duration if args.duration else None
    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        compression_type="snappy",
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
    )
    await producer.start()
    print(
        f"publishing to {args.topic} via {args.bootstrap_servers} "
        f"at target {args.rate:g}/s with {args.producers} task(s)",
        flush=True,
    )
    report_task = asyncio.create_task(reporter(counters, args.summary_interval))
    workers = [
        asyncio.create_task(
            worker(index, args, producer, counters, sent_events, deadline)
        )
        for index in range(args.producers)
    ]
    try:
        await asyncio.gather(*workers)
    finally:
        report_task.cancel()
        await asyncio.gather(report_task, return_exceptions=True)
        await producer.stop()
        print(
            "final "
            f"sent={counters.sent} duplicates={counters.duplicates} late={counters.late} "
            f"out_of_order={counters.out_of_order} malformed={counters.malformed}",
            flush=True,
        )


def main() -> None:
    args = parse_args()
    try:
        validate_args(args)
        asyncio.run(run(args))
    except (ValueError, KeyboardInterrupt) as error:
        if isinstance(error, ValueError):
            raise SystemExit(f"configuration error: {error}") from error
        print("\nStopped by user.", flush=True)


if __name__ == "__main__":
    main()
