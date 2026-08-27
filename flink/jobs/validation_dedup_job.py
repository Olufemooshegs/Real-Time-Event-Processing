"""Step 5 Flink job: event-time processing, watermarks, allowed lateness, windowed aggregation.

Late-data routing note (found during Step 5 verification):
PyFlink 1.19.3's WindowedStream.side_output_late_data() did not route late events to the
configured side output in this environment, despite exhaustive verification that every
upstream piece was correct: timestamp assignment produced sane epoch-millisecond values,
the watermark strategy's output watermark advanced correctly and matched live time, the
window operator's own input watermark was confirmed current on both parallel subtasks (no
hot-key/partition-stall issue), and swapping the terminal window operation from aggregate()
to reduce() made no difference. Genuinely late events (up to ~28s+ per Step 2's empirical
data, tested here up to 90s) reliably reached the windowing stage with large, real
watermark-vs-event-time gaps, confirmed via a temporary debug probe, yet the built-in
late-data side output never fired.

Given that, lateness routing here is implemented manually via LatenessRouter, a
ProcessFunction that compares each event's own timestamp against
ctx.timer_service().current_watermark() directly -- the same primitive the debug probe used
to confirm the watermark was live and correct. This sidesteps the non-functional built-in
mechanism entirely rather than continuing to work around a suspected version-specific
PyFlink Python API limitation.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Iterable, Iterator

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import FileSystemCheckpointStorage
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream import CheckpointingMode, OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows


REQUIRED_FIELDS: dict[str, type] = {
    "event_id": str,
    "user_id": str,
    "merchant_id": str,
    "amount": int,
    "currency": str,
    "transaction_type": str,
    "event_time": str,
    "ingest_time": str,
    "schema_version": int,
}

# Step 2 observed late arrival up to ~28s, distinct from the ~20ms in-order jitter used for
# the watermark's own out-of-orderness bound below. This is deliberately far larger than
# that bound: allowed lateness is about retaining an already-closed window for late updates,
# not about ordering jitter within a still-open window.
ALLOWED_LATENESS_MS = 45_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--source-topic", default="transactions.raw")
    parser.add_argument("--deadletter-topic", default="transactions.deadletter")
    parser.add_argument("--late-topic", default="transactions.late")
    parser.add_argument("--consumer-group", default="flink-validation-dedup-v1")
    return parser.parse_args()


def structural_validation_error(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    """Validate JSON shape and types only; semantic business rules intentionally pass here."""
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return None, "malformed_json"

    if not isinstance(event, dict):
        return None, "invalid_type:root"

    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in event:
            return None, f"missing_field:{field}"
        value = event[field]
        # Python bool is a subclass of int, but it is not a valid numeric amount/version.
        if not isinstance(value, expected_type) or (
            expected_type is int and isinstance(value, bool)
        ):
            return None, f"invalid_type:{field}"
    return event, None


class StructuralValidationProcessFunction(ProcessFunction):
    def __init__(self, deadletter_tag: OutputTag) -> None:
        self.deadletter_tag = deadletter_tag

    def process_element(self, value: str, ctx: ProcessFunction.Context) -> Iterator[str]:
        event, reason = structural_validation_error(value)
        if reason:
            deadletter = json.dumps(
                {"reason_code": reason, "raw_record": value}, separators=(",", ":")
            )
            print(f"DEADLETTERED reason={reason}")
            # PyFlink's Python ProcessFunction API does not expose ctx.output(tag, value)
            # the way the Java API does. Side outputs are emitted by yielding a
            # (OutputTag, value) tuple alongside normal yields.
            yield self.deadletter_tag, deadletter
            return

        # Negative amounts, unknown currencies, and unknown transaction types are semantic
        # malformed cases. They deliberately pass Step 4 and are handled in the Step 6 work.
        yield json.dumps(event, separators=(",", ":"))


class DeduplicateByEventId(KeyedProcessFunction):
    """Drop exact producer retry records by event_id using a bounded keyed-state TTL."""

    def open(self, runtime_context: Any) -> None:
        ttl_config = (
            StateTtlConfig.new_builder(Time.minutes(10))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        descriptor = ValueStateDescriptor("seen-event-id", Types.BOOLEAN())
        descriptor.enable_time_to_live(ttl_config)
        self.seen = runtime_context.get_state(descriptor)

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context) -> Iterator[str]:
        event = json.loads(value)
        if self.seen.value():
            print(f"DEDUPLICATED_DROP event_id={event['event_id']}")
            return

        self.seen.update(True)
        print(f"VALIDATED_PASS event_id={event['event_id']}")
        yield value


class EventTimeAssigner(TimestampAssigner):
    """Assign the producer's ISO-8601 event_time rather than Kafka arrival time."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        event_time = json.loads(value)["event_time"]
        return int(datetime.fromisoformat(event_time.replace("Z", "+00:00")).timestamp() * 1000)


class LatenessRouter(ProcessFunction):
    """Manually route events past allowed lateness to a side output.

    Replaces WindowedStream.side_output_late_data(), which was confirmed non-functional
    in this PyFlink 1.19.3 environment (see module docstring). Runs before windowing:
    compares each event's own event-time timestamp against this operator's current
    watermark directly, using the same ctx.timer_service().current_watermark() primitive
    that the Step 5 debugging probe used to confirm the watermark itself was live and
    correct throughout this pipeline.
    """

    def __init__(self, late_tag: OutputTag, allowed_lateness_ms: int) -> None:
        self.late_tag = late_tag
        self.allowed_lateness_ms = allowed_lateness_ms

    def process_element(self, value: str, ctx: ProcessFunction.Context) -> Iterator[str]:
        event_ts = ctx.timestamp()
        watermark = ctx.timer_service().current_watermark()
        if event_ts is not None and (watermark - event_ts) > self.allowed_lateness_ms:
            event = json.loads(value)
            print(
                f"LATE_EVENT_ROUTED event_id={event.get('event_id')} "
                f"lag_ms={watermark - event_ts}"
            )
            yield self.late_tag, value
            return
        yield value


class TransactionAggregate(AggregateFunction):
    """Incrementally maintain count and minor-unit volume for each user/window."""

    def create_accumulator(self) -> tuple[int, int]:
        return 0, 0

    def add(self, value: str, accumulator: tuple[int, int]) -> tuple[int, int]:
        amount = json.loads(value)["amount"]
        return accumulator[0] + 1, accumulator[1] + amount

    def get_result(self, accumulator: tuple[int, int]) -> tuple[int, int]:
        return accumulator

    def merge(
        self, first: tuple[int, int], second: tuple[int, int]
    ) -> tuple[int, int]:
        return first[0] + second[0], first[1] + second[1]


class FormatUserWindowAggregate(ProcessWindowFunction):
    """Attach user and event-time window bounds to the incremental aggregate."""

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[tuple[int, int]],
    ) -> Iterator[str]:
        count, total_volume = next(iter(elements))
        result = {
            "user_id": key,
            "window_start": context.window().start,
            "window_end": context.window().end,
            "transaction_count": count,
            "total_volume": total_volume,
            "average_transaction_value": total_volume / count,
        }
        yield json.dumps(result, separators=(",", ":"))


def build_deadletter_sink(bootstrap_servers: str, topic: str) -> KafkaSink:
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(serializer)
        # Dead letters are diagnostic output. Duplicate diagnostics are acceptable here.
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def build_diagnostic_sink(bootstrap_servers: str, topic: str) -> KafkaSink:
    """Route diagnostic streams to Kafka; at-least-once is sufficient for diagnostics."""
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    args = parse_args()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage(FileSystemCheckpointStorage("file:///opt/flink/checkpoints"))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics(args.source_topic)
        .set_group_id(args.consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw_events = env.from_source(source, WatermarkStrategy.no_watermarks(), "transactions.raw source")

    deadletter_tag = OutputTag("structural-deadletter", Types.STRING())
    validated = raw_events.process(
        StructuralValidationProcessFunction(deadletter_tag), output_type=Types.STRING()
    )
    deadletters = validated.get_side_output(deadletter_tag)
    deadletters.sink_to(build_deadletter_sink(args.bootstrap_servers, args.deadletter_topic))

    deduplicated = validated.key_by(
        lambda value: json.loads(value)["event_id"], key_type=Types.STRING()
    ).process(DeduplicateByEventId(), output_type=Types.STRING())

    # Step 2 observed ~20 ms within-partition inversions. A 50 ms bound (2.5x that measured
    # scale) absorbs normal ordering jitter without delaying event-time progress by seconds.
    # The 30 s idleness timeout prevents quiet Kafka partitions, likely under known key skew,
    # from indefinitely holding back this source's watermark.
    event_time_events = deduplicated.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(50))
        .with_timestamp_assigner(EventTimeAssigner())
        .with_idleness(Duration.of_seconds(30))
    )

    late_event_tag = OutputTag("events-past-allowed-lateness", Types.STRING())
    # Manual lateness routing -- see module docstring and LatenessRouter for why this
    # replaces WindowedStream.side_output_late_data(). Runs before windowing so genuinely
    # late events never enter window state at all.
    routed = event_time_events.process(
        LatenessRouter(late_event_tag, ALLOWED_LATENESS_MS), output_type=Types.STRING()
    )
    late_events = routed.get_side_output(late_event_tag)
    late_events.sink_to(build_diagnostic_sink(args.bootstrap_servers, args.late_topic))
    late_events.map(
        lambda value: f"LATE_EVENT_OUTPUT {value}", output_type=Types.STRING()
    ).print()

    user_windows = routed.key_by(
        lambda value: json.loads(value)["user_id"], key_type=Types.STRING()
    ).window(TumblingEventTimeWindows.of(Time.seconds(10)))
    aggregates = user_windows.aggregate(
        TransactionAggregate(),
        FormatUserWindowAggregate(),
        accumulator_type=Types.TUPLE([Types.LONG(), Types.LONG()]),
        output_type=Types.STRING(),
    )

    # Step 5 has no durable aggregate sink. Stdout is the inspection surface until Step 7.
    aggregates.map(
        lambda value: f"USER_WINDOW_AGGREGATE {value}", output_type=Types.STRING()
    ).print()

    # Keep the existing Step 4 per-record verification output additive to windowing.
    deduplicated.map(
        lambda value: f"VALIDATED_PASS_OUTPUT {value}", output_type=Types.STRING()
    ).print()

    env.execute("step-5-event-time-validation-and-deduplication")


if __name__ == "__main__":
    main()