"""Step 4 Flink job: structural validation, dead-letter routing, and deduplication only."""

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


class DebugWatermarkProbe(ProcessFunction):
    def process_element(self, value, ctx):
        event = json.loads(value)
        print(f"DEBUG_PROBE event_id={event['event_id']} "
              f"element_ts={ctx.timestamp()} "
              f"current_watermark={ctx.timer_service().current_watermark()}")
        yield value


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
    event_time_events = event_time_events.process(DebugWatermarkProbe(), output_type=Types.STRING())

    late_event_tag = OutputTag("events-past-allowed-lateness", Types.STRING())
    # Step 2 observed late arrival up to ~28 s, distinct from the ~20 ms in-order jitter above.
    # 45 s provides measured headroom while deliberately remaining far larger than the 50 ms
    # watermark bound: allowed lateness retains an already-closed window for late updates.
    user_windows = (
        event_time_events.key_by(
            lambda value: json.loads(value)["user_id"], key_type=Types.STRING()
        )
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .allowed_lateness(45_000)
        .side_output_late_data(late_event_tag)
    )
    # TEMPORARY DIAGNOSTIC: swapped aggregate() for reduce() to test whether aggregate()
    # itself is dropping the late-data side output in this PyFlink version. REVERT AFTER TEST.
    def _debug_reduce(a, b):
        ea, eb = json.loads(a), json.loads(b)
        return a
    aggregates = user_windows.reduce(_debug_reduce)

    # WindowedStream.side_output_late_data captures records dropped only after watermark >
    # window end + allowed lateness. Preserve the original validated/deduplicated payload in
    # transactions.late instead of silently discarding it.
    late_events = aggregates.get_side_output(late_event_tag)
    late_events.sink_to(build_diagnostic_sink(args.bootstrap_servers, args.late_topic))
    late_events.map(
        lambda value: f"LATE_EVENT_OUTPUT {value}", output_type=Types.STRING()
    ).print()

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
