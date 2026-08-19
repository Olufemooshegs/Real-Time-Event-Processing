"""Step 4 Flink job: structural validation, dead-letter routing, and deduplication only."""

from __future__ import annotations

import argparse
import json
from typing import Any, Iterator

from pyflink.common import Types
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
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.watermark_strategy import WatermarkStrategy


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
            print(f"DEADLETTERED reason={reason}", flush=True)
            ctx.output(self.deadletter_tag, deadletter)
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
            print(f"DEDUPLICATED_DROP event_id={event['event_id']}", flush=True)
            return

        self.seen.update(True)
        print(f"VALIDATED_PASS event_id={event['event_id']}", flush=True)
        yield value


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


def main() -> None:
    args = parse_args()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage("file:///opt/flink/checkpoints")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics(args.source_topic)
        .set_group_id(args.consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw_events = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "transactions.raw source"
    )

    deadletter_tag = OutputTag("structural-deadletter", Types.STRING())
    validated = raw_events.process(
        StructuralValidationProcessFunction(deadletter_tag), output_type=Types.STRING()
    )
    deadletters = validated.get_side_output(deadletter_tag)
    deadletters.sink_to(build_deadletter_sink(args.bootstrap_servers, args.deadletter_topic))

    deduplicated = validated.key_by(
        lambda value: json.loads(value)["event_id"], key_type=Types.STRING()
    ).process(DeduplicateByEventId(), output_type=Types.STRING())

    # Step 4 intentionally has no durable valid-event sink. This stdout sink is the manual
    # verification surface until Step 7 adds the Postgres and aggregate Kafka sinks.
    deduplicated.map(
        lambda value: f"VALIDATED_PASS_OUTPUT {value}", output_type=Types.STRING()
    ).print()

    env.execute("step-4-validation-and-deduplication")


if __name__ == "__main__":
    main()
