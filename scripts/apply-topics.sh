#!/usr/bin/env bash
set -euo pipefail

# Applies the intentionally flat infrastructure/kafka/topics.yml manifest. The raw topic was
# once deleted and immediately recreated during Step 2, and Kafka's asynchronous deletion led
# to a silent partition-count mismatch. This script never deletes topics: it creates missing
# topics, increases partition counts only when safe, and reconciles mutable topic settings.

manifest="${1:-infrastructure/kafka/topics.yml}"

if [[ ! -f "$manifest" ]]; then
  printf 'ERROR: Topic manifest not found: %s\n' "$manifest" >&2
  exit 1
fi

topic_names=$(awk '/^  - name: / { print $3 }' "$manifest")
if [[ -z "$topic_names" ]]; then
  printf 'ERROR: No topics found in %s\n' "$manifest" >&2
  exit 1
fi

value_for() {
  local topic="$1"
  local key="$2"
  awk -v topic="$topic" -v key="$key" '
    /^  - name: / { active = ($3 == topic); next }
    active && $1 == key ":" {
      value = $2
      sub(/#.*/, "", value)
      print value
      exit
    }
  ' "$manifest"
}

kafka_exec() {
  docker compose exec -T kafka "$@"
}

for topic in $topic_names; do
  partitions=$(value_for "$topic" "partitions")
  replication_factor=$(value_for "$topic" "replication_factor")
  cleanup_policy=$(value_for "$topic" "cleanup.policy")
  retention_ms=$(value_for "$topic" "retention.ms")
  compression_type=$(value_for "$topic" "compression.type")
  min_insync_replicas=$(value_for "$topic" "min.insync.replicas")

  for required in partitions replication_factor cleanup_policy retention_ms compression_type min_insync_replicas; do
    if [[ -z "${!required}" ]]; then
      printf 'ERROR: %s is missing %s in %s\n' "$topic" "$required" "$manifest" >&2
      exit 1
    fi
  done

  kafka_exec kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor "$replication_factor"

  description=$(kafka_exec kafka-topics --bootstrap-server kafka:29092 --describe --topic "$topic")
  current_partitions=$(sed -n 's/.*PartitionCount:[[:space:]]*\([0-9][0-9]*\).*/\1/p' <<<"$description" | head -n 1)
  if [[ -z "$current_partitions" ]]; then
    printf 'ERROR: Could not read partition count for %s\n' "$topic" >&2
    exit 1
  fi
  if (( current_partitions < partitions )); then
    kafka_exec kafka-topics --bootstrap-server kafka:29092 --alter \
      --topic "$topic" --partitions "$partitions"
    printf 'UPDATED: %s partitions %s -> %s\n' "$topic" "$current_partitions" "$partitions"
  elif (( current_partitions > partitions )); then
    printf 'ERROR: %s has %s partitions but the manifest declares %s. Kafka cannot shrink partitions safely; update the manifest or handle this explicitly.\n' \
      "$topic" "$current_partitions" "$partitions" >&2
    exit 1
  else
    printf 'OK: %s has %s partitions\n' "$topic" "$partitions"
  fi

  kafka_exec kafka-configs --bootstrap-server kafka:29092 --entity-type topics \
    --entity-name "$topic" --alter --add-config \
    "cleanup.policy=$cleanup_policy,retention.ms=$retention_ms,compression.type=$compression_type,min.insync.replicas=$min_insync_replicas"
  printf 'APPLIED: %s topic configuration\n' "$topic"
done
