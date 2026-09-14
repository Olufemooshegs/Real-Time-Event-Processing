#!/usr/bin/env bash
set -euo pipefail

# Reset only Kafka's durable volume. Postgres and Flink state volumes are intentionally kept.
log() { printf '[step12-reset] %s\n' "$*"; }
die() { printf '[step12-reset] FAIL: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null || die "docker is required"

mapfile -t kafka_volumes < <(docker volume ls --filter label=com.docker.compose.volume=kafka-data --format '{{.Name}}')
(( ${#kafka_volumes[@]} == 1 )) || die "expected exactly one Compose kafka-data volume, found ${#kafka_volumes[@]}"
kafka_volume="${kafka_volumes[0]}"
volume_label="$(docker volume inspect "$kafka_volume" --format '{{index .Labels "com.docker.compose.volume"}}')"
[[ "$volume_label" == "kafka-data" ]] || die "refusing to remove $kafka_volume because its Compose volume label is not kafka-data"

log "Stopping the Compose stack without -v; preserving Postgres and Flink volumes."
docker compose down
log "Removing only Kafka volume: $kafka_volume"
docker volume rm "$kafka_volume"
docker compose up -d
for attempt in $(seq 1 30); do
  if docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1 || die "Kafka did not become ready before topic apply"
make topics-apply

for attempt in $(seq 1 30); do
  if docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic transactions.raw > /tmp/step12-raw-offsets.txt 2>/dev/null; then
    break
  fi
  sleep 2
done
[[ -s /tmp/step12-raw-offsets.txt ]] || die "could not read transactions.raw offsets after reset"
partitions="$(awk -F: '$1=="transactions.raw" {print $2}' /tmp/step12-raw-offsets.txt | sort -u | wc -l | tr -d '[:space:]')"
nonzero="$(awk -F: '$1=="transactions.raw" && $NF!=0 {count++} END {print count+0}' /tmp/step12-raw-offsets.txt)"
[[ "$partitions" == "6" && "$nonzero" == "0" ]] || { cat /tmp/step12-raw-offsets.txt >&2; die "transactions.raw must have six zero offsets after reset"; }
log "PASS: transactions.raw has six partitions at offset 0."
