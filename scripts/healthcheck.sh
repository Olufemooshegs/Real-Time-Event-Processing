#!/usr/bin/env bash
set -euo pipefail

pass() { printf 'PASS: %s\n' "$1"; }
fail() { printf 'FAIL: %s\n' "$1" >&2; exit 1; }

if docker compose exec -T kafka \
  kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
  pass 'Kafka broker is accepting connections'
else
  fail 'Kafka broker is not accepting connections'
fi

health_topic="healthcheck.probe.$(date +%s)"
if docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic "$health_topic" \
  --partitions 1 \
  --replication-factor 1 >/dev/null 2>&1 \
  && docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --describe \
    --topic "$health_topic" >/dev/null 2>&1; then
  pass 'Kafka topic creation and description succeeded'
else
  fail 'Kafka topic creation or description failed'
fi

if docker compose exec -T postgres sh -c \
  'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null'; then
  pass 'Postgres is accepting connections'
else
  fail 'Postgres is not accepting connections'
fi
