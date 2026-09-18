#!/usr/bin/env bash
set -euo pipefail

OUT="${1:-step12-results/$(date -u +%Y%m%dT%H%M%SZ)}"
API_URL="${API_URL:-http://localhost:8000}"
POLL=5
TIMEOUT=180
PROFILE=(--bootstrap-servers localhost:9092 --topic transactions.raw --rate 100 --duration 60 --user-pool-size 5 --merchant-pool-size 10 --duplicate-rate 0.10 --late-rate 0.10 --late-max-delay-seconds 30 --out-of-order-rate 0.10 --malformed-rate 0.10 --malformed-mode structural --summary-interval 5)
mkdir -p "$OUT"
log() { printf '[step12] %s\n' "$*"; }
die() { printf '[step12] FAIL: %s\n' "$*" >&2; exit 1; }
offsets() { docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic "$1"; }
total() { awk -F: '{sum += $NF} END {print sum+0}' "$1"; }
pg_count() { docker compose exec -T postgres sh -c 'psql -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select count(*) from transactions.events"' | tr -d '[:space:]'; }
copy_raw() { local topic="$1" before="$2" after="$3" output="$4"; : > "$output"; for p in $(seq 0 5); do local start end count; start=$(awk -F: -v p="$p" '$2==p{print $NF}' "$before"); end=$(awk -F: -v p="$p" '$2==p{print $NF}' "$after"); count=$((${end:-0}-${start:-0})); ((count>0)) || continue; docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic "$topic" --partition "$p" --offset "${start:-0}" --max-messages "$count" --timeout-ms 30000 2>/dev/null >> "$output" || true; done; }
reconcile() { local start="$1" end="$2"; docker compose exec -T postgres sh -c "psql -At -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c \"select event_id from transactions.events where received_at between to_timestamp($start/1000.0) and to_timestamp($end/1000.0)\"" > "$OUT/events.ids"; docker compose exec -T postgres sh -c "psql -At -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c \"select event_id from transactions.events where received_at between to_timestamp($start/1000.0) and to_timestamp($end/1000.0) group by event_id having count(*) > 1\"" > "$OUT/duplicate.ids"; python3 scripts/reconcile_event_ids.py "$OUT"; }

bash scripts/step12-reset-kafka-backlog.sh
make flink-health
make flink-job-submit > "$OUT/flink-submit.log" 2>&1
for attempt in $(seq 1 30); do
  docker compose exec -T jobmanager flink list > "$OUT/flink-list.txt" 2>&1 || true
  if grep -q RUNNING "$OUT/flink-list.txt"; then break; fi
  sleep 2
done
grep -q RUNNING "$OUT/flink-list.txt" || die "Flink job did not reach RUNNING"

printf '%q ' "${PROFILE[@]}" > "$OUT/producer-profile.txt"; printf '\n' >> "$OUT/producer-profile.txt"
offsets transactions.raw > "$OUT/raw-before.offsets"; offsets transactions.deadletter > "$OUT/deadletter-before.offsets"; start="$(date +%s%3N)"
python3 producers/transaction_generator/main.py "${PROFILE[@]}" > "$OUT/producer.log" 2>&1 & producer=$!
while kill -0 "$producer" 2>/dev/null; do printf '{"timestamp":"%s","raw_offset_total":%s,"postgres_events":%s}\n' "$(date -u +%FT%T.%3NZ)" "$(offsets transactions.raw | awk -F: '{sum += $NF} END {print sum+0}')" "$(pg_count)" >> "$OUT/polls.jsonl"; sleep "$POLL"; done
wait "$producer" || die "producer exited unsuccessfully"
previous_raw=-1; previous_pg=-1; stable=0; deadline=$(( $(date +%s) + TIMEOUT ))
while (( $(date +%s) < deadline )); do offsets transactions.raw > "$OUT/raw-drain.offsets"; now_raw="$(total "$OUT/raw-drain.offsets")"; now_pg="$(pg_count)"; printf '{"timestamp":"%s","raw_offset_total":%s,"postgres_events":%s}\n' "$(date -u +%FT%T.%3NZ)" "$now_raw" "$now_pg" >> "$OUT/polls.jsonl"; [[ "$now_raw" == "$previous_raw" && "$now_pg" == "$previous_pg" ]] && stable=$((stable+1)) || stable=0; previous_raw="$now_raw"; previous_pg="$now_pg"; ((stable>=3)) && break; sleep "$POLL"; done
end="$(date +%s%3N)"; ((stable>=3)) || die "pipeline did not reach the three-poll steady-state rule within ${TIMEOUT}s"
offsets transactions.raw > "$OUT/raw-after.offsets"; offsets transactions.deadletter > "$OUT/deadletter-after.offsets"; copy_raw transactions.raw "$OUT/raw-before.offsets" "$OUT/raw-after.offsets" "$OUT/raw-events.jsonl"; copy_raw transactions.deadletter "$OUT/deadletter-before.offsets" "$OUT/deadletter-after.offsets" "$OUT/deadletters.jsonl"; reconcile "$start" "$end"
python3 scripts/step12-validate-api.py "$API_URL" "$start" "$end" > "$OUT/api-validation.json"

log "Correctness run passed; resetting Kafka again before the isolated benchmark sweep."
bash scripts/step12-reset-kafka-backlog.sh
make flink-health
make flink-job-submit > "$OUT/backlog-flink-submit.log" 2>&1
SKIP_RECOVERY=true bash scripts/step11-benchmark.sh "1000 10000 50000" 120 "$OUT/backlog-isolation"
python3 scripts/step12-analyze.py "$OUT" "$OUT/backlog-isolation"
log "Evidence written to $OUT"
