#!/usr/bin/env bash
set -euo pipefail

# Repeatable Step 8 experiment. This script deliberately does not claim success: it
# captures the evidence needed to decide whether the run was lossless and duplicate-free.
# Run from the repository root in Codespaces/Linux.

EVENTS_TARGET="${EVENTS_TARGET:-100000}"
RATE="${RATE:-1000}"
DURATION="${DURATION:-$((EVENTS_TARGET / RATE + 60))}"
KILL_AFTER="${KILL_AFTER:-20}"
PARTITIONS="${PARTITIONS:-6}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="${OUT_DIR:-step8-results/$RUN_ID}"
mkdir -p "$OUT_DIR"

log() { printf '[step8] %s\n' "$*"; }
die() { printf '[step8] FAIL: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null || die "docker is required"
command -v python3 >/dev/null || die "python3 is required"

offsets() {
  local topic="$1"
  docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list kafka:29092 --topic "$topic" --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum + 0}'
}

capture_partition_offsets() {
  local topic="$1" file="$2"
  docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list kafka:29092 --topic "$topic" --time -1 2>/dev/null \
    | sort -t: -nk2 > "$file"
}

read_offset() {
  local file="$1" partition="$2"
  awk -F: -v p="$partition" '$2 == p {print $3; found=1} END {if (!found) print 0}' "$file"
}

consume_range() {
  local topic="$1" before_file="$2" after_file="$3" output="$4"
  : > "$output"
  for partition in $(seq 0 $((PARTITIONS - 1))); do
    local start end count
    start="$(read_offset "$before_file" "$partition")"
    end="$(read_offset "$after_file" "$partition")"
    count=$((end - start))
    if (( count > 0 )); then
      docker compose exec -T kafka kafka-console-consumer \
        --bootstrap-server kafka:29092 --topic "$topic" --partition "$partition" \
        --offset "$start" --max-messages "$count" --timeout-ms 30000 2>/dev/null >> "$output" || true
    fi
  done
}

log "Output directory: $OUT_DIR"
log "Configured target: approximately $EVENTS_TARGET records at $RATE records/sec"
log "The producer's final sent counter is the authoritative produced count."

capture_partition_offsets transactions.raw "$OUT_DIR/raw-before.offsets"
capture_partition_offsets transactions.deadletter "$OUT_DIR/deadletter-before.offsets"
raw_before="$(offsets transactions.raw)"
deadletter_before="$(offsets transactions.deadletter)"
docker compose exec -T postgres sh -c \
  'psql -v ON_ERROR_STOP=1 -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM transactions.events"' \
  > "$OUT_DIR/events-before.count"

log "Submitting the existing Step 7 job"
if docker compose exec -T jobmanager flink list 2>/dev/null | grep -q "RUNNING"; then
  die "a Flink job is already RUNNING; cancel it before starting a controlled experiment"
fi
docker compose exec -T jobmanager flink run -d -py /opt/flink/usrlib/jobs/validation_dedup_job.py \
  > "$OUT_DIR/flink-submit.log" 2>&1 &
submit_pid=$!
sleep 10
docker compose exec -T jobmanager flink list > "$OUT_DIR/flink-before-producer.txt" 2>&1
grep -q "RUNNING" "$OUT_DIR/flink-before-producer.txt" || die "Flink job is not RUNNING before producer start"

log "Starting producer for $DURATION seconds"
python3 producers/transaction_generator/main.py \
  --bootstrap-servers localhost:9092 --topic transactions.raw \
  --rate "$RATE" --duration "$DURATION" --summary-interval 5 \
  > "$OUT_DIR/producer.log" 2>&1 &
producer_pid=$!

sleep "$KILL_AFTER"
docker compose ps taskmanager > "$OUT_DIR/taskmanager-before-kill.txt"
log "Forcibly killing TaskManager after ${KILL_AFTER}s (producer remains running)"
docker compose kill -s SIGKILL taskmanager
date -u +%FT%TZ > "$OUT_DIR/taskmanager-killed-at.txt"

wait "$producer_pid"
wait "$submit_pid" || true
log "Producer finished; waiting 45s for checkpoint recovery and sink drain"
sleep 45
docker compose exec -T jobmanager flink list > "$OUT_DIR/flink-after-recovery.txt" 2>&1 || true

capture_partition_offsets transactions.raw "$OUT_DIR/raw-after.offsets"
capture_partition_offsets transactions.deadletter "$OUT_DIR/deadletter-after.offsets"
raw_after="$(offsets transactions.raw)"
deadletter_after="$(offsets transactions.deadletter)"
docker compose exec -T postgres sh -c \
  'psql -v ON_ERROR_STOP=1 -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM transactions.events"' \
  > "$OUT_DIR/events-after.count"

consume_range transactions.raw "$OUT_DIR/raw-before.offsets" "$OUT_DIR/raw-after.offsets" \
  "$OUT_DIR/raw-events.jsonl"
consume_range transactions.deadletter "$OUT_DIR/deadletter-before.offsets" \
  "$OUT_DIR/deadletter-after.offsets" "$OUT_DIR/deadletters.jsonl"

docker compose exec -T postgres sh -c \
  'psql -v ON_ERROR_STOP=1 -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT event_id FROM transactions.events ORDER BY event_id"' \
  > "$OUT_DIR/events.ids"
docker compose exec -T postgres sh -c \
  'psql -v ON_ERROR_STOP=1 -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM (SELECT event_id FROM transactions.events GROUP BY event_id HAVING count(*) > 1) duplicates"' \
  > "$OUT_DIR/duplicate-event-id-rows.count"
docker compose exec -T postgres sh -c \
  'psql -v ON_ERROR_STOP=1 -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM (SELECT user_id, window_start FROM transactions.window_aggregates GROUP BY user_id, window_start HAVING count(*) > 1) duplicates"' \
  > "$OUT_DIR/duplicate-window-key-rows.count"

python3 - "$OUT_DIR" <<'PY'
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
raw_ids = set()
deadletter_ids = set()

for line in (out / "raw-events.jsonl").read_text(errors="replace").splitlines():
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        continue
    if isinstance(event, dict) and isinstance(event.get("event_id"), str):
        raw_ids.add(event["event_id"])

for line in (out / "deadletters.jsonl").read_text(errors="replace").splitlines():
    try:
        envelope = json.loads(line)
        raw = envelope.get("raw_record", "")
        event = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        continue
    if isinstance(event, dict) and isinstance(event.get("event_id"), str):
        deadletter_ids.add(event["event_id"])

db_ids = {line.strip() for line in (out / "events.ids").read_text().splitlines() if line.strip()}
unaccounted = sorted(raw_ids - db_ids - deadletter_ids)
(out / "raw-unique.ids").write_text("\n".join(sorted(raw_ids)) + "\n")
(out / "unaccounted.ids").write_text("\n".join(unaccounted) + ("\n" if unaccounted else ""))
(out / "reconciliation.txt").write_text(
    f"raw_unique_ids={len(raw_ids)}\n"
    f"events_table_ids={len(db_ids)}\n"
    f"deadletter_ids={len(deadletter_ids)}\n"
    f"unaccounted_ids={len(unaccounted)}\n"
    "Note: raw duplicates collapse to one ID; compare producer sent and raw offset deltas too.\n"
)
PY

cat > "$OUT_DIR/report.txt" <<EOF
Step 8 kill-test evidence
========================
run_id=$RUN_ID
configured_target=$EVENTS_TARGET
configured_rate=$RATE
duration_seconds=$DURATION
taskmanager_kill_after_seconds=$KILL_AFTER
raw_records_delta=$((raw_after - raw_before))
deadletter_records_delta=$((deadletter_after - deadletter_before))
events_table_before=$(cat "$OUT_DIR/events-before.count")
events_table_after=$(cat "$OUT_DIR/events-after.count")
duplicate_event_id_rows=$(cat "$OUT_DIR/duplicate-event-id-rows.count")
duplicate_window_key_rows=$(cat "$OUT_DIR/duplicate-window-key-rows.count")

Producer final summary is in producer.log.
ID reconciliation is in reconciliation.txt.
Flink submission output is in flink-submit.log.
EOF

log "Evidence captured in $OUT_DIR"
cat "$OUT_DIR/report.txt"
cat "$OUT_DIR/reconciliation.txt"
log "Inspect TaskManager recovery and vertex attempts before concluding exactly-once."
