#!/usr/bin/env bash
set -euo pipefail

# Step 11 evidence harness. Requested rates are never substituted for measured rates.
RATES="${1:-1000 5000 10000 50000 100000}"
DURATION="${2:-120}"
OUT="${3:-step11-results/$(date -u +%Y%m%dT%H%M%SZ)}"
POLL=5
TIMEOUT=180
GROUP="flink-validation-dedup-v1"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
mkdir -p "$OUT"
log() { printf '[step11] %s\n' "$*"; }
warn() { printf '[step11] WARNING: %s\n' "$*" >&2; }
die() { printf '[step11] FAIL: %s\n' "$*" >&2; exit 1; }
command -v docker >/dev/null || die "docker is required"
command -v curl >/dev/null || die "curl is required"
command -v python3 >/dev/null || die "python3 is required"

offsets() { docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic transactions.raw; }
total() { awk -F: '{sum += $NF} END {print sum+0}' "$1"; }
pg_count() { docker compose exec -T postgres sh -c 'psql -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select count(*) from transactions.events"' | tr -d '[:space:]'; }
lag() { docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "$GROUP" 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}' || true; }
job_id() { curl --fail --silent "$FLINK_URL/jobs/overview" | python3 -c 'import json,sys; print(next((j["jid"] for j in json.load(sys.stdin).get("jobs",[]) if j.get("state")=="RUNNING"),""))'; }

discover() {
  python3 - "$FLINK_URL" "$1" "$2" <<'PY'
import json,sys,urllib.request
base,jid,out=sys.argv[1:]
def get(path):
    with urllib.request.urlopen(base+path,timeout=10) as r: return json.load(r)
found=[]
for v in get(f'/jobs/{jid}').get('vertices',[]):
    metrics=get(f"/jobs/{jid}/vertices/{v['id']}/metrics")
    ids=[m['id'] for m in metrics if any(x in m['id'].lower() for x in ('lag','pending','backlog','emit'))]
    found.append({'vertex_id':v['id'],'name':v.get('name'),'metric_ids':ids})
open(out,'w').write(json.dumps(found,indent=2))
PY
}

metric_snapshot() {
  python3 - "$FLINK_URL" "$1" "$2" "$3" <<'PY'
import json,sys,urllib.parse,urllib.request
base,jid,discovery,out=sys.argv[1:]; result=[]
for v in json.load(open(discovery)):
  for metric in v['metric_ids']:
    try:
      q=urllib.parse.urlencode({'get':metric})
      with urllib.request.urlopen(f"{base}/jobs/{jid}/vertices/{v['vertex_id']}/metrics?{q}",timeout=10) as r: result.extend(json.load(r))
    except Exception as exc: result.append({'id':metric,'error':str(exc)})
open(out,'w').write(json.dumps(result))
PY
}

poll() {
  local dir="$1" jid="$2" discovery="$3" phase="$4"; local file="$dir/raw-$phase.offsets"
  offsets > "$file"; metric_snapshot "$jid" "$discovery" "$dir/metrics-$phase-$(date +%s).json"
  printf '{"timestamp":"%s","phase":"%s","raw_offset_total":%s,"postgres_events":%s,"consumer_group_lag":%s}\n' \
    "$(date -u +%FT%T.%3NZ)" "$phase" "$(total "$file")" "$(pg_count)" "$(lag)" >> "$dir/polls.jsonl"
}

copy_raw() {
  local topic="$1" before="$2" after="$3" output="$4"; : > "$output"
  for p in $(seq 0 5); do
    start=$(awk -F: -v p="$p" '$2==p{print $NF}' "$before"); end=$(awk -F: -v p="$p" '$2==p{print $NF}' "$after"); start=${start:-0}; end=${end:-0}; count=$((end-start)); ((count>0)) || continue
    docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic "$topic" --partition "$p" --offset "$start" --max-messages "$count" --timeout-ms 30000 2>/dev/null >> "$output" || true
  done
}

reconcile() {
  local dir="$1"
  docker compose exec -T postgres sh -c 'psql -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select event_id from transactions.events"' > "$dir/events.ids"
  docker compose exec -T postgres sh -c 'psql -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select event_id from transactions.events group by event_id having count(*)>1"' > "$dir/duplicate.ids"
  : > "$dir/deadletters.jsonl"
  python3 - "$dir" <<'PY'
import json,pathlib,sys
d=pathlib.Path(sys.argv[1]); raw=set(); dead=set(); db={x.strip() for x in (d/'events.ids').read_text().splitlines() if x.strip()}
for line in (d/'raw-events.jsonl').read_text(errors='replace').splitlines():
  try: raw.add(json.loads(line)['event_id'])
  except (ValueError,KeyError,TypeError): pass
for line in (d/'deadletters.jsonl').read_text(errors='replace').splitlines():
  try:
    event=json.loads(json.loads(line).get('raw_record',''))
    if isinstance(event.get('event_id'),str): dead.add(event['event_id'])
  except (ValueError,KeyError,TypeError): pass
(d/'reconciliation.json').write_text(json.dumps({'raw_unique_event_ids':len(raw),'landed_event_ids':len(db),'deadletter_event_ids':len(dead),'unaccounted_event_ids':len(raw-db-dead),'unaccounted_ids':sorted(raw-db-dead),'duplicate_event_id_rows':sum(1 for _ in (d/'duplicate.ids').read_text().splitlines())},indent=2)+'\n')
PY
}

capture_environment() {
  { echo nproc:; nproc; echo free_h:; free -h; echo docker_stats:; docker stats --no-stream; echo codespaces_env:; env | grep -E '^(CODESPACES|CODESPACE_NAME|GITHUB_|RUNNER_)' || true; } > "$OUT/environment.txt"
  docker compose exec -T jobmanager cat /opt/flink/conf/config.yaml > "$OUT/flink-config.yaml"
  python3 - "$OUT" <<'PY'
import json,os,sys
json.dump({'codespaces_environment':{k:v for k,v in os.environ.items() if k in ('CODESPACES','CODESPACE_NAME')},'machine_type_note':'Fill manually from Codespace settings if metadata is absent.','environment_text':'environment.txt','flink_config':'flink-config.yaml'},open(sys.argv[1]+'/environment.json','w'),indent=2)
PY
}

capture_environment
offsets > "$OUT/kafka-offset-tool-check.offsets" || die "kafka-get-offsets is unavailable"
best=""
for rate in $RATES; do
  dir="$OUT/level-$rate"; mkdir -p "$dir"; jid="$(job_id)"; test -n "$jid" || die "no RUNNING Flink job"
  discover "$jid" "$dir/metrics-discovery.json"; docker compose ps > "$dir/compose-before.txt"; printf '%s\n' "$(pg_count)" > "$dir/postgres-before.count"; offsets > "$dir/raw-before.offsets"; docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic transactions.deadletter > "$dir/deadletter-before.offsets"; start="$(date +%s%3N)"
  printf '{"requested_rate":%s,"duration_seconds":%s,"raw_before":%s}\n' "$rate" "$DURATION" "$(total "$dir/raw-before.offsets")" > "$dir/metadata.json"
  python3 producers/transaction_generator/main.py --bootstrap-servers localhost:9092 --topic transactions.raw --rate "$rate" --duration "$DURATION" --summary-interval "$POLL" > "$dir/producer.log" 2>&1 & producer=$!
  while kill -0 "$producer" 2>/dev/null; do poll "$dir" "$jid" "$dir/metrics-discovery.json" live; sleep "$POLL"; done
  wait "$producer" || true; end="$(date +%s%3N)"; previous_raw=-1; previous_pg=-1; stable=0; deadline=$(( $(date +%s)+TIMEOUT ))
  while (( $(date +%s)<deadline )); do
    poll "$dir" "$jid" "$dir/metrics-discovery.json" drain; now_raw="$(total "$dir/raw-drain.offsets")"; now_pg="$(pg_count)"
    if [[ "$now_raw" == "$previous_raw" && "$now_pg" == "$previous_pg" ]]; then stable=$((stable+1)); else stable=0; fi
    previous_raw="$now_raw"; previous_pg="$now_pg"; if (( stable >= 3 )); then break; fi; sleep "$POLL"
  done
  steady=false; ((stable>=3)) && steady=true; $steady && best="$rate" || warn "rate $rate did not stabilize before timeout"
  offsets > "$dir/raw-after.offsets"; docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic transactions.deadletter > "$dir/deadletter-after.offsets"; copy_raw transactions.raw "$dir/raw-before.offsets" "$dir/raw-after.offsets" "$dir/raw-events.jsonl"; copy_raw transactions.deadletter "$dir/deadletter-before.offsets" "$dir/deadletter-after.offsets" "$dir/deadletters.jsonl"; reconcile "$dir"
  curl --fail --silent "$FLINK_URL/jobs/$jid/checkpoints" > "$dir/checkpoints.json"
  docker compose exec -T postgres sh -c "psql -At -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c \"SELECT json_build_object('event_to_received_ms', json_build_object('p50', percentile_cont(0.50) within group (order by extract(epoch from (received_at-event_time))*1000), 'p95', percentile_cont(0.95) within group (order by extract(epoch from (received_at-event_time))*1000), 'p99', percentile_cont(0.99) within group (order by extract(epoch from (received_at-event_time))*1000)), 'ingest_to_received_ms', json_build_object('p50', percentile_cont(0.50) within group (order by extract(epoch from (received_at-ingest_time))*1000), 'p95', percentile_cont(0.95) within group (order by extract(epoch from (received_at-ingest_time))*1000), 'p99', percentile_cont(0.99) within group (order by extract(epoch from (received_at-ingest_time))*1000))) FROM transactions.events WHERE event_time BETWEEN to_timestamp($start/1000.0) AND to_timestamp($end/1000.0)\"" > "$dir/latency.json"
  python3 scripts/step11_metrics.py "$dir" "$rate" "$DURATION" "$start" "$end" "$steady"
done

if [[ -n "$best" ]]; then
  log "Highest stabilized requested rate: $best"
  dir="$OUT/recovery-$best"; mkdir -p "$dir"; jid="$(job_id)"; discover "$jid" "$dir/metrics-discovery.json"; offsets > "$dir/raw-before.offsets"; docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic transactions.deadletter > "$dir/deadletter-before.offsets"; start="$(date +%s%3N)"
  python3 producers/transaction_generator/main.py --bootstrap-servers localhost:9092 --topic transactions.raw --rate "$best" --duration "$DURATION" > "$dir/producer.log" 2>&1 & producer=$!
  sleep 20; kill_at="$(date +%s%3N)"; docker compose kill -s SIGKILL taskmanager; docker compose up -d taskmanager; wait "$producer" 2>/dev/null || true
  previous=-1; stable=0; deadline=$(( $(date +%s)+TIMEOUT ))
  while (( $(date +%s)<deadline )); do poll "$dir" "$jid" "$dir/metrics-discovery.json" recovery; now="$(pg_count)"; [[ "$now" == "$previous" ]] && stable=$((stable+1)) || stable=0; previous="$now"; if (( stable >= 3 )); then break; fi; sleep "$POLL"; done
  offsets > "$dir/raw-after.offsets"; docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic transactions.deadletter > "$dir/deadletter-after.offsets"; copy_raw transactions.raw "$dir/raw-before.offsets" "$dir/raw-after.offsets" "$dir/raw-events.jsonl"; copy_raw transactions.deadletter "$dir/deadletter-before.offsets" "$dir/deadletter-after.offsets" "$dir/deadletters.jsonl"; reconcile "$dir"
  recovery_at="$(date +%s%3N)"; printf '{"requested_rate":%s,"steady_state":%s,"poll_count":%s,"kill_at_ms":%s,"stabilized_at_ms":%s,"recovery_time_ms":%s}\n' "$best" "$((stable>=3))" "$(wc -l < "$dir/polls.jsonl")" "$kill_at" "$recovery_at" "$((recovery_at-kill_at))" > "$dir/recovery.json"
else
  warn "No requested level stabilized; recovery scenario was not run."
fi
