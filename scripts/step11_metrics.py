"""Turn benchmark artifacts into one level JSON result without inventing values."""
from __future__ import annotations

import json
import pathlib
import re
import sys


def main() -> None:
    directory, rate, duration, start_ms, end_ms, steady = sys.argv[1:]
    root = pathlib.Path(directory)
    raw_ids: set[str] = set()
    for line in (root / "raw-events.jsonl").read_text(errors="replace").splitlines():
        try:
            event = json.loads(line)
            if isinstance(event.get("event_id"), str):
                raw_ids.add(event["event_id"])
        except (json.JSONDecodeError, AttributeError):
            continue
    landed = {line.strip() for line in (root / "events.ids").read_text().splitlines() if line.strip()}
    deadletter_ids: set[str] = set()
    for line in (root / "deadletters.jsonl").read_text(errors="replace").splitlines():
        try:
            envelope = json.loads(line)
            event = json.loads(envelope.get("raw_record", ""))
            if isinstance(event.get("event_id"), str):
                deadletter_ids.add(event["event_id"])
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue
    polls = [json.loads(line) for line in (root / "polls.jsonl").read_text().splitlines()]
    raw_before = json.loads((root / "metadata.json").read_text())["raw_before"]
    raw_after = polls[-1]["raw_offset_total"] if polls else raw_before
    producer = (root / "producer.log").read_text(errors="replace")
    sent = [int(value) for value in re.findall(r"sent=(\d+)", producer)]
    checkpoints = json.loads((root / "checkpoints.json").read_text()).get("history", [])
    try:
        latency = json.loads((root / "latency.json").read_text().strip() or "null")
    except json.JSONDecodeError:
        latency = None
    durations = []
    for checkpoint in checkpoints:
        trigger = checkpoint.get("trigger_timestamp")
        acknowledged = checkpoint.get("latest_ack_timestamp")
        if trigger is not None and acknowledged is not None and int(start_ms) <= int(trigger) <= int(end_ms):
            durations.append(int(acknowledged) - int(trigger))
    result = {
        "requested_rate": int(rate),
        "duration_seconds": int(duration),
        "producer_sent_last_summary": sent[-1] if sent else None,
        "raw_offset_delta": raw_after - raw_before,
        "actual_throughput_events_per_second": (raw_after - raw_before)
        / max((int(end_ms) - int(start_ms)) / 1000, 0.001),
        "steady_state": steady == "true",
        "raw_unique_event_ids": len(raw_ids),
        "landed_event_ids": len(landed),
        "deadletter_event_ids": len(deadletter_ids),
        "unaccounted_event_ids": len(raw_ids - landed - deadletter_ids),
        "unaccounted_ids": sorted(raw_ids - landed - deadletter_ids),
        "duplicate_event_id_rows": sum(1 for _ in (root / "duplicate.ids").read_text().splitlines()),
        "checkpoint_duration_ms": {
            "count": len(durations),
            "min": min(durations) if durations else None,
            "avg": sum(durations) / len(durations) if durations else None,
            "max": max(durations) if durations else None,
            "p95": sorted(durations)[max(0, int(len(durations) * .95) - 1)] if durations else None,
        },
        "latency_percentiles_ms_file": "latency.json",
        "latency_percentiles_ms": latency,
        "polls": polls,
        "artifacts": {"polls": "polls.jsonl", "checkpoints": "checkpoints.json"},
    }
    (root / f"level-{rate}.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
