#!/usr/bin/env python3
"""Create the shared raw/dead-letter/Postgres event-ID reconciliation artifact."""
from __future__ import annotations

import json
import pathlib
import sys


def event_ids_from_raw(path: pathlib.Path) -> set[str]:
    ids: set[str] = set()
    for line in path.read_text(errors="replace").splitlines():
        try:
            event_id = json.loads(line).get("event_id")
            if isinstance(event_id, str):
                ids.add(event_id)
        except (json.JSONDecodeError, AttributeError):
            continue
    return ids


def event_ids_from_deadletters(path: pathlib.Path) -> set[str]:
    ids: set[str] = set()
    for line in path.read_text(errors="replace").splitlines():
        try:
            event_id = json.loads(json.loads(line).get("raw_record", "")).get("event_id")
            if isinstance(event_id, str):
                ids.add(event_id)
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue
    return ids


def main() -> None:
    root = pathlib.Path(sys.argv[1])
    raw = event_ids_from_raw(root / "raw-events.jsonl")
    dead = event_ids_from_deadletters(root / "deadletters.jsonl")
    landed = {line.strip() for line in (root / "events.ids").read_text(errors="replace").splitlines() if line.strip()}
    duplicates = sum(1 for line in (root / "duplicate.ids").read_text(errors="replace").splitlines() if line.strip())
    unaccounted = sorted(raw - landed - dead)
    result = {"raw_unique_event_ids": len(raw), "landed_event_ids": len(landed), "deadletter_event_ids": len(dead), "unaccounted_event_ids": len(unaccounted), "unaccounted_ids": unaccounted, "duplicate_event_id_rows": duplicates}
    (root / "reconciliation.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
