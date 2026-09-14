#!/usr/bin/env python3
"""Analyze existing Step 8/11 evidence and compare fresh Step 11 levels."""
from __future__ import annotations

import json
import pathlib
import re
import sys


def read_json(path: pathlib.Path):
    return json.loads(path.read_text())


def analyze_recovery(step11_root: pathlib.Path, step8_doc: pathlib.Path) -> dict:
    recovery_files = sorted(step11_root.glob("recovery-*/recovery.json"))
    if not recovery_files:
        return {"status": "inconclusive", "reason": "No Step 11 recovery.json artifact was found."}
    # Step 11's written result identifies the 5,000 requested-rate run as its recovery run.
    artifact = next((path for path in recovery_files if path.parent.name == "recovery-5000"), recovery_files[0])
    recovery = read_json(artifact)
    polls_path = artifact.parent / "polls.jsonl"
    polls = [json.loads(line) for line in polls_path.read_text().splitlines()] if polls_path.exists() else []
    first_growth = next(
        (
            {"timestamp": current["timestamp"], "postgres_events": current["postgres_events"]}
            for previous, current in zip(polls, polls[1:])
            if current.get("postgres_events", 0) > previous.get("postgres_events", 0)
        ),
        None,
    )
    baseline_match = re.search(r"recovery(?:[_ -]time)?[^0-9]{0,40}(\d+(?:\.\d+)?)\s*(?:ms|seconds|s)\b", step8_doc.read_text(), re.I)
    result = {
        "step11_artifact": str(artifact),
        "step11_requested_rate": recovery.get("requested_rate"),
        "step11_kill_at_ms": recovery.get("kill_at_ms"),
        "step11_stabilized_at_ms": recovery.get("stabilized_at_ms"),
        "step11_recovery_time_ms": recovery.get("recovery_time_ms"),
        "definition": "The artifact measures SIGKILL to the end of the poll-to-stability drain, not merely a container restart.",
        "first_observed_postgres_growth": first_growth,
    }
    if baseline_match:
        result["step8_baseline_documented"] = baseline_match.group(0)
        result["comparison"] = "A documented Step 8 recovery-time value was found; review the units before comparing."
        result["status"] = "review_required"
    else:
        result["status"] = "inconclusive"
        result["comparison"] = "Step 8's checked-in procedure documents correctness results but no measured recovery duration, so degradation cannot be calculated without its original run artifact. The Step 11 artifact still records the full kill-to-stability duration and the first observed later growth poll."
    return result


def compare_levels(previous: pathlib.Path, fresh: pathlib.Path) -> dict:
    rates = (1000, 10000, 50000)
    rows = []
    for rate in rates:
        old = read_json(previous / f"level-{rate}" / f"level-{rate}.json")
        new_path = fresh / f"level-{rate}" / f"level-{rate}.json"
        new = read_json(new_path) if new_path.exists() else None
        rows.append({"requested_rate": rate, "prior": old, "fresh": new})
    if any(row["fresh"] is None for row in rows):
        conclusion = "Fresh sweep is incomplete; no breakdown comparison is possible."
    else:
        old_break = next((r["requested_rate"] for r in rows if not r["prior"].get("steady_state")), None)
        new_break = next((r["requested_rate"] for r in rows if not r["fresh"].get("steady_state")), None)
        if old_break == new_break:
            conclusion = "The first non-steady level is the same requested rate in both runs; inspect achieved rates before attributing cause."
        else:
            conclusion = "The first non-steady requested rate differs after the reset; accumulated backlog is a plausible contributing factor, not proof of sole causation."
    return {"prior_results_root": str(previous), "fresh_results_root": str(fresh), "levels": rows, "conclusion": conclusion}


def main() -> None:
    output, fresh = map(pathlib.Path, sys.argv[1:3])
    output.mkdir(parents=True, exist_ok=True)
    root = pathlib.Path("step11-results/full-run")
    (output / "recovery-analysis.json").write_text(json.dumps(analyze_recovery(root, pathlib.Path("docs/step8-exactly-once-kill-test.md")), indent=2) + "\n")
    (output / "backlog-isolation-comparison.json").write_text(json.dumps(compare_levels(root, fresh), indent=2) + "\n")


if __name__ == "__main__":
    main()
