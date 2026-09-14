#!/usr/bin/env python3
"""Validate live API responses against the running Postgres container; no mocks."""
from __future__ import annotations

import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request


BASE = sys.argv[1].rstrip("/")
START_MS, END_MS = sys.argv[2:4]


def sql(query: str) -> list[str]:
    command = ["docker", "compose", "exec", "-T", "postgres", "sh", "-c", 'psql -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"', "sh", query]
    completed = subprocess.run(command, text=True, capture_output=True, check=True)
    return [line for line in completed.stdout.splitlines() if line]


def get(path: str, expected: int = 200) -> dict:
    request = urllib.request.Request(BASE + path)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = json.load(response)
            if response.status != expected:
                raise AssertionError(f"{path}: expected {expected}, received {response.status}")
            return body
    except urllib.error.HTTPError as exc:
        if exc.code == expected:
            return json.load(exc)
        raise AssertionError(f"{path}: expected {expected}, received {exc.code}") from exc


def paged(path: str, key) -> dict:
    first = get(path + ("&" if "?" in path else "?") + "limit=3")
    first_keys = [key(item) for item in first["items"]]
    cursor = first.get("next_cursor")
    second_keys: list[object] = []
    if cursor:
        second = get(path + ("&" if "?" in path else "?") + "limit=3&cursor=" + urllib.parse.quote(cursor, safe=""))
        second_keys = [key(item) for item in second["items"]]
        assert not set(first_keys) & set(second_keys), f"pagination overlap at {path}"
    return {"first_page_items": len(first_keys), "second_page_items": len(second_keys), "next_cursor_present": bool(cursor)}


def main() -> None:
    start, end = (str(int(START_MS) / 1000), str(int(END_MS) / 1000))
    user_rows = sql(f"select user_id from transactions.events where received_at between to_timestamp({start}) and to_timestamp({end}) order by received_at desc limit 1")
    anomaly_rows = sql(f"select user_id from transactions.anomalies where detected_at between to_timestamp({start}) and to_timestamp({end}) order by detected_at desc limit 1")
    if not user_rows:
        raise AssertionError("No current-run event is available for API validation.")
    if not anomaly_rows:
        raise AssertionError("No current-run anomaly is available for API validation.")
    user = urllib.parse.quote(user_rows[0], safe="")
    anomaly_user = urllib.parse.quote(anomaly_rows[0], safe="")
    get("/health")
    report = {
        "transactions": paged(f"/users/{user}/transactions", lambda item: item["event_id"]),
        "aggregates": paged(f"/users/{user}/aggregates", lambda item: (item["user_id"], item["window_start"])),
        "user_anomalies": paged(f"/users/{anomaly_user}/anomalies", lambda item: (item["detected_at"], item["anomaly_id"])),
        "global_anomalies": paged("/anomalies", lambda item: (item["detected_at"], item["anomaly_id"])),
    }
    events = get(f"/users/{user}/transactions?limit=3")["items"]
    sampled_ids: list[str] = []
    for event in events:
        db_row = sql(f"select event_id || '|' || user_id || '|' || amount from transactions.events where event_id = '{event['event_id']}'")
        assert db_row == [f"{event['event_id']}|{event['user_id']}|{event['amount']}"], "API transaction does not match Postgres"
        sampled_ids.append(event["event_id"])
    get(f"/users/{user}/transactions?cursor=not-base64", 400)
    get("/anomalies?anomaly_type=NOT_A_RULE", 400)
    report["content_match"] = {"event_ids": sampled_ids, "postgres_rows_matched": True}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
