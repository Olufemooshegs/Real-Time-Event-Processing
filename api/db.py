"""Asyncpg pool lifecycle and parameterized read queries."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from config import Settings
from cursors import CursorError, decode_cursor


class PoolExhaustedError(RuntimeError):
    """The bounded pool could not provide a connection quickly enough."""


POOL_ACQUIRE_TIMEOUT_SECONDS = 2.0


async def create_pool(settings: Settings) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
    )


async def close_pool(pool: asyncpg.Pool) -> None:
    await pool.close()


async def _fetch(pool: asyncpg.Pool, query: str, *args: Any) -> list[asyncpg.Record]:
    try:
        # Fail fast when the small development pool is exhausted. Silently queueing API
        # requests would hide backpressure and turn a database bottleneck into latency.
        async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT_SECONDS) as connection:
            return await connection.fetch(query, *args)
    except asyncio.TimeoutError as exc:
        raise PoolExhaustedError("database connection pool acquire timed out") from exc


async def _fetchrow(pool: asyncpg.Pool, query: str, *args: Any) -> asyncpg.Record:
    try:
        async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT_SECONDS) as connection:
            return await connection.fetchrow(query, *args)
    except asyncio.TimeoutError as exc:
        raise PoolExhaustedError("database connection pool acquire timed out") from exc


EVENTS_QUERY = """
    SELECT event_id, user_id, merchant_id, amount, currency, transaction_type,
           event_time, ingest_time, schema_version, received_at
    FROM transactions.events
    WHERE user_id = $1
      AND ($2::timestamptz IS NULL OR event_time >= $2)
      AND ($3::timestamptz IS NULL OR event_time <= $3)
      AND ($4::timestamptz IS NULL OR (event_time, event_id) < ($4, $5::uuid))
    ORDER BY event_time DESC, event_id DESC
    LIMIT $6
"""
AGGREGATES_QUERY = """
    SELECT user_id, window_start, window_end, transaction_count, total_volume,
           average_transaction_value, updated_at
    FROM transactions.window_aggregates
    WHERE user_id = $1
      AND ($2::timestamptz IS NULL OR window_start >= $2)
      AND ($3::timestamptz IS NULL OR window_start <= $3)
      AND ($4::timestamptz IS NULL OR window_start < $4)
    ORDER BY window_start DESC
    LIMIT $5
"""
USER_ANOMALIES_QUERY = """
    SELECT anomaly_id, anomaly_type, event_id, user_id, transaction_count,
           velocity_window_seconds, velocity_threshold, amount,
           rolling_99th_percentile, history_size, detected_at
    FROM transactions.anomalies
    WHERE user_id = $1
      AND ($2::text IS NULL OR anomaly_type = $2)
      AND ($3::timestamptz IS NULL OR (detected_at, anomaly_id) < ($3, $4::bigint))
    ORDER BY detected_at DESC, anomaly_id DESC
    LIMIT $5
"""
ANOMALIES_QUERY = """
    SELECT anomaly_id, anomaly_type, event_id, user_id, transaction_count,
           velocity_window_seconds, velocity_threshold, amount,
           rolling_99th_percentile, history_size, detected_at
    FROM transactions.anomalies
    WHERE ($1::text IS NULL OR anomaly_type = $1)
      AND ($2::timestamptz IS NULL OR (detected_at, anomaly_id) < ($2, $3::bigint))
    ORDER BY detected_at DESC, anomaly_id DESC
    LIMIT $4
"""


async def fetch_user_transactions(
    pool: asyncpg.Pool,
    user_id: str,
    limit: int,
    cursor: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
) -> list[asyncpg.Record]:
    cursor_time: datetime | None = None
    cursor_id: UUID | None = None
    if cursor:
        cursor_time, cursor_id = decode_cursor(cursor, (datetime, UUID))
    return await _fetch(pool, EVENTS_QUERY, user_id, start_time, end_time, cursor_time, cursor_id, limit)


async def fetch_user_aggregates(
    pool: asyncpg.Pool,
    user_id: str,
    limit: int,
    cursor: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
) -> list[asyncpg.Record]:
    cursor_time: datetime | None = None
    if cursor:
        (cursor_time,) = decode_cursor(cursor, (datetime,))
    return await _fetch(pool, AGGREGATES_QUERY, user_id, start_time, end_time, cursor_time, limit)


async def fetch_user_anomalies(
    pool: asyncpg.Pool,
    user_id: str,
    limit: int,
    cursor: str | None,
    anomaly_type: str | None,
) -> list[asyncpg.Record]:
    cursor_time: datetime | None = None
    cursor_id: int | None = None
    if cursor:
        cursor_time, cursor_id = decode_cursor(cursor, (datetime, str))
        try:
            cursor_id = int(cursor_id)
        except (TypeError, ValueError) as exc:
            raise CursorError("anomaly cursor id is not an integer") from exc
    return await _fetch(pool, USER_ANOMALIES_QUERY, user_id, anomaly_type, cursor_time, cursor_id, limit)


async def fetch_anomalies(
    pool: asyncpg.Pool,
    limit: int,
    cursor: str | None,
    anomaly_type: str | None,
) -> list[asyncpg.Record]:
    cursor_time: datetime | None = None
    cursor_id: int | None = None
    if cursor:
        cursor_time, cursor_id = decode_cursor(cursor, (datetime, str))
        try:
            cursor_id = int(cursor_id)
        except (TypeError, ValueError) as exc:
            raise CursorError("anomaly cursor id is not an integer") from exc
    return await _fetch(pool, ANOMALIES_QUERY, anomaly_type, cursor_time, cursor_id, limit)


async def check_connection(pool: asyncpg.Pool) -> None:
    await _fetchrow(pool, "SELECT 1")
