"""Read-only FastAPI analytics API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

import db
from config import load_settings
from cursors import CursorError, encode_cursor
from models import AnomalyOut, CursorPage, EventOut, WindowAggregateOut


ALLOWED_ANOMALY_TYPES = {"ANOMALY_VELOCITY", "ANOMALY_AMOUNT"}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = load_settings()
    app.state.settings = settings
    app.state.pool = await db.create_pool(settings)
    try:
        yield
    finally:
        await db.close_pool(app.state.pool)


app = FastAPI(title="Real-Time Analytics API", version="1.0.0", lifespan=lifespan)


@app.exception_handler(db.PoolExhaustedError)
async def pool_exhausted_handler(_: Request, exc: db.PoolExhaustedError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"error": "database pool exhausted", "detail": str(exc)},
    )


@app.exception_handler(asyncpg.PostgresError)
async def postgres_error_handler(_: Request, exc: asyncpg.PostgresError) -> JSONResponse:
    return JSONResponse(status_code=503, content={"error": "database unavailable", "detail": str(exc)})


def pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


def validate_anomaly_type(anomaly_type: str | None) -> str | None:
    if anomaly_type is not None and anomaly_type not in ALLOWED_ANOMALY_TYPES:
        raise HTTPException(status_code=400, detail="invalid anomaly_type")
    return anomaly_type


def validate_time(value: datetime | None, name: str) -> datetime | None:
    if value is not None and value.tzinfo is None:
        raise HTTPException(status_code=400, detail=f"{name} must include a timezone")
    return value


def page(items: list, limit: int, cursor_values: tuple | None) -> CursorPage:
    next_cursor = encode_cursor(*cursor_values) if len(items) == limit and cursor_values else None
    return CursorPage(items=items, next_cursor=next_cursor)


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    try:
        await db.check_connection(pool(request))
    except db.PoolExhaustedError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database health check failed: {exc}") from exc
    return {"status": "ok"}


@app.get("/users/{user_id}/transactions", response_model=CursorPage[EventOut])
async def user_transactions(
    user_id: str,
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> CursorPage[EventOut]:
    start_time = validate_time(start_time, "start_time")
    end_time = validate_time(end_time, "end_time")
    try:
        rows = await db.fetch_user_transactions(pool(request), user_id, limit, cursor, start_time, end_time)
    except CursorError as exc:
        raise HTTPException(status_code=400, detail=f"invalid cursor: {exc}") from exc
    items = [EventOut.model_validate(dict(row)) for row in rows]
    last = items[-1] if items else None
    values = (last.event_time, last.event_id) if last else None
    return page(items, limit, values)


@app.get("/users/{user_id}/aggregates", response_model=CursorPage[WindowAggregateOut])
async def user_aggregates(
    user_id: str,
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> CursorPage[WindowAggregateOut]:
    start_time = validate_time(start_time, "start_time")
    end_time = validate_time(end_time, "end_time")
    try:
        rows = await db.fetch_user_aggregates(pool(request), user_id, limit, cursor, start_time, end_time)
    except CursorError as exc:
        raise HTTPException(status_code=400, detail=f"invalid cursor: {exc}") from exc
    items = [WindowAggregateOut.model_validate(dict(row)) for row in rows]
    last = items[-1] if items else None
    values = (last.window_start,) if last else None
    return page(items, limit, values)


@app.get("/users/{user_id}/anomalies", response_model=CursorPage[AnomalyOut])
async def user_anomalies(
    user_id: str,
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    anomaly_type: str | None = None,
) -> CursorPage[AnomalyOut]:
    anomaly_type = validate_anomaly_type(anomaly_type)
    try:
        rows = await db.fetch_user_anomalies(pool(request), user_id, limit, cursor, anomaly_type)
    except CursorError as exc:
        raise HTTPException(status_code=400, detail=f"invalid cursor: {exc}") from exc
    items = [AnomalyOut.model_validate(dict(row)) for row in rows]
    last = items[-1] if items else None
    values = (last.detected_at, str(last.anomaly_id)) if last else None
    return page(items, limit, values)


@app.get("/anomalies", response_model=CursorPage[AnomalyOut])
async def anomalies(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    anomaly_type: str | None = None,
) -> CursorPage[AnomalyOut]:
    anomaly_type = validate_anomaly_type(anomaly_type)
    try:
        rows = await db.fetch_anomalies(pool(request), limit, cursor, anomaly_type)
    except CursorError as exc:
        raise HTTPException(status_code=400, detail=f"invalid cursor: {exc}") from exc
    items = [AnomalyOut.model_validate(dict(row)) for row in rows]
    last = items[-1] if items else None
    values = (last.detected_at, str(last.anomaly_id)) if last else None
    return page(items, limit, values)
