"""Pydantic response models matching the V002 Postgres schema."""

from __future__ import annotations

from datetime import datetime
from typing import Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: UUID
    user_id: str
    merchant_id: str
    amount: int
    currency: str
    transaction_type: str
    event_time: datetime
    ingest_time: datetime
    schema_version: int
    received_at: datetime


class WindowAggregateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: str
    window_start: datetime
    window_end: datetime
    transaction_count: int
    total_volume: int
    average_transaction_value: float
    updated_at: datetime


class AnomalyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    anomaly_id: int
    anomaly_type: Literal["ANOMALY_VELOCITY", "ANOMALY_AMOUNT"]
    event_id: UUID
    user_id: str
    transaction_count: int | None
    velocity_window_seconds: int | None
    velocity_threshold: int | None
    amount: int | None
    rolling_99th_percentile: float | None
    history_size: int | None
    detected_at: datetime


T = TypeVar("T")


class CursorPage(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None
