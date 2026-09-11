"""Environment-driven settings for the read-only analytics API."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


@dataclass(frozen=True)
class Settings:
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10

def load_settings() -> Settings:
    required = ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(f"missing required database environment variables: {', '.join(missing)}")

    min_size = _int_env("DB_POOL_MIN_SIZE", 2)
    max_size = _int_env("DB_POOL_MAX_SIZE", 10)
    if min_size > max_size:
        raise ValueError("DB_POOL_MIN_SIZE cannot exceed DB_POOL_MAX_SIZE")

    return Settings(
        postgres_db=os.environ["POSTGRES_DB"],
        postgres_user=os.environ["POSTGRES_USER"],
        postgres_password=os.environ["POSTGRES_PASSWORD"],
        postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
        postgres_port=_int_env("POSTGRES_PORT", 5432),
        db_pool_min_size=min_size,
        db_pool_max_size=max_size,
    )
