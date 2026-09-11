"""Opaque, URL-safe cursor encoding for keyset pagination."""

from __future__ import annotations

import base64
from datetime import datetime
from typing import Any
from uuid import UUID


class CursorError(ValueError):
    """Raised when a client supplies a malformed or type-invalid cursor."""


def encode_cursor(*values: Any) -> str:
    parts: list[str] = []
    for value in values:
        if isinstance(value, datetime):
            parts.append(value.isoformat())
        elif isinstance(value, UUID):
            parts.append(str(value))
        elif isinstance(value, str):
            parts.append(value)
        else:
            raise TypeError(f"unsupported cursor value type: {type(value).__name__}")
    raw = ",".join(parts).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str, expected_types: tuple[type, ...]) -> tuple[Any, ...]:
    if not cursor or len(cursor) > 512:
        raise CursorError("cursor is empty or too long")
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        decoded = base64.b64decode(padded.encode("ascii"), altchars=b"-_", validate=True)
        parts = decoded.decode("utf-8").split(",")
    except (ValueError, UnicodeError) as exc:
        raise CursorError("cursor is not valid base64") from exc

    if len(parts) != len(expected_types):
        raise CursorError("cursor has the wrong number of values")

    converted: list[Any] = []
    for raw, expected in zip(parts, expected_types):
        try:
            if expected is datetime:
                converted.append(datetime.fromisoformat(raw.replace("Z", "+00:00")))
            elif expected is UUID:
                converted.append(UUID(raw))
            elif expected is str:
                converted.append(raw)
            else:
                raise CursorError("unsupported cursor type")
        except (TypeError, ValueError) as exc:
            raise CursorError("cursor value has the wrong type") from exc
    return tuple(converted)
