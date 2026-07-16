"""Shared event schemas used by Kafka and RabbitMQ buses."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

TOPIC_BARS_FETCHED = "bars.fetched"
TOPIC_NOTIFICATIONS_SEND = "notifications.send"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


@dataclass(frozen=True)
class BarsFetchedEvent:
    """Feed event. Hourly strategies use ``bars``; daily use ``bars_daily``.

    Published every new closed 1h bar. ``new_daily_bar`` is true only when the
    UTC daily bar just advanced.
    """

    symbol: str
    timeframe: str
    market: str
    last_closed_bar: str
    fetched_at: str
    lookback_days: int
    bars: list[dict[str, Any]] = field(default_factory=list)
    bars_daily: list[dict[str, Any]] = field(default_factory=list)
    last_closed_daily_bar: str = ""
    new_daily_bar: bool = False
    with_metrics: bool = True
    with_funding: bool = True
    bar_count: int | None = None
    daily_bar_count: int | None = None
    event_id: str = field(default_factory=_new_id)
    event_type: str = "bars.fetched"

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> BarsFetchedEvent:
        data = json.loads(raw)
        return cls(
            symbol=data["symbol"],
            timeframe=data["timeframe"],
            market=data["market"],
            last_closed_bar=data["last_closed_bar"],
            fetched_at=data["fetched_at"],
            lookback_days=int(data["lookback_days"]),
            bars=list(data.get("bars") or []),
            bars_daily=list(data.get("bars_daily") or []),
            last_closed_daily_bar=data.get("last_closed_daily_bar", ""),
            new_daily_bar=bool(data.get("new_daily_bar", False)),
            with_metrics=bool(data.get("with_metrics", True)),
            with_funding=bool(data.get("with_funding", True)),
            bar_count=data.get("bar_count"),
            daily_bar_count=data.get("daily_bar_count"),
            event_id=data.get("event_id", _new_id()),
            event_type=data.get("event_type", "bars.fetched"),
        )


@dataclass(frozen=True)
class NotificationEvent:
    dedup_key: str
    strategy: str
    side: str
    symbol: str
    bar_time: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=_new_id)
    event_type: str = "notifications.send"
    created_at: str = field(default_factory=_utcnow_iso)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> NotificationEvent:
        data = json.loads(raw)
        return cls(
            dedup_key=data["dedup_key"],
            strategy=data["strategy"],
            side=data["side"],
            symbol=data["symbol"],
            bar_time=data["bar_time"],
            message=data["message"],
            metadata=dict(data.get("metadata") or {}),
            event_id=data.get("event_id", _new_id()),
            event_type=data.get("event_type", "notifications.send"),
            created_at=data.get("created_at", _utcnow_iso()),
        )

    @classmethod
    def from_signal(cls, signal: Any) -> NotificationEvent:
        bar_time = signal.bar_time
        bar_iso = bar_time.isoformat() if hasattr(bar_time, "isoformat") else str(bar_time)
        dedup = getattr(signal, "dedup_key", None) or f"{signal.strategy}:{signal.side}:{bar_iso}"
        return cls(
            dedup_key=dedup,
            strategy=signal.strategy,
            side=signal.side,
            symbol=signal.symbol,
            bar_time=bar_iso,
            message=signal.message,
            metadata=dict(getattr(signal, "metadata", None) or {}),
        )
