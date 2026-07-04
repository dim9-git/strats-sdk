"""Strategy signal types and protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import pandas as pd

from .data import MarketSnapshot

@dataclass(frozen=True)
class Signal:
    strategy: str
    side: str  # "long" | "short"
    bar_time: pd.Timestamp
    symbol: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def dedup_key(self) -> str:
        return f"{self.strategy}:{self.side}:{self.bar_time.isoformat()}"


class Strategy(Protocol):
    name: str

    def evaluate(self, snapshot: MarketSnapshot) -> list[Signal]:
        """Return signals on the latest closed bar only."""
        ...


