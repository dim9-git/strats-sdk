"""Shared types for market data snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DataRequest:
    symbol: str
    timeframe: str
    lookback_days: int = 70
    with_metrics: bool = True
    with_funding: bool = True
    market: str = "futures"  # "futures" | "spot"


@dataclass
class MarketSnapshot:
    """Immutable-ish bundle of fetched data for one evaluation cycle."""

    request: DataRequest
    fetched_at: datetime
    bars: pd.DataFrame
    funding: pd.DataFrame | None = None
    metrics: pd.DataFrame | None = None
    last_closed_bar: pd.Timestamp | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> str:
        if self.last_closed_bar is None:
            return ""
        return self.last_closed_bar.isoformat()
