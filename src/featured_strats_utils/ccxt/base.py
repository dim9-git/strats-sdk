from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import ccxt

@dataclass(frozen=True)
class CcxtParams:
    exchange_id: Literal["binance", 'kucoin', 'okx', 'bybit']
    symbol: str
    timeframe: str
    start: str
    end: str
    limit: int = 500
    market_type: Literal["future", "swap"] | None = None
    cache_dir: Path | None = None


def make_ccxt_exchange(
        exchange_id: Literal["binance", "kucoin", "okx", "bybit"],
        *,
        market_type: Literal["future", "swap"] | None = None,
) -> ccxt.Exchange:
    options = {"enableRateLimit": True}

    if market_type:
        options["options"] = {
            "defaultType": market_type,
            "fetchMarkets": ["linear", "inverse"],
        }
    else:
        options["options"] = {"fetchMarkets": ["spot"]}

    return getattr(ccxt, exchange_id)(options)