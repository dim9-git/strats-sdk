from typing import Iterator

import pandas as pd

from strats_sdk.fetcher import get_cache_path, fetcher
from .base import CcxtParams, make_ccxt_exchange


def fetch_futures(params: CcxtParams) -> pd.DataFrame:
    if params.market_type is None:
        raise ValueError("Market type is required for futures data")
    ex = make_ccxt_exchange(params.exchange_id, market_type=params.market_type)
    ex.load_markets()
    since_ms = ex.parse8601(f"{params.start}T00:00:00Z")
    end_ms = ex.parse8601(f"{params.end}T00:00:00Z") if params.end else None
    tf_ms = int(ex.parse_timeframe(params.timeframe) * 1000)

    cache_path = get_cache_path(params.symbol, params.start, params.end, params.timeframe, params.exchange_id, prefix="oi",
                                   cache_dir=params.cache_dir)

    return fetcher(
        cache_path,
        since_ms=since_ms,
        tf_ms=tf_ms,
        paginate=lambda start_ms: paginate_futures(
            ex, params, since_ms=start_ms, end_ms=end_ms
        ),
        empty_error="No open-interest data fetched. Check symbol, timeframe, exchange support, and the ~30-day Binance window.",
    )

def paginate_futures(
    ex,
    params: CcxtParams,
    *,
    since_ms: int,
    end_ms: int | None,
) -> Iterator[pd.DataFrame]:
    tf_ms = int(ex.parse_timeframe(params.timeframe) * 1000)

    while end_ms is None or since_ms < end_ms:
        batch = ex.fetch_open_interest_history(
            params.symbol,
            timeframe=params.timeframe,
            since=since_ms,
            limit=params.limit,
            params={"until": end_ms} if end_ms else {},
        )
        if not batch:
            break

        rows = []
        for bar in batch:
            rows.append({
                "Date": bar["timestamp"],
                "open_interest": bar.get("openInterestAmount"),
                "open_interest_value": bar.get("openInterestValue"),
                "symbol": bar.get("symbol"),
            })
        df = pd.DataFrame(rows)
        df["Date"] = pd.to_datetime(df["Date"], unit="ms", utc=True)
        df[["open_interest", "open_interest_value"]] = df[
            ["open_interest", "open_interest_value"]
        ].astype(float)
        batch_df = df.set_index("Date")

        yield batch_df

        since_ms = int(batch_df.index.max().value // 1_000_000) + tf_ms
        if len(batch) < params.limit:
            break
