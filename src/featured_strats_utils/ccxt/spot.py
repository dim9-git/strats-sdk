from typing import Iterator

import ccxt
import pandas as pd

from featured_strats_utils.fetcher import get_cache_path, fetcher
from .base import CcxtParams, make_ccxt_exchange

def fetch_spot(params: CcxtParams) -> pd.DataFrame:
    ex = make_ccxt_exchange(params.exchange_id, market_type=None)
    since_ms = ex.parse8601(f"{params.start}T00:00:00Z")
    end_ms = ex.parse8601(f"{params.end}T00:00:00Z") if params.end else None
    tf_ms = int(ex.parse_timeframe(params.timeframe) * 1000)

    cache_path = get_cache_path(params.symbol, params.start, params.end, params.timeframe, params.exchange_id,
                                   cache_dir=params.cache_dir)

    return fetcher(
        cache_path,
        since_ms=since_ms,
        tf_ms=tf_ms,
        paginate=lambda start_ms: paginate_spot(
            ex, params, since_ms=start_ms, end_ms=end_ms
        ),
        empty_error="No OHLCV data fetched. Check symbol, date range, and API availability.",
    )


def paginate_spot(
    ex: ccxt.Exchange,
    params: CcxtParams,
    *,
    since_ms: int,
    end_ms: int | None,
) -> Iterator[pd.DataFrame]:
    tf_ms = int(ex.parse_timeframe(params.timeframe) * 1000)

    while end_ms is None or since_ms < end_ms:
        batch = ex.fetch_ohlcv(
            params.symbol,
            timeframe=params.timeframe,
            since=since_ms,
            limit=params.limit,
        )
        if not batch:
            break

        ohlcv_cols = ["Open", "High", "Low", "Close", "Volume"]
        batch_df = pd.DataFrame(batch, columns=["Date", *ohlcv_cols])
        batch_df[ohlcv_cols] = batch_df[ohlcv_cols].astype(float)
        batch_df["Date"] = pd.to_datetime(batch_df["Date"], unit="ms", utc=True)
        batch_df = batch_df.set_index("Date")
        yield batch_df

        since_ms = int(batch_df.index.max().value // 1_000_000) + tf_ms
        if len(batch) < params.limit:
            break