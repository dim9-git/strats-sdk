"""Binance USDT-M futures klines and funding rates."""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass

import ccxt
import pandas as pd
import requests

from featured_strats_utils.fetcher import fetcher, get_cache_path

FUTURES_KLINES = "https://fapi.binance.com/fapi/v1/klines"
FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"

KLINES_LIMIT = 1500
FUNDING_LIMIT = 1000
FUNDING_TF_MS = 8 * 3_600_000

_KLINE_COLS = [
    "open_time", "Open", "High", "Low", "Close", "Volume", "close_time",
    "quote_vol", "trades", "taker_buy_base", "taker_buy_quote", "ignore",
]
_FLOAT_COLS = [
    "Open", "High", "Low", "Close", "Volume",
    "quote_vol", "taker_buy_base", "taker_buy_quote",
]


@dataclass(frozen=True)
class FuturesKlinesParams:
    symbol: str   # raw Binance symbol, e.g. BTCUSDT
    interval: str  # e.g. 1h, 4h, 1d
    start: str    # inclusive, UTC
    end: str      # exclusive, UTC


@dataclass(frozen=True)
class FundingRatesParams:
    symbol: str
    start: str
    end: str


def fetch_futures_klines(params: FuturesKlinesParams) -> pd.DataFrame:
    tf_ms = int(ccxt.binance().parse_timeframe(params.interval) * 1000)
    since_ms = int(pd.Timestamp(params.start, tz="UTC").timestamp() * 1000)
    end_ms = int(pd.Timestamp(params.end, tz="UTC").timestamp() * 1000)

    cache_path = get_cache_path(
        params.symbol,
        params.start,
        params.end,
        timeframe=params.interval,
        prefix="binance_futures_klines",
    )

    return fetcher(
        cache_path,
        since_ms=since_ms,
        tf_ms=tf_ms,
        paginate=lambda start_ms: paginate_klines(
            params, since_ms=start_ms, end_ms=end_ms
        ),
        empty_error=(
            f"No klines found for {params.symbol} "
            f"between {params.start} and {params.end}."
        ),
    )


def paginate_klines(
    params: FuturesKlinesParams,
    *,
    since_ms: int,
    end_ms: int,
) -> Iterator[pd.DataFrame]:
    session = requests.Session()
    session.headers.update({"User-Agent": "featured-strats-utils/1.0"})

    while since_ms < end_ms:
        url = (
            f"{FUTURES_KLINES}?symbol={params.symbol}&interval={params.interval}"
            f"&startTime={since_ms}&limit={KLINES_LIMIT}"
        )
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        batch_df = _parse_klines_batch(data)
        batch_df = batch_df[batch_df.index < pd.Timestamp(end_ms, unit="ms", tz="UTC")]
        if not batch_df.empty:
            yield batch_df

        since_ms = data[-1][0] + 1
        if len(data) < KLINES_LIMIT:
            break
        time.sleep(0.15)


def _parse_klines_batch(data: list) -> pd.DataFrame:
    df = pd.DataFrame(data, columns=_KLINE_COLS)
    df[_FLOAT_COLS] = df[_FLOAT_COLS].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    return df.set_index("open_time").sort_index()


def fetch_funding_rates(params: FundingRatesParams) -> pd.DataFrame:
    since_ms = int(pd.Timestamp(params.start, tz="UTC").timestamp() * 1000)
    end_ms = int(pd.Timestamp(params.end, tz="UTC").timestamp() * 1000)

    cache_path = get_cache_path(
        params.symbol,
        params.start,
        params.end,
        timeframe="8h",
        prefix="binance_funding_rates",
    )

    try:
        return fetcher(
            cache_path,
            since_ms=since_ms,
            tf_ms=FUNDING_TF_MS,
            paginate=lambda start_ms: paginate_funding_rates(
                params, since_ms=start_ms, end_ms=end_ms
            ),
            empty_error=(
                f"No funding rates found for {params.symbol} "
                f"between {params.start} and {params.end}."
            ),
        )
    except ValueError:
        return pd.DataFrame(columns=["funding_rate"]).astype(float)


def paginate_funding_rates(
    params: FundingRatesParams,
    *,
    since_ms: int,
    end_ms: int,
) -> Iterator[pd.DataFrame]:
    session = requests.Session()
    session.headers.update({"User-Agent": "featured-strats-utils/1.0"})
    end_ts = pd.Timestamp(end_ms, unit="ms", tz="UTC")

    while since_ms < end_ms:
        url = f"{FUNDING_URL}?symbol={params.symbol}&startTime={since_ms}&limit={FUNDING_LIMIT}"
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        df = pd.DataFrame(data)
        df["funding_time"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
        df["funding_rate"] = df["fundingRate"].astype(float)
        batch_df = df.set_index("funding_time")[["funding_rate"]].sort_index()
        batch_df = batch_df[batch_df.index < end_ts]
        if not batch_df.empty:
            yield batch_df

        since_ms = data[-1]["fundingTime"] + 1
        if len(data) < FUNDING_LIMIT:
            break
        time.sleep(0.1)
