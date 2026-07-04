from pathlib import Path
from collections.abc import Callable, Iterator
import requests

import pandas as pd

from .dataframe import ensure_datetime_index

def fetcher(
    cache_path: Path,
    *,
    since_ms: int,
    tf_ms: int,
    paginate: Callable[[int], Iterator[pd.DataFrame]],
    empty_error: str,
) -> pd.DataFrame:
    cached = read_cache(cache_path)
    if cached is not None:
        return cached

    part_dir = cache_path.parent / ".inprogress" / cache_path.stem
    part_dir.mkdir(parents=True, exist_ok=True)

    part_paths = sorted(part_dir.glob("part_*.parquet"))
    next_part_id = 0

    if part_paths:
        last_df = ensure_datetime_index(pd.read_parquet(part_paths[-1]))
        since_ms = int(last_df.index.max().value // 1_000_000) + tf_ms
        next_part_id = len(part_paths)

    for batch_df in paginate(since_ms):
        batch_df = ensure_datetime_index(batch_df)
        part_path = part_dir / f"part_{next_part_id:05d}.parquet"
        batch_df.to_parquet(part_path, index=True)
        part_paths.append(part_path)
        next_part_id += 1

    if not part_paths:
        part_dir.rmdir()
        raise ValueError(empty_error)

    df = pd.concat(
        [ensure_datetime_index(pd.read_parquet(p)) for p in part_paths],
        axis=0,
    )
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df.to_parquet(cache_path, index=True)

    for p in part_paths:
        p.unlink(missing_ok=True)
    part_dir.rmdir()
    return df



def fetch_json_with_retries(url: str, retries: int = 3, timeout: int = 120) -> dict:
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
    raise RuntimeError(f"Failed to fetch JSON after {retries} attempts: {url}") from last_exc


def get_cache_path(
    symbol: str,
    start: str,
    end: str,
    timeframe: str,
    exchange_id: str | None = None,
    prefix: None | str = None,
    cache_dir: Path = Path('cache')) -> Path:

    cache_dir.mkdir(parents=True, exist_ok=True)

    symbol_cleaned = symbol.replace("/", "-").replace(":", "-")
    filename = f"{symbol_cleaned}_{timeframe}_{start}_{end}.parquet"
    filename = f"{exchange_id}_{filename}" if exchange_id else filename
    filename = f"{prefix}_{filename}" if prefix else filename
    return cache_dir / filename


def read_cache(cache_path: Path) -> pd.DataFrame | None:
    if not cache_path.exists():
        return None
    return ensure_datetime_index(pd.read_parquet(cache_path))