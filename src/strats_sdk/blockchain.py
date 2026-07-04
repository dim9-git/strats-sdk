from pathlib import Path
import pandas as pd
from .fetcher import fetch_json_with_retries


DATA_DAILY_BASE_URL = "https://raw.githubusercontent.com/ErcinDedeoglu/crypto-market-data/main/data/daily"

INDICATOR_URLS = {
    "net_flow": "btc_exchange_netflow.json",
    "exchange_reserve": "btc_exchange_reserve.json",
    "mvrv_ratio": "btc_mvrv_ratio.json",
    "funding_rates": "btc_funding_rates.json",
    "open_interest": "btc_open_interest.json",
    "coinbase_premium_index": "btc_coinbase_premium_index.json",
}


def load_daily_json_data(url: str, column_name: str) -> pd.DataFrame:
    cache_dir = Path("data/daily")
    cache_dir.mkdir(parents=True, exist_ok=True)

    dataset_name = url.split("/")[-1].split(".")[0]
    file_source = cache_dir / f"{dataset_name}.csv"

    if file_source.exists():
        out = pd.read_csv(file_source, parse_dates=["Date"], index_col="Date")
        if out.index.tz is None:
            out.index = out.index.tz_localize("UTC")
        out.index = out.index.as_unit("ms")
        return out.sort_index()

    payload = fetch_json_with_retries(url)

    raw = pd.DataFrame(payload["data"])
    out = clean_daily_data(raw, column_name)

    first_date = out.index[0].strftime("%Y-%m-%d")
    last_date = out.index[-1].strftime("%Y-%m-%d")

    out.to_csv(cache_dir / f"{dataset_name}.csv_{first_date}__{last_date}.csv")
    out.to_csv(file_source)

    return out

def clean_daily_data(raw: pd.DataFrame, column_name: str) -> pd.DataFrame:
    out = raw[["timestamp", "value"]].copy()
    out.columns = ["Date", column_name]

    out["Date"] = pd.to_datetime(out["Date"], unit="ms", utc=True)
    out[column_name] = pd.to_numeric(out[column_name], errors="coerce")

    return out.dropna().set_index("Date").sort_index()


def load_all_indicators(start: str | None = None, end: str | None = None) -> pd.DataFrame:
    frames = []
    for column_name, url in INDICATOR_URLS.items():
        frame = load_daily_json_data(DATA_DAILY_BASE_URL + "/" + url, column_name)
        frames.append(frame.loc[start:end] if (start and end) else frame)
    return pd.concat(frames, axis=1).sort_index()