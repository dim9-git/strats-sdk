import pandas as pd
import numpy as np

def ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        return df.reset_index().rename(columns={"index": "Date"}).set_index("Date")
    return df


def color_return(val):
    if pd.isna(val):
        return ""
    if val > 0:
        return "color: green; font-weight: 600"
    if val < 0:
        return "color: red; font-weight: 600"
    return "color: gray"


def sparse_cooldown(mask: pd.Series, cooldown: int) -> pd.Series:
    pos = np.flatnonzero(mask.fillna(False).to_numpy())
    out = np.zeros(len(mask), dtype=bool)
    last_kept = -10**9
    for p in pos:
        if p - last_kept >= cooldown:
            out[p] = True
            last_kept = p
    return pd.Series(out, index=mask.index)