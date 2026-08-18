"""
rotation_monitor.returns — absolute return calculations (§5).

All returns are computed on the dividend-adjusted close series cached in
SQLite by data_loader.py. Trading-day lookback windows (1D/3D/5D/10D/20D)
count actual cached rows, not calendar days, so weekends/holidays never
distort the window.

Every function returns `None` when there isn't enough history to compute a
value — callers are expected to render that as DATA_UNAVAILABLE (§30), never
estimate a missing number.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor import storage

TRADING_DAY_WINDOWS = [1, 3, 5, 10, 20, 60, 120, 252]


def load_price_df(symbol: str) -> pd.DataFrame:
    """
    Ascending-date DataFrame indexed by date (as pandas Timestamp) with
    columns: open, high, low, close, adj_close, volume.
    """
    rows = storage.get_price_history(symbol)
    if not rows:
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "adj_close", "volume"]
        )

    df = pd.DataFrame(
        rows, columns=["date", "open", "high", "low", "close", "adj_close", "volume"]
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(subset="date", keep="last").set_index("date").sort_index()
    return df


def return_over_n_days(df: pd.DataFrame, n: int) -> Optional[float]:
    """Simple return from n trading days ago to the latest close."""
    closes = df["adj_close"].dropna()
    if len(closes) < n + 1:
        return None
    latest = closes.iloc[-1]
    prior = closes.iloc[-1 - n]
    if prior == 0:
        return None
    return (latest / prior) - 1.0


def _period_to_date_return(df: pd.DataFrame, freq_start) -> Optional[float]:
    """
    Return from the first available close on/after `freq_start` through the
    latest close. Used for MTD/QTD/YTD.
    """
    closes = df["adj_close"].dropna()
    if closes.empty:
        return None

    latest = closes.iloc[-1]
    period_slice = closes[closes.index >= freq_start]
    if period_slice.empty:
        return None

    start_price = period_slice.iloc[0]
    if start_price == 0:
        return None

    return (latest / start_price) - 1.0


def mtd_return(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    latest_date = df["adj_close"].dropna().index.max()
    month_start = latest_date.replace(day=1)
    return _period_to_date_return(df, month_start)


def qtd_return(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    latest_date = df["adj_close"].dropna().index.max()
    quarter_start_month = 3 * ((latest_date.month - 1) // 3) + 1
    quarter_start = latest_date.replace(month=quarter_start_month, day=1)
    return _period_to_date_return(df, quarter_start)


def ytd_return(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    latest_date = df["adj_close"].dropna().index.max()
    year_start = latest_date.replace(month=1, day=1)
    return _period_to_date_return(df, year_start)


def compute_returns(symbol: str, df: Optional[pd.DataFrame] = None) -> Dict[str, Optional[float]]:
    """
    All absolute-return horizons for one symbol (§5): 1D/3D/5D/10D/20D plus
    MTD/QTD/YTD. Values are simple returns (0.01 == +1%), or None when
    there isn't enough cached history.
    """
    if df is None:
        df = load_price_df(symbol)

    result: Dict[str, Optional[float]] = {}
    for n in [1, 3, 5, 10, 20]:
        result[f"{n}D"] = return_over_n_days(df, n)

    result["MTD"] = mtd_return(df)
    result["QTD"] = qtd_return(df)
    result["YTD"] = ytd_return(df)

    return result


def latest_close_date(symbol: str, df: Optional[pd.DataFrame] = None) -> Optional[str]:
    if df is None:
        df = load_price_df(symbol)
    if df.empty:
        return None
    return df["adj_close"].dropna().index.max().strftime("%Y-%m-%d")
