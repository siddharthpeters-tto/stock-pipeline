"""
rotation_monitor.trend — trend descriptors (§8). Purely descriptive: these
feed persistence/regime scoring and the dashboard, and are never turned into
buy/sell signals.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor.returns import load_price_df, return_over_n_days
from rotation_monitor.scoring_config import TREND_THRESHOLDS


def moving_average(df: pd.DataFrame, window: int) -> pd.Series:
    return df["adj_close"].dropna().rolling(window).mean()


def _last_or_none(series: pd.Series) -> Optional[float]:
    series = series.dropna()
    return float(series.iloc[-1]) if not series.empty else None


def distance_from_ma(df: pd.DataFrame, window: int) -> Optional[float]:
    """(close - MA) / MA, as a fraction."""
    ma = moving_average(df, window)
    closes = df["adj_close"].dropna()
    if ma.empty or closes.empty:
        return None
    last_ma = ma.iloc[-1]
    if pd.isna(last_ma) or last_ma == 0:
        return None
    return float((closes.iloc[-1] - last_ma) / last_ma)


def ma_slope(df: pd.DataFrame, window: int, slope_days: int = 5) -> Optional[float]:
    """% change of the MA itself over the last `slope_days` trading days."""
    ma = moving_average(df, window).dropna()
    if len(ma) < slope_days + 1:
        return None
    prior = ma.iloc[-1 - slope_days]
    if prior == 0:
        return None
    return float((ma.iloc[-1] / prior) - 1.0)


def classify_trend(dist_ma20: Optional[float], slope_ma20: Optional[float]) -> str:
    """
    STRONG_UPTREND / UPTREND / FLAT / DOWNTREND / STRONG_DOWNTREND, based on
    distance from the 20D MA and the 20D MA's own slope. Thresholds are in
    scoring_config.TREND_THRESHOLDS.
    """
    if dist_ma20 is None or slope_ma20 is None:
        return "DATA_UNAVAILABLE"

    t = TREND_THRESHOLDS
    if abs(dist_ma20) <= t["flat_distance"] and abs(slope_ma20) <= t["flat_slope"]:
        return "FLAT"

    if dist_ma20 >= t["strong_distance"] and slope_ma20 > 0:
        return "STRONG_UPTREND"
    if dist_ma20 <= -t["strong_distance"] and slope_ma20 < 0:
        return "STRONG_DOWNTREND"
    if dist_ma20 > 0 and slope_ma20 >= 0:
        return "UPTREND"
    if dist_ma20 < 0 and slope_ma20 <= 0:
        return "DOWNTREND"
    return "FLAT"


def compute_trend(symbol: str, df: Optional[pd.DataFrame] = None) -> Dict:
    if df is None:
        df = load_price_df(symbol)

    dist20 = distance_from_ma(df, 20)
    dist50 = distance_from_ma(df, 50)
    slope20 = ma_slope(df, 20, slope_days=5)
    slope50 = ma_slope(df, 50, slope_days=10)

    return {
        "distance_from_ma20": dist20,
        "distance_from_ma50": dist50,
        "ma20_slope": slope20,
        "ma50_slope": slope50,
        "momentum_5d": return_over_n_days(df, 5),
        "momentum_10d": return_over_n_days(df, 10),
        "momentum_20d": return_over_n_days(df, 20),
        "trend_state": classify_trend(dist20, slope20),
        "above_ma20": None if dist20 is None else dist20 > 0,
        "above_ma50": None if dist50 is None else dist50 > 0,
    }
