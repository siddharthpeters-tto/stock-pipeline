"""
rotation_monitor.correlations — rolling return correlation engine (§10).

Tracks whether pairs of themes are moving together, decoupling, or
reverting toward their historical norm. Historical "normal" correlation is
always computed from the data itself (trailing 1-year percentile of the
rolling correlation series) — never hard-coded, per the spec's explicit
instruction not to assume e.g. SOXX/IGV "should" run around +0.70.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor import config
from rotation_monitor.returns import load_price_df

ROLLING_WINDOWS = [20, 60]
PERCENTILE_LOOKBACK_DAYS = 252  # trailing ~1 year of trading days


def _daily_returns(df: pd.DataFrame) -> pd.Series:
    return df["adj_close"].dropna().pct_change().dropna()


def rolling_correlation_series(
    df_a: pd.DataFrame, df_b: pd.DataFrame, window: int
) -> pd.Series:
    ret_a = _daily_returns(df_a)
    ret_b = _daily_returns(df_b)

    joined = pd.concat([ret_a, ret_b], axis=1, join="inner", keys=["a", "b"])
    if len(joined) < window:
        return pd.Series(dtype=float)

    return joined["a"].rolling(window).corr(joined["b"]).dropna()


def _percentile_rank(series: pd.Series, value: float) -> Optional[float]:
    """% of trailing-window observations at or below `value`, 0-100."""
    if series.empty:
        return None
    trailing = series.tail(PERCENTILE_LOOKBACK_DAYS)
    if len(trailing) < 20:  # not enough history for a meaningful percentile
        return None
    return round(float((trailing <= value).mean() * 100.0), 1)


def correlation_summary(
    symbol_a: str, symbol_b: str,
    df_a: Optional[pd.DataFrame] = None, df_b: Optional[pd.DataFrame] = None,
) -> Dict:
    if df_a is None:
        df_a = load_price_df(symbol_a)
    if df_b is None:
        df_b = load_price_df(symbol_b)

    out: Dict = {"pair": f"{symbol_a}_{symbol_b}"}

    for window in ROLLING_WINDOWS:
        series = rolling_correlation_series(df_a, df_b, window)
        key = f"{window}D"

        if series.empty:
            out[key] = {"current": None, "prior": None, "change": None, "percentile_1y": None}
            continue

        current = float(series.iloc[-1])
        prior = float(series.iloc[-2]) if len(series) >= 2 else None

        out[key] = {
            "current": round(current, 3),
            "prior": round(prior, 3) if prior is not None else None,
            "change": round(current - prior, 3) if prior is not None else None,
            "percentile_1y": _percentile_rank(series, current),
        }

    return out


def all_correlation_summaries(price_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Dict]:
    out = {}
    for a, b in config.CORRELATION_PAIRS:
        df_a = price_dfs.get(a) if price_dfs else None
        df_b = price_dfs.get(b) if price_dfs else None
        out[f"{a}_{b}"] = correlation_summary(a, b, df_a=df_a, df_b=df_b)
    return out
