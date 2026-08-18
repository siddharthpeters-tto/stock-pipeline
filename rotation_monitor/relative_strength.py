"""
rotation_monitor.relative_strength — relative returns & RS ratios (§6, §7).

Two distinct concepts, both implemented here:

1. Relative return = ETF return - benchmark return, for a given horizon.
   Example: SOXX -3.0% vs SPY -0.5% on 1D => relative return -2.5%. This
   isolates theme-specific strength/weakness from broad market moves.

2. Relative-strength ratio = ETF price / other price, normalized to 100 at
   the start of a chosen window. A rising IGV/SOXX ratio means software is
   outperforming semis over that window, independent of direction of the
   broader market. Normalization anchors every window to the same starting
   point so the *shape* of the line is what carries the signal, not its
   absolute level.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor import config
from rotation_monitor.returns import compute_returns, load_price_df

RELATIVE_HORIZONS = ["1D", "3D", "5D", "10D", "20D", "MTD"]


def relative_returns(
    etf_returns: Dict[str, Optional[float]],
    benchmark_returns: Dict[str, Optional[float]],
) -> Dict[str, Optional[float]]:
    """
    ETF return - benchmark return, per horizon. None propagates if either
    side is unavailable (never estimate a missing value, per §30).
    """
    out: Dict[str, Optional[float]] = {}
    for h in RELATIVE_HORIZONS:
        etf_r = etf_returns.get(h)
        bench_r = benchmark_returns.get(h)
        out[h] = None if (etf_r is None or bench_r is None) else etf_r - bench_r
    return out


def compute_relative_returns_for_universe(
    universe_returns: Dict[str, Dict[str, Optional[float]]]
) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
    """
    Given {symbol: {horizon: return}} for the whole universe (from
    returns.compute_returns), returns:
        {symbol: {"vs_SPY": {horizon: rel_return}, "vs_QQQ": {...}}}
    """
    out: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
    spy = universe_returns.get("SPY", {})
    qqq = universe_returns.get("QQQ", {})

    for symbol, r in universe_returns.items():
        out[symbol] = {
            "vs_SPY": relative_returns(r, spy) if symbol != "SPY" else {h: 0.0 for h in RELATIVE_HORIZONS},
            "vs_QQQ": relative_returns(r, qqq) if symbol != "QQQ" else {h: 0.0 for h in RELATIVE_HORIZONS},
        }

    return out


def normalized_ratio_series(
    df_a: pd.DataFrame, df_b: pd.DataFrame, window: Optional[int] = None
) -> pd.Series:
    """
    (close_a / close_b) aligned on shared dates, normalized to 100 at the
    first point of the series (or `window` trading days back from the latest
    point if given).

    Uses `window + 1` rows so the normalized start point sits exactly
    `window` trading days before the end — matching the convention in
    returns.return_over_n_days(df, window), which compares the latest close
    against the close `window` rows earlier. Without the "+1" a "20D window"
    ratio and a "20D return" would silently disagree by one trading day.
    """
    a = df_a["adj_close"].dropna()
    b = df_b["adj_close"].dropna()

    joined = pd.concat([a, b], axis=1, join="inner", keys=["a", "b"])
    if joined.empty:
        return pd.Series(dtype=float)

    if window:
        joined = joined.tail(window + 1)

    ratio = joined["a"] / joined["b"]
    if ratio.empty or ratio.iloc[0] == 0:
        return pd.Series(dtype=float)

    return (ratio / ratio.iloc[0]) * 100.0


def rs_ratio_summary(
    symbol_a: str, symbol_b: str, window: int = 20,
    df_a: Optional[pd.DataFrame] = None, df_b: Optional[pd.DataFrame] = None,
) -> Dict[str, Optional[float]]:
    """
    Current normalized RS ratio value and its % change over `window` trading
    days for one pair (e.g. IGV/SOXX). A value above 100 means `symbol_a`
    has outperformed `symbol_b` since the start of the window.

    Pass df_a/df_b to reuse already-loaded price history (e.g. from
    snapshot.py) instead of re-querying SQLite for every call.
    """
    if df_a is None:
        df_a = load_price_df(symbol_a)
    if df_b is None:
        df_b = load_price_df(symbol_b)

    series = normalized_ratio_series(df_a, df_b, window=window)
    if series.empty:
        return {"pair": f"{symbol_a}/{symbol_b}", "window_days": window,
                "current": None, "change_pct": None, "start_date": None, "end_date": None}

    current = series.iloc[-1]
    change_pct = current - 100.0  # already normalized to 100 at window start

    return {
        "pair": f"{symbol_a}/{symbol_b}",
        "window_days": window,
        "current": round(float(current), 3),
        "change_pct": round(float(change_pct), 3),
        "start_date": series.index.min().strftime("%Y-%m-%d"),
        "end_date": series.index.max().strftime("%Y-%m-%d"),
    }


def all_rs_ratio_summaries(window: int = 20) -> Dict[str, Dict]:
    return {
        f"{a}_{b}": rs_ratio_summary(a, b, window=window)
        for a, b in config.RS_RATIO_PAIRS
    }
