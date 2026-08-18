"""
rotation_monitor.backtest — descriptive rotation pattern analysis (§31).

This is NOT a trading-strategy backtest. It produces descriptive statistics
about historical leadership behavior between a pair of themes — flip
frequency, run lengths, spread volatility, correlation drift — at several
lookback horizons, so a "wave pattern" hypothesis can be checked against
data rather than assumed. It reuses wave_detection's own skeptical
classification at each horizon rather than a separate, looser set of rules.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor.correlations import rolling_correlation_series
from rotation_monitor.returns import load_price_df
from rotation_monitor.wave_detection import _spread_stats, daily_spread, pair_wave_summary

BACKTEST_WINDOWS = {"20D": 20, "60D": 60, "6mo": 126, "1yr": 252}


def _window_stats(spread: pd.Series, window_days: int) -> Dict:
    window = spread.tail(window_days)
    stats = _spread_stats(window)

    flips_per_month = None
    if stats["flip_count"] is not None and window_days > 0:
        months = window_days / 21.0
        flips_per_month = round(stats["flip_count"] / months, 2) if months > 0 else None

    spread_volatility = float(window.std()) if len(window) > 1 else None

    return {
        "window_days": window_days,
        "n_days_available": stats["n_days"],
        "flip_count": stats["flip_count"],
        "flips_per_month": flips_per_month,
        "avg_run_length": stats["avg_run_length"],
        "median_run_length": stats["median_run_length"],
        "max_run_length": stats["max_run_length"],
        "spread_volatility": round(spread_volatility, 5) if spread_volatility is not None else None,
        "pct_days_symbol_a_led": stats["pct_positive_days"],
    }


def _correlation_drift(df_a: pd.DataFrame, df_b: pd.DataFrame, window_days: int) -> Dict:
    series = rolling_correlation_series(df_a, df_b, window=min(20, max(5, window_days // 4)))
    windowed = series.tail(window_days)
    if windowed.empty:
        return {"start": None, "end": None, "change": None}

    start = float(windowed.iloc[0])
    end = float(windowed.iloc[-1])
    return {"start": round(start, 3), "end": round(end, 3), "change": round(end - start, 3)}


def analyze_pair(
    symbol_a: str, symbol_b: str,
    df_a: Optional[pd.DataFrame] = None, df_b: Optional[pd.DataFrame] = None,
) -> Dict:
    if df_a is None:
        df_a = load_price_df(symbol_a)
    if df_b is None:
        df_b = load_price_df(symbol_b)

    spread = daily_spread(df_a, df_b)

    windows: Dict[str, Dict] = {}
    for label, days in BACKTEST_WINDOWS.items():
        stats = _window_stats(spread, days)
        stats["correlation_drift"] = _correlation_drift(df_a, df_b, days)
        # reuse wave_detection's own skeptical classification at this horizon,
        # always measured against the pair's full-history baseline
        wave = pair_wave_summary(symbol_a, symbol_b, df_a=df_a, df_b=df_b, recent_window_days=days)
        stats["classification"] = wave.get("classification")
        stats["z_score"] = wave.get("z_score")
        windows[label] = stats

    # does one side eventually establish persistent leadership? look at the
    # longest available window's leadership share
    longest_label = max(windows, key=lambda k: windows[k]["window_days"] if windows[k]["n_days_available"] else -1)
    longest = windows[longest_label]
    established_leader = None
    if longest.get("pct_days_symbol_a_led") is not None:
        pct = longest["pct_days_symbol_a_led"]
        if pct >= 65:
            established_leader = symbol_a
        elif pct <= 35:
            established_leader = symbol_b

    return {
        "pair": f"{symbol_a}_{symbol_b}",
        "windows": windows,
        "established_leader_longest_window": established_leader,
    }


def analyze_pairs(pairs, price_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Dict]:
    out = {}
    for a, b in pairs:
        df_a = price_dfs.get(a) if price_dfs else None
        df_b = price_dfs.get(b) if price_dfs else None
        out[f"{a}_{b}"] = analyze_pair(a, b, df_a=df_a, df_b=df_b)
    return out
