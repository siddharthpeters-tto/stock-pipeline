"""
rotation_monitor.breadth — rotation breadth, overall and by category (§16).

Distinguishes "one ETF rallying" from "broad category leadership" by
measuring what fraction of themes (overall, and within each category) are
outperforming SPY, and what fraction sit above their moving averages.

Breadth is also produced as a full historical daily series (not just
today's snapshot) by reusing persistence.build_rolling_relative_return_matrix
— this gives alerts.py a real trend (e.g. "5D ago" vs "today") without
needing accumulated daily state history.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor import config, persistence, trend
from rotation_monitor.returns import load_price_df
from rotation_monitor.scoring_config import BREADTH_HORIZONS


def category_map() -> Dict[str, list]:
    universe = config.load_universe()
    out: Dict[str, list] = {}
    for sym, cfg in universe.items():
        if sym in config.BENCHMARKS:
            continue
        out.setdefault(cfg["category"], []).append(sym)
    return out


def breadth_time_series(
    symbols: list, horizon_days: int, price_dfs: Dict[str, pd.DataFrame]
) -> pd.Series:
    """Fraction of `symbols` with positive rolling relative return vs SPY, per day."""
    matrix = persistence.build_rolling_relative_return_matrix(price_dfs, symbols, horizon=horizon_days)
    if matrix.empty:
        return pd.Series(dtype=float)
    cols = [c for c in symbols if c in matrix.columns]
    if not cols:
        return pd.Series(dtype=float)
    return (matrix[cols] > 0).mean(axis=1)


def _pct_above_ma(symbols: list, window: int, price_dfs: Dict[str, pd.DataFrame]) -> Optional[float]:
    flags = []
    for sym in symbols:
        df = price_dfs.get(sym)
        if df is None:
            continue
        dist = trend.distance_from_ma(df, window)
        if dist is not None:
            flags.append(dist > 0)
    if not flags:
        return None
    return round(sum(flags) / len(flags) * 100.0, 1)


def _breadth_snapshot(symbols: list, price_dfs: Dict[str, pd.DataFrame]) -> Dict:
    out: Dict = {}
    for h in BREADTH_HORIZONS:
        n = int(h.replace("D", ""))
        series = breadth_time_series(symbols, n, price_dfs)
        out[f"outperforming_spy_{h.lower()}_pct"] = round(float(series.iloc[-1]) * 100.0, 1) if not series.empty else None

    out["above_ma20_pct"] = _pct_above_ma(symbols, 20, price_dfs)
    out["above_ma50_pct"] = _pct_above_ma(symbols, 50, price_dfs)
    return out


def compute_breadth(price_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> Dict:
    themes = config.non_benchmark_tickers()

    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in config.universe_tickers()}

    overall = _breadth_snapshot(themes, price_dfs)

    persistence_scores = persistence.compute_persistence_scores(price_dfs=price_dfs)
    scored = [v["score"] for v in persistence_scores.values() if v["score"] is not None]
    overall["positive_persistence_pct"] = (
        round(sum(1 for s in scored if s >= 50) / len(scored) * 100.0, 1) if scored else None
    )

    by_category = {}
    for cat, symbols in category_map().items():
        by_category[cat] = _breadth_snapshot(symbols, price_dfs)

    return {"overall": overall, "by_category": by_category}


def breadth_trend(
    symbols: list, horizon_days: int, lookback_days: int, price_dfs: Dict[str, pd.DataFrame]
) -> Optional[float]:
    """Change in breadth (percentage points) over the last `lookback_days`."""
    series = breadth_time_series(symbols, horizon_days, price_dfs)
    if len(series) < lookback_days + 1:
        return None
    current = series.iloc[-1]
    prior = series.iloc[-1 - lookback_days]
    return round((current - prior) * 100.0, 1)
