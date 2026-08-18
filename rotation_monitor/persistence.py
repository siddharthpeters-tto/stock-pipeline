"""
rotation_monitor.persistence — Rotation Persistence Score (§12).

Rather than requiring the pipeline to have run daily for weeks to build up
a history of past rankings, this reconstructs the last WAVE_RECENT_WINDOW
trading days of cross-sectional relative-return ranking directly from the
2 years of cached price history using vectorized pandas operations. That
means a meaningful persistence score is available from the very first run,
not just after weeks of accumulated daily snapshots.

Formula (weights documented in scoring_config.PERSISTENCE_WEIGHTS):
    + pct_positive_rel_20d   — % of the last 20 days with positive rolling
                                5D relative return vs SPY
    + pct_top_quartile_20d   — % of the last 20 days ranked top-quartile
                                among the tracked themes on that measure
    + trend_component        — above MA20/MA50 (1.0 / 0.5 / 0.0)
    + rs_ratio_trend         — primary-benchmark RS ratio rising over 20D
    - one-day-spike penalty  — when the latest 1D move dominates the 5D
                                move, the recent "strength" looks more like
                                a single session than a persistent trend
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor import config, trend
from rotation_monitor.relative_strength import rs_ratio_summary
from rotation_monitor.returns import load_price_df, return_over_n_days
from rotation_monitor.scoring_config import (
    PERSISTENCE_BANDS,
    PERSISTENCE_ONE_DAY_SPIKE_PENALTY,
    PERSISTENCE_ONE_DAY_SPIKE_RATIO,
    PERSISTENCE_WEIGHTS,
    WAVE_RECENT_WINDOW,
)

RELATIVE_RETURN_HORIZON = 5  # rolling N-day window used for the daily persistence series


def label_for_score(score: float) -> str:
    for lo, hi, label in PERSISTENCE_BANDS:
        if lo <= score < hi or (hi == 100 and score == 100):
            return label
    return "DATA_UNAVAILABLE"


def _rolling_n_day_return_series(closes: pd.Series, n: int) -> pd.Series:
    return closes / closes.shift(n) - 1.0


def build_rolling_relative_return_matrix(
    price_dfs: Dict[str, pd.DataFrame],
    symbols: list,
    horizon: int = RELATIVE_RETURN_HORIZON,
) -> pd.DataFrame:
    """
    DataFrame indexed by date, one column per symbol: the symbol's rolling
    `horizon`-day return minus SPY's rolling `horizon`-day return, for every
    date where both are available. This is the substrate for persistence
    and for breadth's day-by-day reconstruction.
    """
    spy_closes = price_dfs["SPY"]["adj_close"].dropna()
    spy_roll = _rolling_n_day_return_series(spy_closes, horizon)

    series_by_symbol = {}
    for sym in symbols:
        df = price_dfs.get(sym)
        if df is None or df.empty:
            continue
        closes = df["adj_close"].dropna()
        sym_roll = _rolling_n_day_return_series(closes, horizon)
        series_by_symbol[sym] = sym_roll

    matrix = pd.DataFrame(series_by_symbol)
    matrix = matrix.sub(spy_roll, axis=0)
    matrix = matrix.dropna(how="all")
    return matrix


def compute_persistence_scores(
    price_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    window: int = WAVE_RECENT_WINDOW,
) -> Dict[str, Dict]:
    symbols = config.non_benchmark_tickers()

    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in symbols + ["SPY"]}

    matrix = build_rolling_relative_return_matrix(price_dfs, symbols)
    recent = matrix.tail(window).dropna(how="all", axis=0)

    # cross-sectional percentile rank per day (0..1), NaN-safe
    pct_rank = recent.rank(axis=1, pct=True)

    results: Dict[str, Dict] = {}

    for sym in symbols:
        df = price_dfs.get(sym)
        components: Dict[str, Optional[float]] = {}

        if sym in recent.columns and not recent[sym].dropna().empty:
            series = recent[sym].dropna()
            components["pct_positive_rel_20d"] = float((series > 0).mean())
        else:
            components["pct_positive_rel_20d"] = None

        if sym in pct_rank.columns and not pct_rank[sym].dropna().empty:
            ranks = pct_rank[sym].dropna()
            components["pct_top_quartile_20d"] = float((ranks >= 0.75).mean())
        else:
            components["pct_top_quartile_20d"] = None

        tr = trend.compute_trend(sym, df=df) if df is not None else {}
        above20 = tr.get("above_ma20")
        above50 = tr.get("above_ma50")
        if above20 is None or above50 is None:
            components["trend_component"] = None
        else:
            components["trend_component"] = 1.0 if (above20 and above50) else (0.5 if (above20 or above50) else 0.0)

        benchmark = config.get_benchmark(sym) or "SPY"
        ratio_summary = rs_ratio_summary(sym, benchmark, window=20, df_a=df, df_b=price_dfs.get(benchmark))
        change_pct = ratio_summary.get("change_pct")
        components["rs_ratio_trend"] = None if change_pct is None else (1.0 if change_pct > 0 else 0.0)

        # weighted sum over whichever components are available; renormalize
        # by the weight actually used so missing data doesn't silently drag
        # the score toward zero
        weighted_sum = 0.0
        weight_used = 0.0
        for key, weight in PERSISTENCE_WEIGHTS.items():
            val = components.get(key)
            if val is None:
                continue
            weighted_sum += val * weight
            weight_used += weight

        if weight_used == 0:
            results[sym] = {"score": None, "label": "DATA_UNAVAILABLE", "components": components}
            continue

        raw_score = (weighted_sum / weight_used) * 100.0

        # one-day-spike penalty
        one_day = return_over_n_days(df, 1) if df is not None else None
        five_day = return_over_n_days(df, 5) if df is not None else None
        spike_flag = False
        if one_day is not None and five_day is not None and abs(five_day) > 1e-9:
            same_direction = (one_day > 0) == (five_day > 0)
            if same_direction and abs(one_day) / abs(five_day) >= PERSISTENCE_ONE_DAY_SPIKE_RATIO:
                spike_flag = True

        penalty = PERSISTENCE_ONE_DAY_SPIKE_PENALTY if spike_flag else 0
        final_score = max(0.0, min(100.0, raw_score - penalty))

        results[sym] = {
            "score": round(final_score, 1),
            "label": label_for_score(final_score),
            "components": {k: (round(v, 3) if v is not None else None) for k, v in components.items()},
            "one_day_spike_penalty_applied": spike_flag,
        }

    return results
