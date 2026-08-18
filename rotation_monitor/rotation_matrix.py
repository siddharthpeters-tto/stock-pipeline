"""
rotation_monitor.rotation_matrix — category-level rotation matrix (§22).

Shows, per category, which way relative strength vs SPY is moving across
1D/5D/10D/20D, plus the category's average persistence score. Every cell is
computed, not eyeballed — thresholds are in
scoring_config.ROTATION_MATRIX_THRESHOLDS.

Uses SPY uniformly as the comparison benchmark for every category (rather
than each ticker's own configured benchmark) because this view is
specifically "category vs the broad market", distinct from the
ticker-specific benchmarking used in persistence.py/regime.py.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor import breadth as breadth_mod
from rotation_monitor import config, persistence
from rotation_monitor.relative_strength import relative_returns
from rotation_monitor.returns import compute_returns, load_price_df
from rotation_monitor.scoring_config import ROTATION_MATRIX_HORIZONS, ROTATION_MATRIX_THRESHOLDS


def _label(avg_rel: Optional[float]) -> str:
    if avg_rel is None:
        return "DATA_UNAVAILABLE"
    t = ROTATION_MATRIX_THRESHOLDS
    if avg_rel >= t["strong"]:
        return "STRONG_UP"
    if avg_rel <= -t["strong"]:
        return "STRONG_DOWN"
    if abs(avg_rel) <= t["flat"]:
        return "FLAT"
    return "UP" if avg_rel > 0 else "DOWN"


def compute_rotation_matrix(
    price_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    persistence_scores: Optional[Dict] = None,
) -> Dict:
    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in config.universe_tickers()}

    if persistence_scores is None:
        persistence_scores = persistence.compute_persistence_scores(price_dfs=price_dfs)

    spy_returns = compute_returns("SPY", df=price_dfs["SPY"])
    categories = breadth_mod.category_map()

    matrix = {}
    for cat, symbols in categories.items():
        row: Dict[str, object] = {}

        for h in ROTATION_MATRIX_HORIZONS:
            vals = []
            for sym in symbols:
                df = price_dfs.get(sym)
                if df is None:
                    continue
                sym_returns = compute_returns(sym, df=df)
                rel = relative_returns(sym_returns, spy_returns).get(h)
                if rel is not None:
                    vals.append(rel)
            avg_rel = sum(vals) / len(vals) if vals else None
            row[h] = {"label": _label(avg_rel), "avg_relative_return": round(avg_rel, 4) if avg_rel is not None else None}

        scores = [persistence_scores.get(s, {}).get("score") for s in symbols]
        scores = [s for s in scores if s is not None]
        row["avg_persistence"] = round(sum(scores) / len(scores), 1) if scores else None
        row["members"] = symbols

        matrix[cat] = row

    return matrix
