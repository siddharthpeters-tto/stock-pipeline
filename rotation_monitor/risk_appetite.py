"""
rotation_monitor.risk_appetite — Risk Appetite Score (§15), 0-100.

A weighted blend of speculative/growth relative strength vs defensive
relative weakness. Weights and the percentage-point-to-score scaling are in
scoring_config.RISK_APPETITE_WEIGHTS / RISK_APPETITE_PCT_TO_SCORE_SCALE —
nothing here is an arbitrary inline number.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor import config
from rotation_monitor.relative_strength import relative_returns, rs_ratio_summary
from rotation_monitor.returns import compute_returns, load_price_df
from rotation_monitor.scoring_config import (
    RISK_APPETITE_BANDS,
    RISK_APPETITE_HORIZON,
    RISK_APPETITE_PCT_TO_SCORE_SCALE,
    RISK_APPETITE_WEIGHTS,
)


def label_for_score(score: float) -> str:
    for lo, hi, label in RISK_APPETITE_BANDS:
        if lo <= score < hi or (hi == 100 and score == 100):
            return label
    return "DATA_UNAVAILABLE"


def _pct_to_score(relative_return_pct: Optional[float]) -> Optional[float]:
    if relative_return_pct is None:
        return None
    # relative_return_pct is a fraction (0.05 == 5%); convert to percentage
    # points before scaling, i.e. 0.05 -> 5pp -> 5*SCALE score points.
    score = 50.0 + (relative_return_pct * 100.0) * RISK_APPETITE_PCT_TO_SCORE_SCALE
    return max(0.0, min(100.0, score))


def compute_risk_appetite(price_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> Dict:
    needed = ["SPY", "QQQ", "XBI", "UFO", "DRIV", "KRE", "SOXX", "XLY", "XLP", "XLV", "XLU"]

    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in needed}

    universe = config.load_universe()
    rets = {s: compute_returns(s, df=price_dfs[s]) for s in needed if s in price_dfs}

    def rel_vs(symbol: str, benchmark: str) -> Optional[float]:
        if symbol not in rets or benchmark not in rets:
            return None
        r = relative_returns(rets[symbol], rets[benchmark])
        return r.get(RISK_APPETITE_HORIZON)

    components_raw: Dict[str, Optional[float]] = {
        "XBI": rel_vs("XBI", universe.get("XBI", {}).get("benchmark", "SPY")),
        "UFO": rel_vs("UFO", universe.get("UFO", {}).get("benchmark", "QQQ")),
        "DRIV": rel_vs("DRIV", universe.get("DRIV", {}).get("benchmark", "QQQ")),
        "KRE": rel_vs("KRE", universe.get("KRE", {}).get("benchmark", "SPY")),
        "SOXX": rel_vs("SOXX", universe.get("SOXX", {}).get("benchmark", "QQQ")),
    }

    xly_xlp = rs_ratio_summary("XLY", "XLP", window=20, df_a=price_dfs.get("XLY"), df_b=price_dfs.get("XLP"))
    xly_xlp_change = xly_xlp.get("change_pct")  # already a percentage-point-like number

    defensive_vals = [v for v in (
        rel_vs("XLV", "SPY"), rel_vs("XLP", "SPY"), rel_vs("XLU", "SPY")
    ) if v is not None]
    defensive_avg = sum(defensive_vals) / len(defensive_vals) if defensive_vals else None

    scores: Dict[str, Optional[float]] = {
        "XBI": _pct_to_score(components_raw["XBI"]),
        "UFO": _pct_to_score(components_raw["UFO"]),
        "DRIV": _pct_to_score(components_raw["DRIV"]),
        "KRE": _pct_to_score(components_raw["KRE"]),
        "XLY_XLP_ratio": None if xly_xlp_change is None else max(0.0, min(100.0, 50.0 + xly_xlp_change * RISK_APPETITE_PCT_TO_SCORE_SCALE)),
        "SOXX": _pct_to_score(components_raw["SOXX"]),
        "defensive_weakness": None if defensive_avg is None else _pct_to_score(-defensive_avg),
    }

    weighted_sum = 0.0
    weight_used = 0.0
    for key, weight in RISK_APPETITE_WEIGHTS.items():
        val = scores.get(key)
        if val is None:
            continue
        weighted_sum += val * weight
        weight_used += weight

    if weight_used == 0:
        return {"score": None, "label": "DATA_UNAVAILABLE", "components": scores}

    final_score = weighted_sum / weight_used
    final_score = max(0.0, min(100.0, final_score))

    return {
        "score": round(final_score, 1),
        "label": label_for_score(final_score),
        "components": {k: (round(v, 1) if v is not None else None) for k, v in scores.items()},
        "raw_relative_returns_20d": {k: (round(v, 4) if v is not None else None) for k, v in components_raw.items()},
    }
