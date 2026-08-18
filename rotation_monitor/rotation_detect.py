"""
rotation_monitor.rotation_detect — pairwise capital-rotation detection (§13).

For each configured pair (config.ROTATION_DETECT_PAIRS), tests both possible
directions ("A over B" and "B over A") against a fixed, weighted set of
deterministic conditions (scoring_config.ROTATION_DETECT_WEIGHTS), picks
whichever direction the evidence actually favors, and reports both the
evidence that supports it and the evidence that argues against it —
mirroring the worked example in the spec (IGV/SOXX: software strengthening
short-term, while semis remain stronger on the 20D absolute-return
timeframe).

This module only classifies; it never recommends a trade.
"""

from typing import Dict, Optional

import numpy as np
import pandas as pd

from rotation_monitor import config
from rotation_monitor.correlations import correlation_summary
from rotation_monitor.relative_strength import rs_ratio_summary
from rotation_monitor.returns import load_price_df, return_over_n_days
from rotation_monitor.scoring_config import (
    ROTATION_DETECT_ESTABLISHED_MIN_DURATION,
    ROTATION_DETECT_MIN_CONFIDENCE,
    ROTATION_DETECT_WEIGHTS,
)
from rotation_monitor.volume import compute_volume
from rotation_monitor.wave_detection import _runs, daily_spread


def _windowed_return(closes: pd.Series, end_offset: int, window: int) -> Optional[float]:
    """Return over `window` trading days, ending `end_offset` days before the latest close."""
    idx_end = len(closes) - 1 - end_offset
    idx_start = idx_end - window
    if idx_start < 0 or idx_end < 0:
        return None
    start_price = closes.iloc[idx_start]
    end_price = closes.iloc[idx_end]
    if start_price == 0:
        return None
    return float(end_price / start_price - 1.0)


def _relative_return_trend(df_sym: pd.DataFrame, df_bench: pd.DataFrame, window: int = 5):
    """(current windowed relative return, prior windowed relative return)."""
    sym_closes = df_sym["adj_close"].dropna()
    bench_closes = df_bench["adj_close"].dropna()

    current_sym = _windowed_return(sym_closes, 0, window)
    current_bench = _windowed_return(bench_closes, 0, window)
    prior_sym = _windowed_return(sym_closes, window, window)
    prior_bench = _windowed_return(bench_closes, window, window)

    current_rel = None if current_sym is None or current_bench is None else current_sym - current_bench
    prior_rel = None if prior_sym is None or prior_bench is None else prior_sym - prior_bench
    return current_rel, prior_rel


def _trailing_run(spread: pd.Series):
    """(sign, length) of the most recent consecutive-day run in a spread series."""
    signs = np.sign(spread.to_numpy())
    signs = signs[signs != 0]
    if len(signs) == 0:
        return 0, 0
    runs = _runs(signs)
    return int(signs[-1]), int(runs[-1])


def _direction_score(
    leader: str, laggard: str,
    df_leader: pd.DataFrame, df_laggard: pd.DataFrame,
    df_leader_bench: pd.DataFrame, df_laggard_bench: pd.DataFrame,
    ratio_5d_change: Optional[float], ratio_20d_change: Optional[float],
    ratio_direction_sign: int,
) -> Dict:
    """Score one hypothesis ("leader outperforming laggard"). Positive sign
    on the ratio change (leader/laggard) is what supports this hypothesis."""
    w = ROTATION_DETECT_WEIGHTS
    score = 0.0
    matched = []

    def _matches_direction(value: Optional[float]) -> bool:
        # a change of exactly zero (or None) is not evidence for either
        # direction -- only a real sign match counts.
        if value is None or value == 0:
            return False
        return (value > 0) == (ratio_direction_sign > 0)

    if _matches_direction(ratio_5d_change):
        score += w["rs_ratio_5d_direction"]
        matched.append(f"{leader}/{laggard} 5D RS ratio moving in {leader}'s favor")

    if _matches_direction(ratio_20d_change):
        score += w["rs_ratio_20d_direction"]
        matched.append(f"{leader}/{laggard} 20D RS ratio moving in {leader}'s favor")

    leader_current, leader_prior = _relative_return_trend(df_leader, df_leader_bench)
    if leader_current is not None and leader_prior is not None and leader_current > leader_prior:
        score += w["leader_relative_improving"]
        matched.append(f"{leader} relative strength improving vs its benchmark")

    laggard_current, laggard_prior = _relative_return_trend(df_laggard, df_laggard_bench)
    if laggard_current is not None and laggard_prior is not None and laggard_current < laggard_prior:
        score += w["laggard_relative_deteriorating"]
        matched.append(f"{laggard} relative strength deteriorating vs its benchmark")

    return {"score": score, "matched": matched}


def detect_pair_rotation(
    symbol_a: str, symbol_b: str, label_a_over_b: str, label_b_over_a: str,
    price_dfs: Optional[Dict[str, pd.DataFrame]] = None,
) -> Dict:
    universe = config.load_universe()
    bench_a = universe.get(symbol_a, {}).get("benchmark") or "SPY"
    bench_b = universe.get(symbol_b, {}).get("benchmark") or "SPY"

    def get_df(sym):
        if price_dfs and sym in price_dfs:
            return price_dfs[sym]
        return load_price_df(sym)

    df_a, df_b = get_df(symbol_a), get_df(symbol_b)
    df_bench_a, df_bench_b = get_df(bench_a), get_df(bench_b)

    ratio_5d = rs_ratio_summary(symbol_a, symbol_b, window=5, df_a=df_a, df_b=df_b)
    ratio_20d = rs_ratio_summary(symbol_a, symbol_b, window=20, df_a=df_a, df_b=df_b)

    # hypothesis 1: A over B (ratio A/B rising supports this => sign +1)
    hyp_a = _direction_score(symbol_a, symbol_b, df_a, df_b, df_bench_a, df_bench_b,
                              ratio_5d.get("change_pct"), ratio_20d.get("change_pct"), +1)
    # hypothesis 2: B over A (ratio A/B falling supports this => sign -1)
    hyp_b = _direction_score(symbol_b, symbol_a, df_b, df_a, df_bench_b, df_bench_a,
                              ratio_5d.get("change_pct"), ratio_20d.get("change_pct"), -1)

    if hyp_a["score"] == 0 and hyp_b["score"] == 0:
        winner, state, matched = None, "NEUTRAL", []
    elif hyp_a["score"] >= hyp_b["score"]:
        winner, state, matched = symbol_a, label_a_over_b, hyp_a["matched"]
    else:
        winner, state, matched = symbol_b, label_b_over_a, hyp_b["matched"]

    directional_score = max(hyp_a["score"], hyp_b["score"])

    evidence = list(matched)
    counter_evidence = []

    # confirming conditions layered on top of the winning direction
    corr = correlation_summary(symbol_a, symbol_b, df_a=df_a, df_b=df_b)
    corr_20 = corr.get("20D", {})
    corr_declining = (
        corr_20.get("current") is not None and corr_20.get("change") is not None
        and corr_20["change"] < 0
    )
    confirmation_score = 0.0
    if state != "NEUTRAL" and corr_declining:
        confirmation_score += ROTATION_DETECT_WEIGHTS["correlation_declining"]
        evidence.append(f"20D correlation between {symbol_a} and {symbol_b} is declining ({corr_20.get('current')}, prior {corr_20.get('prior')}).")
    elif state != "NEUTRAL":
        counter_evidence.append(f"20D correlation between {symbol_a} and {symbol_b} is not declining ({corr_20.get('current')}).")

    vol_a = compute_volume(symbol_a, df=df_a)
    vol_b = compute_volume(symbol_b, df=df_b)
    volume_confirms = vol_a["classification"] in ("ELEVATED", "VERY_ELEVATED") or vol_b["classification"] in ("ELEVATED", "VERY_ELEVATED")
    if state != "NEUTRAL" and volume_confirms:
        confirmation_score += ROTATION_DETECT_WEIGHTS["volume_confirmation"]
        evidence.append(f"Trading volume confirmation present ({symbol_a}: {vol_a['classification']}, {symbol_b}: {vol_b['classification']}).")
    elif state != "NEUTRAL":
        counter_evidence.append("No elevated trading volume confirming the move on either side.")

    confidence = min(100.0, directional_score + confirmation_score)

    # explicit counter-evidence: does the "loser" still lead on raw 20D
    # absolute return? (mirrors the spec's own worked example)
    if state != "NEUTRAL":
        loser = symbol_b if winner == symbol_a else symbol_a
        winner_abs_20d = return_over_n_days(df_a if winner == symbol_a else df_b, 20)
        loser_abs_20d = return_over_n_days(df_b if winner == symbol_a else df_a, 20)
        if winner_abs_20d is not None and loser_abs_20d is not None and loser_abs_20d > winner_abs_20d:
            counter_evidence.append(f"{loser} remains stronger than {winner} on raw 20D absolute return.")

    # duration: current consecutive-day run of daily leadership matching the winner
    duration_days = 0
    if state != "NEUTRAL":
        spread = daily_spread(df_a, df_b)  # positive => A led that day
        sign, length = _trailing_run(spread)
        matches_winner = (sign > 0 and winner == symbol_a) or (sign < 0 and winner == symbol_b)
        duration_days = length if matches_winner else 0
        if duration_days < ROTATION_DETECT_ESTABLISHED_MIN_DURATION:
            counter_evidence.append(
                f"Daily leadership has only favored {winner} for {duration_days} consecutive session(s) — "
                f"below the {ROTATION_DETECT_ESTABLISHED_MIN_DURATION}-day bar for an established move."
            )

    if confidence < ROTATION_DETECT_MIN_CONFIDENCE:
        state = "NEUTRAL"

    return {
        "pair": f"{symbol_a}_{symbol_b}",
        "state": state,
        "confidence": round(confidence, 1),
        "duration_days": duration_days,
        "evidence": evidence,
        "counter_evidence": counter_evidence,
    }


def all_pair_rotations(price_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> list:
    out = []
    for spec in config.ROTATION_DETECT_PAIRS:
        a, b = spec["pair"]
        out.append(detect_pair_rotation(a, b, spec["a_over_b"], spec["b_over_a"], price_dfs=price_dfs))
    return out
