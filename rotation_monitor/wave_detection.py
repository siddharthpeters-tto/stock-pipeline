"""
rotation_monitor.wave_detection — leadership-flip / oscillation analysis
(§21, §32).

This module is deliberately built to try to DISPROVE an apparent wave
pattern before labeling one:

  1. A recent leadership-flip rate is only "unusual" if it deviates from
     this exact pair's own historical baseline flip rate by a real margin —
     tested with a binomial z-score, not eyeballed. Thresholds are in
     scoring_config (WAVE_Z_POSSIBLE_OSCILLATION / WAVE_Z_REPEATED_ROTATION).
  2. A high flip rate with tiny day-to-day magnitude is noise, not
     rotation — flip days must clear a magnitude floor relative to the
     pair's own historical daily-spread volatility.
  3. A pattern that only exists because of one extreme session is not a
     pattern — the z-score is recomputed with the single largest-magnitude
     day removed, and the classification is downgraded if the pattern
     doesn't survive that.
  4. A strongly one-sided period (one theme leading almost every day) is
     reported as PERSISTENT_ONE_SIDE_LEADERSHIP, explicitly distinct from
     oscillation — a low flip rate should never be mislabeled as a wave.

The output for a pair is one of:
    NO_PATTERN, POSSIBLE_OSCILLATION, REPEATED_ROTATION,
    PERSISTENT_ONE_SIDE_LEADERSHIP, INSUFFICIENT_HISTORY
"""

import math
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from rotation_monitor import config
from rotation_monitor.returns import load_price_df
from rotation_monitor.scoring_config import (
    WAVE_BASELINE_WINDOW,
    WAVE_MAGNITUDE_FLOOR_STD_MULTIPLE,
    WAVE_MIN_BASELINE_DAYS,
    WAVE_ONE_SIDE_DOMINANCE,
    WAVE_RECENT_WINDOW,
    WAVE_Z_POSSIBLE_OSCILLATION,
    WAVE_Z_REPEATED_ROTATION,
)


def daily_spread(df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.Series:
    """Daily return of A minus daily return of B, aligned on shared dates."""
    ret_a = df_a["adj_close"].dropna().pct_change().dropna()
    ret_b = df_b["adj_close"].dropna().pct_change().dropna()
    joined = pd.concat([ret_a, ret_b], axis=1, join="inner", keys=["a", "b"])
    return (joined["a"] - joined["b"]).dropna()


def _runs(signs: np.ndarray) -> List[int]:
    """Consecutive-run lengths from a +1/-1 sign array (zeros pre-filtered)."""
    if len(signs) == 0:
        return []
    runs = []
    current_len = 1
    for i in range(1, len(signs)):
        if signs[i] == signs[i - 1]:
            current_len += 1
        else:
            runs.append(current_len)
            current_len = 1
    runs.append(current_len)
    return runs


def _flip_count(signs: np.ndarray) -> int:
    if len(signs) < 2:
        return 0
    return int((signs[1:] != signs[:-1]).sum())


def _binomial_z(observed_flips: int, n_interior: int, p_baseline: Optional[float]) -> Optional[float]:
    if p_baseline is None or n_interior <= 0:
        return None
    expected = n_interior * p_baseline
    variance = n_interior * p_baseline * (1 - p_baseline)
    if variance <= 0:
        return None
    return (observed_flips - expected) / math.sqrt(variance)


def _spread_stats(spread: pd.Series) -> Dict:
    values = spread.to_numpy()
    signs = np.sign(values)
    signs = signs[signs != 0]  # drop exact-zero spread days (rare with floats)

    flips = _flip_count(signs)
    n_interior = max(len(signs) - 1, 0)
    flip_rate = flips / n_interior if n_interior > 0 else None
    runs = _runs(signs)

    return {
        "n_days": int(len(signs)),
        "flip_count": flips,
        "n_interior": n_interior,
        "flip_rate": flip_rate,
        "runs": runs,
        "avg_run_length": round(float(np.mean(runs)), 2) if runs else None,
        "median_run_length": float(np.median(runs)) if runs else None,
        "max_run_length": int(np.max(runs)) if runs else None,
        "pct_positive_days": round(float((signs > 0).mean()) * 100.0, 1) if len(signs) else None,
    }


def pair_wave_summary(
    symbol_a: str, symbol_b: str,
    df_a: Optional[pd.DataFrame] = None, df_b: Optional[pd.DataFrame] = None,
    recent_window_days: Optional[int] = None, baseline_window_days: Optional[int] = None,
) -> Dict:
    """
    recent_window_days/baseline_window_days default to
    scoring_config.WAVE_RECENT_WINDOW/WAVE_BASELINE_WINDOW, but can be
    overridden — backtest.py uses this to run the same disproof logic at
    several different lookback horizons (20D/60D/6mo/1yr).
    """
    if df_a is None:
        df_a = load_price_df(symbol_a)
    if df_b is None:
        df_b = load_price_df(symbol_b)

    recent_window_days = recent_window_days or WAVE_RECENT_WINDOW
    baseline_window_days = baseline_window_days or WAVE_BASELINE_WINDOW

    spread = daily_spread(df_a, df_b)
    pair_label = f"{symbol_a}_{symbol_b}"

    if len(spread) < WAVE_MIN_BASELINE_DAYS:
        return {
            "pair": pair_label,
            "classification": "INSUFFICIENT_HISTORY",
            "reason": f"Only {len(spread)} days of overlapping history; need at least {WAVE_MIN_BASELINE_DAYS}.",
        }

    baseline_window = spread.tail(baseline_window_days)
    recent_window = spread.tail(recent_window_days)

    baseline_stats = _spread_stats(baseline_window)
    recent_stats = _spread_stats(recent_window)

    baseline_p = baseline_stats["flip_rate"]
    z = _binomial_z(recent_stats["flip_count"], recent_stats["n_interior"], baseline_p)

    # magnitude floor: average |spread| in the recent window vs the full
    # baseline's spread volatility
    baseline_std = float(baseline_window.std()) if len(baseline_window) > 1 else None
    recent_avg_abs = float(recent_window.abs().mean()) if len(recent_window) else None
    magnitude_meaningful = (
        baseline_std is not None and baseline_std > 0
        and recent_avg_abs is not None
        and recent_avg_abs >= WAVE_MAGNITUDE_FLOOR_STD_MULTIPLE * baseline_std
    )

    dominant_side_pct = recent_stats["pct_positive_days"]
    one_side_dominant = (
        dominant_side_pct is not None
        and (dominant_side_pct / 100.0 >= WAVE_ONE_SIDE_DOMINANCE
             or dominant_side_pct / 100.0 <= 1 - WAVE_ONE_SIDE_DOMINANCE)
    )

    evidence: List[str] = []
    counter_evidence: List[str] = []

    if one_side_dominant:
        classification = "PERSISTENT_ONE_SIDE_LEADERSHIP"
        leader = symbol_a if dominant_side_pct >= 50 else symbol_b
        evidence.append(f"{leader} led on {max(dominant_side_pct, 100 - dominant_side_pct):.1f}% of the last {recent_window_days} trading days.")
        evidence.append(f"Average leadership run length over the window: {recent_stats['avg_run_length']} days.")
    elif z is None:
        classification = "NO_PATTERN"
        counter_evidence.append("Baseline flip rate could not be established.")
    elif magnitude_meaningful and z >= WAVE_Z_REPEATED_ROTATION:
        classification = "REPEATED_ROTATION"
        evidence.append(f"Recent flip rate is {z:.2f} standard deviations above this pair's own 1yr baseline flip rate.")
        evidence.append(f"Average |daily spread| on recent flips ({recent_avg_abs:.4f}) clears the noise floor ({WAVE_MAGNITUDE_FLOOR_STD_MULTIPLE}x baseline std = {WAVE_MAGNITUDE_FLOOR_STD_MULTIPLE * baseline_std:.4f}).")
    elif magnitude_meaningful and z >= WAVE_Z_POSSIBLE_OSCILLATION:
        classification = "POSSIBLE_OSCILLATION"
        evidence.append(f"Recent flip rate is {z:.2f} standard deviations above baseline — elevated but not strongly significant.")
    else:
        classification = "NO_PATTERN"
        if z is not None and z < WAVE_Z_POSSIBLE_OSCILLATION:
            counter_evidence.append(f"Recent flip rate ({recent_stats['flip_rate']:.2f}/day) is not meaningfully different from baseline ({baseline_p:.2f}/day), z={z:.2f}.")
        if not magnitude_meaningful and recent_avg_abs is not None and baseline_std:
            counter_evidence.append(f"Average |daily spread| on recent flips ({recent_avg_abs:.4f}) is below the noise floor ({WAVE_MAGNITUDE_FLOOR_STD_MULTIPLE * baseline_std:.4f}) — flips are too small to call a real pattern.")

    # Robustness check: does the pattern survive removing the single
    # largest-magnitude day in the recent window? If not, it's flagged and
    # downgraded — a real pattern should not depend on one outlier session.
    single_day_dependent = False
    if classification in ("REPEATED_ROTATION", "POSSIBLE_OSCILLATION") and len(recent_window) > 2:
        outlier_idx = recent_window.abs().idxmax()
        trimmed = recent_window.drop(index=outlier_idx)
        trimmed_stats = _spread_stats(trimmed)
        z_trimmed = _binomial_z(trimmed_stats["flip_count"], trimmed_stats["n_interior"], baseline_p)

        if z_trimmed is not None and z is not None:
            threshold = WAVE_Z_REPEATED_ROTATION if classification == "REPEATED_ROTATION" else WAVE_Z_POSSIBLE_OSCILLATION
            if z_trimmed < threshold:
                single_day_dependent = True
                counter_evidence.append(
                    f"Removing the single largest-magnitude session ({outlier_idx.strftime('%Y-%m-%d')}) drops the "
                    f"z-score from {z:.2f} to {z_trimmed:.2f}, below the threshold for this classification — "
                    f"the pattern leans heavily on one session."
                )
                if classification == "REPEATED_ROTATION":
                    classification = "POSSIBLE_OSCILLATION"
                    evidence.append("Downgraded from REPEATED_ROTATION due to single-session dependency (see counter-evidence).")
                else:
                    classification = "NO_PATTERN"
                    evidence = []

    return {
        "pair": pair_label,
        "classification": classification,
        "z_score": round(z, 2) if z is not None else None,
        "single_day_dependent": single_day_dependent,
        "recent": recent_stats,
        "baseline": baseline_stats,
        "evidence": evidence,
        "counter_evidence": counter_evidence,
    }


def all_wave_summaries(price_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Dict]:
    out = {}
    for a, b in config.WAVE_PAIRS:
        df_a = price_dfs.get(a) if price_dfs else None
        df_b = price_dfs.get(b) if price_dfs else None
        out[f"{a}_{b}"] = pair_wave_summary(a, b, df_a=df_a, df_b=df_b)
    return out
