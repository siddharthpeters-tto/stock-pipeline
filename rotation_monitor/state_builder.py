"""
rotation_monitor.state_builder — assembles rotation_state.json (§23).

This is the single point where every other module's output is gathered
into one snapshot. Nothing is computed here that isn't already computed
deterministically elsewhere — this module only assembles and shapes it for
storage/serving.
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd

from rotation_monitor import (
    alerts as alerts_mod,
    backtest as backtest_mod,
    breadth as breadth_mod,
    config,
    correlations,
    persistence,
    regime as regime_mod,
    relative_strength,
    returns,
    reversal as reversal_mod,
    risk_appetite as risk_appetite_mod,
    rotation_detect,
    rotation_matrix as rotation_matrix_mod,
    timeline as timeline_mod,
    wave_detection,
)

PRIORITY_PAIR = ("IGV", "SOXX")  # highest-priority relationship per the spec
from rotation_monitor.rankings import RANK_HORIZONS, leaders_and_laggards, rank_by_absolute_return, rank_by_relative_return
from rotation_monitor.returns import load_price_df

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(_REPO_ROOT, "outputs")
STATE_PATH = os.path.join(OUTPUT_DIR, "rotation_state.json")


def _load_all_price_dfs() -> Dict[str, pd.DataFrame]:
    return {s: load_price_df(s) for s in config.universe_tickers()}


def _session_status(price_dfs: Dict[str, pd.DataFrame]) -> Dict:
    """§36 — distinguish INTRADAY from FINAL_CLOSE. Since this module only
    ever reads completed daily bars from the cache (data_loader.py pulls
    end-of-day data), every date it sees is a completed session; there is
    no separate intraday feed. This is stated explicitly rather than
    silently assumed."""
    spy_dates = price_dfs.get("SPY")
    latest = returns.latest_close_date("SPY", df=spy_dates) if spy_dates is not None else None
    return {
        "status": "FINAL_CLOSE" if latest else "DATA_UNAVAILABLE",
        "as_of_date": latest,
        "note": "Data source is end-of-day only; there is no intraday feed in this pipeline.",
    }


def build_rotation_state(price_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> Dict:
    if price_dfs is None:
        price_dfs = _load_all_price_dfs()

    universe_returns = {s: returns.compute_returns(s, df=df) for s, df in price_dfs.items()}
    rel_returns = relative_strength.compute_relative_returns_for_universe(universe_returns)

    abs_rank = rank_by_absolute_return(universe_returns)
    rel_rank_spy = rank_by_relative_return(rel_returns, vs="vs_SPY")
    abs_leaders_laggards = leaders_and_laggards(abs_rank, top_n=3)
    rel_leaders_laggards = leaders_and_laggards(rel_rank_spy, top_n=3)

    persistence_scores = persistence.compute_persistence_scores(price_dfs=price_dfs)
    breadth_result = breadth_mod.compute_breadth(price_dfs=price_dfs)
    risk_appetite_result = risk_appetite_mod.compute_risk_appetite(price_dfs=price_dfs)
    regime_result = regime_mod.compute_regime(price_dfs=price_dfs, persistence_scores=persistence_scores)
    rotation_flags = rotation_detect.all_pair_rotations(price_dfs=price_dfs)
    reversals = reversal_mod.all_pair_reversals(price_dfs=price_dfs)
    wave_summaries = wave_detection.all_wave_summaries(price_dfs=price_dfs)
    matrix = rotation_matrix_mod.compute_rotation_matrix(price_dfs=price_dfs, persistence_scores=persistence_scores)
    rs_ratios = relative_strength.all_rs_ratio_summaries(window=20)
    corr_summaries = correlations.all_correlation_summaries(price_dfs=price_dfs)
    leadership_timeline = timeline_mod.compute_leadership_timeline(days=20, price_dfs=price_dfs)
    alert_list = alerts_mod.compute_alerts(price_dfs=price_dfs, regime_result=regime_result)
    priority_backtest = backtest_mod.analyze_pair(
        *PRIORITY_PAIR, df_a=price_dfs.get(PRIORITY_PAIR[0]), df_b=price_dfs.get(PRIORITY_PAIR[1])
    )

    session = _session_status(price_dfs)

    state = {
        "date": session["as_of_date"],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "session_status": session["status"],
        "session_note": session["note"],

        "market_regime": regime_result["regime"],
        "regime_confidence": regime_result["confidence"],
        "regime_explanation": regime_result.get("explanation", []),
        "regime_counter_evidence": regime_result.get("counter_evidence", []),

        "risk_appetite_score": risk_appetite_result["score"],
        "risk_appetite_label": risk_appetite_result["label"],
        "risk_appetite_components": risk_appetite_result["components"],

        "leaders": {h: abs_leaders_laggards[h]["leaders"] for h in RANK_HORIZONS},
        "laggards": {h: abs_leaders_laggards[h]["laggards"] for h in RANK_HORIZONS},
        "relative_leaders_vs_spy": {h: rel_leaders_laggards[h]["leaders"] for h in RANK_HORIZONS},
        "relative_laggards_vs_spy": {h: rel_leaders_laggards[h]["laggards"] for h in RANK_HORIZONS},

        "persistence_scores": persistence_scores,
        "breadth": breadth_result,
        "rotation_matrix": matrix,

        "rotations": rotation_flags,
        "reversals": reversals,
        "wave_detection": wave_summaries,
        "rs_ratios": rs_ratios,
        "correlations": corr_summaries,

        "leadership_timeline": leadership_timeline,
        "alerts": alert_list,

        "priority_pair_backtest": priority_backtest,

        "universe": config.load_universe(),
    }

    return state


def save_state(state: Optional[Dict] = None) -> str:
    if state is None:
        state = build_rotation_state()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)

    return STATE_PATH
