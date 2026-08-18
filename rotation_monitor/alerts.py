"""
rotation_monitor.alerts — informational rotation alerts (§29).

Every alert here is derived from thresholds in scoring_config.ALERT_THRESHOLDS
and from other modules' deterministic output — never invented, and never a
BUY/SELL recommendation. Stronger alerts (a named rotation state, rather
than "developing") require both a confidence bar and a minimum duration,
per the spec's "require persistence or multiple confirmations" instruction.
"""

from typing import Dict, List, Optional

import pandas as pd

from rotation_monitor import breadth as breadth_mod
from rotation_monitor import config, correlations, regime as regime_mod
from rotation_monitor import reversal as reversal_mod
from rotation_monitor import risk_appetite as risk_appetite_mod
from rotation_monitor import rotation_detect
from rotation_monitor.returns import load_price_df
from rotation_monitor.regime import CYCLICAL_GROUP, DEFENSIVE_GROUP
from rotation_monitor.scoring_config import ALERT_THRESHOLDS


def _trim_price_dfs(price_dfs: Dict[str, pd.DataFrame], n_days_back: int) -> Optional[Dict[str, pd.DataFrame]]:
    spy_dates = price_dfs["SPY"]["adj_close"].dropna().index
    if len(spy_dates) < n_days_back + 1:
        return None
    cutoff = spy_dates[-1 - n_days_back]
    return {sym: df[df.index <= cutoff] for sym, df in price_dfs.items()}


def _risk_appetite_alerts(price_dfs: Dict[str, pd.DataFrame]) -> List[Dict]:
    lookback = ALERT_THRESHOLDS["trend_lookback_days"]
    threshold = ALERT_THRESHOLDS["risk_appetite_trend_pts"]

    now = risk_appetite_mod.compute_risk_appetite(price_dfs=price_dfs)
    prior_dfs = _trim_price_dfs(price_dfs, lookback)
    if prior_dfs is None or now.get("score") is None:
        return []
    prior = risk_appetite_mod.compute_risk_appetite(price_dfs=prior_dfs)
    if prior.get("score") is None:
        return []

    delta = now["score"] - prior["score"]
    if delta >= threshold:
        return [{"type": "RISK_APPETITE_EXPANDING", "message": f"Risk Appetite Score rose {delta:+.1f} pts over the last {lookback} sessions ({prior['score']} -> {now['score']})."}]
    if delta <= -threshold:
        return [{"type": "RISK_APPETITE_CONTRACTING", "message": f"Risk Appetite Score fell {delta:+.1f} pts over the last {lookback} sessions ({prior['score']} -> {now['score']})."}]
    return []


def _breadth_alerts(price_dfs: Dict[str, pd.DataFrame]) -> List[Dict]:
    lookback = ALERT_THRESHOLDS["trend_lookback_days"]
    threshold = ALERT_THRESHOLDS["breadth_trend_pts"]
    out = []

    cyclical_trend = breadth_mod.breadth_trend(CYCLICAL_GROUP, 5, lookback, price_dfs)
    if cyclical_trend is not None and cyclical_trend >= threshold:
        out.append({"type": "CYCLICAL_BREADTH_EXPANDING", "message": f"Cyclical breadth (5D-positive-vs-SPY) rose {cyclical_trend:+.1f}pp over {lookback} sessions."})

    defensive_trend = breadth_mod.breadth_trend(DEFENSIVE_GROUP, 5, lookback, price_dfs)
    if defensive_trend is not None and defensive_trend >= threshold:
        out.append({"type": "DEFENSIVE_BREADTH_EXPANDING", "message": f"Defensive breadth (5D-positive-vs-SPY) rose {defensive_trend:+.1f}pp over {lookback} sessions."})

    return out


def _correlation_alerts(price_dfs: Dict[str, pd.DataFrame]) -> List[Dict]:
    out = []
    threshold = ALERT_THRESHOLDS["correlation_breakdown_percentile"]
    for pair_key, summary in correlations.all_correlation_summaries(price_dfs=price_dfs).items():
        pct = summary.get("20D", {}).get("percentile_1y")
        if pct is not None and pct <= threshold:
            out.append({
                "type": "CORRELATION_BREAKDOWN",
                "message": f"{pair_key.replace('_', '/')} 20D correlation ({summary['20D']['current']}) is at the {pct}th percentile of its trailing 1yr range — unusually decoupled.",
            })
    return out


def _rotation_alerts(price_dfs: Dict[str, pd.DataFrame]) -> List[Dict]:
    out = []
    developing_min = ALERT_THRESHOLDS["rotation_developing_min_confidence"]
    established_min = ALERT_THRESHOLDS["rotation_established_min_confidence"]
    established_dur = ALERT_THRESHOLDS["rotation_established_min_duration"]

    for r in rotation_detect.all_pair_rotations(price_dfs=price_dfs):
        if r["state"] == "NEUTRAL":
            continue
        if r["confidence"] >= established_min and r["duration_days"] >= established_dur:
            out.append({"type": r["state"], "message": f"{r['pair'].replace('_', '/')}: {r['state']} — confidence {r['confidence']}, {r['duration_days']} consecutive session(s)."})
        elif r["confidence"] >= developing_min:
            out.append({"type": "ROTATION_DEVELOPING", "message": f"{r['pair'].replace('_', '/')}: developing signs of {r['state']} — confidence {r['confidence']} (not yet established)."})

    return out


def _reversal_alerts(price_dfs: Dict[str, pd.DataFrame]) -> List[Dict]:
    out = []
    for r in reversal_mod.all_pair_reversals(price_dfs=price_dfs):
        if r["reversal_flag"]:
            out.append({"type": "LEADERSHIP_REVERSAL", "message": f"{r['pair'].replace('_', '/')}: short-term leadership flipped day-over-day (persistent 20D state remains {r['persistent_20d_state']})."})
    return out


def _regime_alert(regime_result: Dict) -> List[Dict]:
    if regime_result.get("regime") == "TRANSITION":
        return [{"type": "REGIME_CHANGE_POSSIBLE", "message": "Leadership is actively flipping across multiple tracked pairs; no regime has established confidence yet."}]
    return []


def compute_alerts(
    price_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    regime_result: Optional[Dict] = None,
) -> List[Dict]:
    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in config.universe_tickers()}

    if regime_result is None:
        regime_result = regime_mod.compute_regime(price_dfs=price_dfs)

    alerts: List[Dict] = []
    alerts += _rotation_alerts(price_dfs)
    alerts += _reversal_alerts(price_dfs)
    alerts += _correlation_alerts(price_dfs)
    alerts += _risk_appetite_alerts(price_dfs)
    alerts += _breadth_alerts(price_dfs)
    alerts += _regime_alert(regime_result)

    return alerts
