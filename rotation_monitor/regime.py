"""
rotation_monitor.regime — market regime classification (§14).

Each candidate regime is defined as a fixed list of deterministic
conditions over relative returns/persistence/breadth (never an LLM guess).
The regime whose conditions are most fully met wins, with its match score
(the fraction of its own conditions satisfied) reported as the regime
confidence. If nothing clears REGIME_MIN_CONFIDENCE, the engine falls back
to TRANSITION (when tracked pairs are actively flipping — see
wave_detection) or MIXED_ROTATION (no clear signal at all).
"""

from typing import Dict, List, Optional, Tuple

import pandas as pd

from rotation_monitor import config, persistence
from rotation_monitor.relative_strength import relative_returns
from rotation_monitor.returns import compute_returns, load_price_df
from rotation_monitor.scoring_config import (
    REGIME_MIN_CONFIDENCE,
    REGIME_TRANSITION_FLIP_THRESHOLD,
    SHORT_TERM_HORIZON,
)
from rotation_monitor.wave_detection import all_wave_summaries

BROAD_TECH_GROUP = ["SOXX", "IGV", "CIBR", "SKYY"]
SPECULATIVE_GROUP = ["UFO", "DRIV", "XBI", "KRE"]
CYCLICAL_GROUP = ["XLF", "KRE", "XLI", "XLE", "XLY", "XHB"]
DEFENSIVE_GROUP = ["XLP", "XLV", "XLU"]

Condition = Tuple[bool, Optional[bool], str]  # (evaluable, met, explanation)


def _rel_return_cache(price_dfs: Dict[str, pd.DataFrame], horizon: str) -> Dict[str, Optional[float]]:
    """{symbol: relative return vs its configured benchmark, for `horizon`}."""
    universe = config.load_universe()
    rets = {s: compute_returns(s, df=price_dfs[s]) for s in price_dfs}

    out: Dict[str, Optional[float]] = {}
    for sym in price_dfs:
        bench = universe.get(sym, {}).get("benchmark") or "SPY"
        if bench not in rets or sym not in rets:
            out[sym] = None
            continue
        rel = relative_returns(rets[sym], rets[bench])
        out[sym] = rel.get(horizon)

    return out


def _group_positive_fraction(group: List[str], rel: Dict[str, Optional[float]]) -> Optional[float]:
    vals = [rel.get(s) for s in group if rel.get(s) is not None]
    if not vals:
        return None
    return sum(1 for v in vals if v > 0) / len(vals)


def _score_regime(conditions: List[Condition]) -> Tuple[Optional[float], List[str], List[str]]:
    evaluable = [c for c in conditions if c[0]]
    if not evaluable:
        return None, [], []

    met = [c for c in evaluable if c[1]]
    unmet = [c for c in evaluable if not c[1]]
    score = round(len(met) / len(evaluable) * 100.0, 1)
    return score, [c[2] for c in met], [c[2] for c in unmet]


def _build_regime_defs(rel5: Dict, persistence_scores: Dict) -> Dict[str, List[Condition]]:
    def pscore(sym):
        v = persistence_scores.get(sym, {}).get("score")
        return v

    soxx, igv = rel5.get("SOXX"), rel5.get("IGV")

    return {
        "AI_HARDWARE_LEADERSHIP": [
            (soxx is not None, soxx is not None and soxx > 0, "SOXX relative return vs its benchmark is positive over 5D"),
            (soxx is not None and igv is not None, soxx is not None and igv is not None and soxx > igv, "SOXX outperforming IGV over 5D"),
            (pscore("SOXX") is not None, pscore("SOXX") is not None and pscore("SOXX") >= 60, "SOXX persistence score >= 60 (persistent, not one-day)"),
        ],
        "SOFTWARE_LEADERSHIP": [
            (igv is not None, igv is not None and igv > 0, "IGV relative return vs its benchmark is positive over 5D"),
            (soxx is not None and igv is not None, soxx is not None and igv is not None and igv > soxx, "IGV outperforming SOXX over 5D"),
            (pscore("IGV") is not None, pscore("IGV") is not None and pscore("IGV") >= 60, "IGV persistence score >= 60 (persistent, not one-day)"),
        ],
        "BROAD_TECH_RISK_ON": [
            (rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) > 0, f"{s} relative return positive over 5D")
            for s in BROAD_TECH_GROUP
        ],
        "SPECULATIVE_RISK_ON": [
            (rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) > 0, f"{s} relative return positive over 5D")
            for s in SPECULATIVE_GROUP
        ],
        "CYCLICAL_ROTATION": (
            [(rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) > 0, f"{s} (cyclical) relative return positive over 5D")
             for s in CYCLICAL_GROUP]
            + [(rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) < 0, f"{s} (tech) relative return negative over 5D")
               for s in BROAD_TECH_GROUP]
        ),
        "DEFENSIVE_ROTATION": (
            [(rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) > 0, f"{s} (defensive) relative return positive over 5D")
             for s in DEFENSIVE_GROUP]
            + [(rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) < 0, f"{s} (growth) relative return negative over 5D")
               for s in BROAD_TECH_GROUP]
        ),
        "BROAD_RISK_OFF": (
            [(rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) < 0, f"{s} (speculative) relative return negative over 5D")
             for s in SPECULATIVE_GROUP]
            + [(rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) < 0, f"{s} (growth) relative return negative over 5D")
               for s in BROAD_TECH_GROUP]
            + [(rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) < 0, f"{s} (cyclical) relative return negative over 5D")
               for s in CYCLICAL_GROUP]
            + [(rel5.get(s) is not None, rel5.get(s) is not None and rel5.get(s) > 0, f"{s} (defensive) relative return positive over 5D")
               for s in DEFENSIVE_GROUP]
        ),
    }


def compute_regime(
    price_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    persistence_scores: Optional[Dict] = None,
) -> Dict:
    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in config.universe_tickers()}

    if persistence_scores is None:
        persistence_scores = persistence.compute_persistence_scores(price_dfs=price_dfs)

    rel5 = _rel_return_cache(price_dfs, SHORT_TERM_HORIZON)
    regime_defs = _build_regime_defs(rel5, persistence_scores)

    scored = {}
    for name, conditions in regime_defs.items():
        score, matched, unmatched = _score_regime(conditions)
        scored[name] = {"score": score, "matched": matched, "unmatched": unmatched}

    ranked = sorted(
        ((name, v) for name, v in scored.items() if v["score"] is not None),
        key=lambda kv: kv[1]["score"], reverse=True,
    )

    if not ranked:
        return {
            "regime": "MIXED_ROTATION",
            "confidence": None,
            "explanation": ["Not enough data to evaluate any regime definition."],
            "candidates": scored,
        }

    top_name, top = ranked[0]

    if top["score"] >= REGIME_MIN_CONFIDENCE:
        return {
            "regime": top_name,
            "confidence": top["score"],
            "explanation": top["matched"],
            "counter_evidence": top["unmatched"],
            "candidates": scored,
        }

    # No regime clearly established — decide between TRANSITION and
    # MIXED_ROTATION using how much active flip/oscillation behavior the
    # wave-detection engine is currently seeing across tracked pairs.
    waves = all_wave_summaries(price_dfs=price_dfs)
    active_flip_count = sum(
        1 for w in waves.values() if w.get("classification") in ("POSSIBLE_OSCILLATION", "REPEATED_ROTATION")
    )

    if active_flip_count >= REGIME_TRANSITION_FLIP_THRESHOLD:
        return {
            "regime": "TRANSITION",
            "confidence": top["score"],
            "explanation": [f"{active_flip_count} tracked pair(s) currently show oscillating leadership (see wave_detection)."],
            "counter_evidence": [f"Best-matching regime ({top_name}) only reached {top['score']}% of its conditions."],
            "candidates": scored,
        }

    return {
        "regime": "MIXED_ROTATION",
        "confidence": top["score"],
        "explanation": [f"No regime reached the {REGIME_MIN_CONFIDENCE}% confidence bar; closest was {top_name} at {top['score']}%."],
        "counter_evidence": top["unmatched"],
        "candidates": scored,
    }
