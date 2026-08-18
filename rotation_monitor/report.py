"""
rotation_monitor.report — human-readable rotation_report.md (§26).

Every sentence here is templated from the deterministic values already
computed elsewhere in the pipeline (state_builder's output) — nothing is
invented or phrased by an LLM. Language follows the spec's restrained-
wording guidance (§39): describe what the data shows, never "this pattern
will continue" or anything that reads as a trade recommendation.
"""

import os
from typing import Dict, List, Optional

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(_REPO_ROOT, "outputs")
REPORT_PATH = os.path.join(OUTPUT_DIR, "rotation_report.md")

PRIORITY_PAIR_KEY = "IGV_SOXX"


def _pct(v: Optional[float], digits: int = 1) -> str:
    return "DATA_UNAVAILABLE" if v is None else f"{v * 100:+.{digits}f}%"


def _num(v: Optional[float], digits: int = 2) -> str:
    return "DATA_UNAVAILABLE" if v is None else f"{v:.{digits}f}"


def _join_sentences(items: List[str]) -> str:
    """Join short evidence/counter-evidence fragments into one sentence,
    stripping any trailing period each fragment already has so the joined
    result never ends up with a doubled '..'."""
    return "; ".join(item.rstrip(".") for item in items) + "."


def _find_pair(items: List[Dict], pair_key: str) -> Optional[Dict]:
    for item in items:
        if item.get("pair") == pair_key:
            return item
    return None


def _breadth_category_line(state: Dict, category: str, horizon: str = "5D") -> str:
    matrix = state.get("rotation_matrix", {}).get(category, {})
    members = matrix.get("members", [])
    breadth_cat = state.get("breadth", {}).get("by_category", {}).get(category, {})
    pct = breadth_cat.get(f"outperforming_spy_{horizon.lower()}_pct")
    if pct is None or not members:
        return f"- {category}: DATA_UNAVAILABLE"
    n_strengthening = round(pct / 100.0 * len(members))
    return f"- {category}: {n_strengthening}/{len(members)} themes strengthening vs SPY ({horizon})"


def _regime_narrative(state: Dict) -> str:
    regime = state.get("market_regime")
    confidence = state.get("regime_confidence")
    explanation = state.get("regime_explanation", [])
    counter = state.get("regime_counter_evidence", [])

    lines = [f"The current regime reads as **{regime}** (confidence {confidence}/100)."]
    if explanation:
        lines.append("Supporting evidence: " + _join_sentences(explanation[:4]))
    if counter:
        lines.append("Counter-evidence / conditions not met: " + _join_sentences(counter[:4]))
    return " ".join(lines)


def _priority_pair_section(state: Dict) -> List[str]:
    lines = ["## Key Rotation: IGV vs SOXX", ""]

    reversal = _find_pair(state.get("reversals", []), PRIORITY_PAIR_KEY)
    rotation = _find_pair(state.get("rotations", []), PRIORITY_PAIR_KEY)
    corr = state.get("correlations", {}).get("SOXX_IGV", {})

    if reversal:
        lines.append(f"Short-term state: **{reversal['short_term_state']}**")
        lines.append(f"Persistent 20D state: **{reversal['persistent_20d_state']}**")
        if reversal.get("reversal_flag"):
            lines.append("A short-term leadership reversal was detected day-over-day; this does not by itself overwrite the 20D persistent state above.")
        lines.append("")

    if corr:
        d20 = corr.get("20D", {})
        d60 = corr.get("60D", {})
        lines.append(f"20D rolling correlation: {_num(d20.get('current'), 3)} (percentile of trailing 1yr range: {d20.get('percentile_1y')})")
        lines.append(f"60D rolling correlation: {_num(d60.get('current'), 3)} (percentile of trailing 1yr range: {d60.get('percentile_1y')})")
        lines.append("")

    if rotation:
        lines.append(f"Pairwise rotation call: **{rotation['state']}**, confidence {rotation['confidence']}, {rotation['duration_days']} consecutive session(s) of daily leadership.")
        if rotation.get("evidence"):
            lines.append("Evidence: " + _join_sentences(rotation["evidence"]))
        if rotation.get("counter_evidence"):
            lines.append("Counter-evidence: " + _join_sentences(rotation["counter_evidence"]))
        lines.append("")

    backtest = state.get("priority_pair_backtest")
    if backtest:
        lines.append("Historical context (descriptive, not predictive):")
        for label, w in backtest.get("windows", {}).items():
            lines.append(
                f"- {label}: {w['flip_count']} flips ({w.get('flips_per_month')}/month), "
                f"avg run length {w['avg_run_length']} days, classification **{w['classification']}**"
                + (f" (z={w['z_score']})" if w.get("z_score") is not None else "")
            )
        leader = backtest.get("established_leader_longest_window")
        if leader:
            lines.append(f"Over the longest available window, {leader} has established persistent leadership.")
        else:
            lines.append("No single side has established persistent leadership over the longest available window.")

    return lines


def _final_five_questions(state: Dict) -> List[str]:
    lines = ["## Summary — The Five Questions", ""]

    persistent_leaders = state.get("relative_leaders_vs_spy", {}).get("20D", [])
    persistent_laggards = state.get("relative_laggards_vs_spy", {}).get("20D", [])

    lines.append(f"**1. Where is capital currently showing relative strength?** "
                 f"{', '.join(persistent_leaders) if persistent_leaders else 'DATA_UNAVAILABLE'} "
                 f"(20D relative return vs SPY).")

    lines.append(f"**2. Where is relative strength weakening?** "
                 f"{', '.join(persistent_laggards) if persistent_laggards else 'DATA_UNAVAILABLE'} "
                 f"(20D relative return vs SPY).")

    reversal_count = sum(1 for r in state.get("reversals", []) if r.get("reversal_flag"))
    lines.append(f"**3. Is the move short-term or persistent?** "
                 f"{reversal_count} of the tracked pairs flipped short-term leadership day-over-day; "
                 f"the persistent 20D state is reported separately for each pair above and should be read "
                 f"alongside, not replaced by, the short-term reading.")

    lines.append(f"**4. What market regime best describes the current environment?** "
                 f"{state.get('market_regime')} (confidence {state.get('regime_confidence')}/100).")

    wave_classes = [w.get("classification") for w in state.get("wave_detection", {}).values()]
    repeatable = [c for c in wave_classes if c in ("POSSIBLE_OSCILLATION", "REPEATED_ROTATION")]
    one_sided = [c for c in wave_classes if c == "PERSISTENT_ONE_SIDE_LEADERSHIP"]
    if repeatable:
        q5 = f"{len(repeatable)} tracked pair(s) currently show statistically elevated flip activity relative to their own baseline."
    elif one_sided:
        q5 = f"{len(one_sided)} tracked pair(s) show persistent one-side leadership rather than oscillation."
    else:
        q5 = "No tracked pair currently shows flip activity that is statistically distinguishable from its own historical baseline."
    lines.append(f"**5. Is there evidence of repeatable rotation or oscillation between major themes?** {q5}")

    return lines


def generate_report_markdown(state: Dict) -> str:
    lines: List[str] = []
    lines.append("# Market Rotation Report")
    lines.append("")
    lines.append(f"Date: {state.get('date')}  ")
    lines.append(f"Session status: {state.get('session_status')}  ")
    lines.append(f"Generated: {state.get('generated_at_utc')}")
    lines.append("")

    lines.append("## Current Regime")
    lines.append("")
    lines.append(state.get("market_regime", "DATA_UNAVAILABLE"))
    lines.append("")
    lines.append(f"Confidence: {state.get('regime_confidence')}/100")
    lines.append("")
    lines.append(_regime_narrative(state))
    lines.append("")

    lines.append("## Risk Appetite")
    lines.append("")
    lines.append(f"Score: {state.get('risk_appetite_score')}/100 — **{state.get('risk_appetite_label')}**")
    lines.append("")

    lines.append("## Short-Term Leadership (1D)")
    lines.append("")
    for i, sym in enumerate(state.get("leaders", {}).get("1D", []), 1):
        lines.append(f"{i}. {sym}")
    lines.append("")

    lines.append("## Persistent Leadership (20D, relative to SPY)")
    lines.append("")
    for i, sym in enumerate(state.get("relative_leaders_vs_spy", {}).get("20D", []), 1):
        lines.append(f"{i}. {sym}")
    lines.append("")

    lines += _priority_pair_section(state)
    lines.append("")

    lines.append("## Breadth")
    lines.append("")
    for cat in ["AI_GROWTH", "SPECULATIVE", "FINANCIALS", "CYCLICAL", "DEFENSIVE"]:
        lines.append(_breadth_category_line(state, cat))
    overall = state.get("breadth", {}).get("overall", {})
    lines.append("")
    lines.append(f"Overall: {overall.get('outperforming_spy_5d_pct')}% of tracked themes outperforming SPY over 5D; "
                 f"{overall.get('above_ma20_pct')}% above their 20D moving average; "
                 f"{overall.get('positive_persistence_pct')}% with a persistence score of 50 or higher.")
    lines.append("")

    lines.append("## Rotation Matrix")
    lines.append("")
    lines.append("| Category | 1D | 5D | 10D | 20D | Persistence |")
    lines.append("|---|---|---|---|---|---|")
    for cat, row in state.get("rotation_matrix", {}).items():
        cells = " | ".join(row.get(h, {}).get("label", "DATA_UNAVAILABLE") for h in ["1D", "5D", "10D", "20D"])
        lines.append(f"| {cat} | {cells} | {row.get('avg_persistence')} |")
    lines.append("")

    lines.append("## Rotation Alerts")
    lines.append("")
    alerts = state.get("alerts", [])
    if not alerts:
        lines.append("No alerts met the confirmation threshold for this session.")
    else:
        for a in alerts:
            lines.append(f"- **{a['type']}** — {a['message']}")
    lines.append("")

    lines.append("## Leadership Timeline (last 20 sessions)")
    lines.append("")
    lines.append("| Date | #1 | #2 | #3 | Weakest | Dominant Category |")
    lines.append("|---|---|---|---|---|---|")
    for row in state.get("leadership_timeline", []):
        leaders = row["leaders"] + ["", "", ""]
        lines.append(f"| {row['date']} | {leaders[0]} | {leaders[1]} | {leaders[2]} | {row['weakest']} | {row['dominant_category']} |")
    lines.append("")

    lines += _final_five_questions(state)
    lines.append("")

    lines.append("---")
    lines.append("*This report describes observed relative performance, persistence, breadth, and correlation "
                 "behavior across ETF themes. It does not recommend individual stocks or issue buy/sell signals.*")

    return "\n".join(lines)


def save_report(state: Dict) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    markdown = generate_report_markdown(state)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(markdown)
    return REPORT_PATH
