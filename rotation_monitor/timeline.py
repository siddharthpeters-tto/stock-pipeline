"""
rotation_monitor.timeline — daily leadership timeline (§19).

Reconstructs the #1/#2/#3 relative-return leader and the weakest theme for
each of the last N trading days directly from cached price history (same
technique as persistence.py), so the timeline is available immediately
rather than only after N days of accumulated runs.

"Dominant category" is defined deterministically as whichever category (or
categories, tied) appears most often among that day's top-3 leaders — the
spec's own worked example doesn't state an exact rule, so this one is
spelled out here rather than left implicit.
"""

from typing import Dict, List, Optional

import pandas as pd

from rotation_monitor import breadth as breadth_mod
from rotation_monitor import config, persistence
from rotation_monitor.returns import load_price_df


def _category_lookup() -> Dict[str, str]:
    universe = config.load_universe()
    return {sym: cfg["category"] for sym, cfg in universe.items()}


def _dominant_category(top_symbols: List[str], cat_lookup: Dict[str, str]) -> str:
    cats = [cat_lookup.get(s) for s in top_symbols if cat_lookup.get(s)]
    if not cats:
        return "DATA_UNAVAILABLE"
    counts: Dict[str, int] = {}
    for c in cats:
        counts[c] = counts.get(c, 0) + 1
    max_count = max(counts.values())
    winners = sorted([c for c, n in counts.items() if n == max_count])
    return " / ".join(winners)


def compute_leadership_timeline(
    days: int = 20, price_dfs: Optional[Dict[str, pd.DataFrame]] = None
) -> List[Dict]:
    symbols = config.non_benchmark_tickers()

    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in symbols + ["SPY"]}

    matrix = persistence.build_rolling_relative_return_matrix(price_dfs, symbols, horizon=1)
    if matrix.empty:
        return []

    recent = matrix.tail(days)
    cat_lookup = _category_lookup()

    timeline = []
    for date, row in recent.iterrows():
        ranked = row.dropna().sort_values(ascending=False)
        if ranked.empty:
            continue

        leaders = list(ranked.index[:3])
        weakest = ranked.index[-1]

        timeline.append({
            "date": date.strftime("%Y-%m-%d"),
            "leaders": leaders,
            "leader_values": {s: round(float(ranked[s]), 4) for s in leaders},
            "weakest": weakest,
            "weakest_value": round(float(ranked[weakest]), 4),
            "dominant_category": _dominant_category(leaders, cat_lookup),
        })

    return timeline
