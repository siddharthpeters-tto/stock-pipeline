"""
rotation_monitor.rankings — daily leadership rankings (§11).

Ranks the theme universe (SPY/QQQ excluded — they're benchmarks, not
themes) by absolute return and by return relative to SPY/QQQ, across
1D/5D/10D/20D. Kept as plain sorted lists rather than a "top N" cut so
callers can decide how many leaders/laggards to show.
"""

from typing import Dict, List, Optional

from rotation_monitor import config

RANK_HORIZONS = ["1D", "5D", "10D", "20D"]


def _rank(values: Dict[str, Optional[float]], descending: bool = True) -> List[Dict]:
    """
    values: {symbol: value_or_None}. Returns rank-ordered list of
    {symbol, value, rank}, skipping symbols with no value (DATA_UNAVAILABLE).
    """
    available = [(sym, v) for sym, v in values.items() if v is not None]
    available.sort(key=lambda kv: kv[1], reverse=descending)

    return [
        {"symbol": sym, "value": round(v, 5), "rank": i + 1}
        for i, (sym, v) in enumerate(available)
    ]


def rank_by_absolute_return(
    universe_returns: Dict[str, Dict[str, Optional[float]]]
) -> Dict[str, List[Dict]]:
    themes = config.non_benchmark_tickers()
    out = {}
    for h in RANK_HORIZONS:
        values = {sym: universe_returns.get(sym, {}).get(h) for sym in themes}
        out[h] = _rank(values)
    return out


def rank_by_relative_return(
    relative_returns_by_symbol: Dict[str, Dict[str, Dict[str, Optional[float]]]],
    vs: str = "vs_SPY",
) -> Dict[str, List[Dict]]:
    """
    relative_returns_by_symbol: output of
    relative_strength.compute_relative_returns_for_universe.
    vs: "vs_SPY" or "vs_QQQ".
    """
    themes = config.non_benchmark_tickers()
    out = {}
    for h in RANK_HORIZONS:
        values = {sym: relative_returns_by_symbol.get(sym, {}).get(vs, {}).get(h) for sym in themes}
        out[h] = _rank(values)
    return out


def leaders_and_laggards(ranked: Dict[str, List[Dict]], top_n: int = 3) -> Dict[str, Dict]:
    """Convenience slice: top N leaders / bottom N laggards per horizon."""
    out = {}
    for h, rows in ranked.items():
        out[h] = {
            "leaders": [r["symbol"] for r in rows[:top_n]],
            "laggards": [r["symbol"] for r in rows[-top_n:][::-1]] if rows else [],
        }
    return out
