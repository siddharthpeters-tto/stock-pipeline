"""
rotation_monitor.heatmap — monthly heatmap data (§17).

Produces the numeric matrix (theme rows x trading-day columns) that
charts.py renders as a PNG, and that verify/report code can also print as a
plain table — the spec explicitly wants actual numeric values, not just
colors.
"""

from typing import Dict, List, Optional

import pandas as pd

from rotation_monitor import config
from rotation_monitor.returns import load_price_df

MODES = ("absolute", "relative_spy", "relative_qqq")


def _daily_returns(df: pd.DataFrame) -> pd.Series:
    return df["adj_close"].dropna().pct_change().dropna()


def compute_monthly_heatmap(
    mode: str = "relative_spy",
    price_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> Dict:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")

    themes = config.non_benchmark_tickers()

    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in themes + ["SPY", "QQQ"]}

    spy_ret = _daily_returns(price_dfs["SPY"])
    qqq_ret = _daily_returns(price_dfs["QQQ"])

    # anchor the month on SPY's latest available date unless overridden
    latest_date = spy_ret.index.max()
    year = year or latest_date.year
    month = month or latest_date.month

    def in_month(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
        return idx[(idx.year == year) & (idx.month == month)]

    all_dates = sorted(set(in_month(spy_ret.index)))

    rows = []
    for sym in themes:
        df = price_dfs.get(sym)
        if df is None:
            continue
        ret = _daily_returns(df)
        ret = ret[ret.index.isin(all_dates)]

        if mode == "relative_spy":
            ret = ret - spy_ret.reindex(ret.index)
        elif mode == "relative_qqq":
            ret = ret - qqq_ret.reindex(ret.index)

        rows.append((sym, ret))

    date_labels = [d.strftime("%Y-%m-%d") for d in all_dates]

    values: List[List[Optional[float]]] = []
    themes_out: List[str] = []
    for sym, ret in rows:
        row_vals = [round(float(ret[d]) * 100.0, 3) if d in ret.index and pd.notna(ret[d]) else None for d in all_dates]
        values.append(row_vals)
        themes_out.append(sym)

    return {
        "mode": mode,
        "year": year,
        "month": month,
        "dates": date_labels,
        "themes": themes_out,
        "values_pct": values,  # percentage points, e.g. 1.5 == +1.5%
    }
