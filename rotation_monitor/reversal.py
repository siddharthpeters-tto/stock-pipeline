"""
rotation_monitor.reversal — short-term vs persistent state, and
day-to-day leadership-reversal flagging (§20).

A one-day sign flip in daily leadership is reported as
SHORT_TERM_LEADERSHIP_REVERSAL, but it never overwrites the 20D persistent
state — both are always reported side by side, exactly per the spec's
worked example ("Short-Term State: SOFTWARE_OVER_SEMIS" /
"Persistent 20D State: SEMICONDUCTOR_LEADERSHIP").
"""

from typing import Dict, Optional

import numpy as np
import pandas as pd

from rotation_monitor import config
from rotation_monitor.relative_strength import rs_ratio_summary
from rotation_monitor.returns import load_price_df
from rotation_monitor.wave_detection import daily_spread


def detect_pair_reversal(
    symbol_a: str, symbol_b: str, label_a_over_b: str, label_b_over_a: str,
    df_a: Optional[pd.DataFrame] = None, df_b: Optional[pd.DataFrame] = None,
) -> Dict:
    if df_a is None:
        df_a = load_price_df(symbol_a)
    if df_b is None:
        df_b = load_price_df(symbol_b)

    spread = daily_spread(df_a, df_b)  # positive => A led that day
    reversal_flag = False
    short_term_state = "NEUTRAL"

    if len(spread) >= 1:
        today_sign = float(np.sign(spread.iloc[-1]))
        short_term_state = label_a_over_b if today_sign > 0 else (label_b_over_a if today_sign < 0 else "NEUTRAL")

        if len(spread) >= 2:
            yesterday_sign = float(np.sign(spread.iloc[-2]))
            reversal_flag = today_sign != 0 and yesterday_sign != 0 and today_sign != yesterday_sign

    ratio_20d = rs_ratio_summary(symbol_a, symbol_b, window=20, df_a=df_a, df_b=df_b)
    change_20d = ratio_20d.get("change_pct")
    if change_20d is None:
        persistent_state = "DATA_UNAVAILABLE"
    elif change_20d > 0:
        persistent_state = label_a_over_b
    elif change_20d < 0:
        persistent_state = label_b_over_a
    else:
        persistent_state = "NEUTRAL"

    agrees_with_persistent = (
        persistent_state not in ("DATA_UNAVAILABLE", "NEUTRAL")
        and short_term_state == persistent_state
    )

    return {
        "pair": f"{symbol_a}_{symbol_b}",
        "short_term_state": short_term_state,
        "persistent_20d_state": persistent_state,
        "reversal_flag": "SHORT_TERM_LEADERSHIP_REVERSAL" if reversal_flag else None,
        "short_term_agrees_with_persistent": agrees_with_persistent,
    }


def all_pair_reversals(price_dfs: Optional[Dict[str, pd.DataFrame]] = None) -> list:
    out = []
    for spec in config.ROTATION_DETECT_PAIRS:
        a, b = spec["pair"]
        df_a = price_dfs.get(a) if price_dfs else None
        df_b = price_dfs.get(b) if price_dfs else None
        out.append(detect_pair_reversal(a, b, spec["a_over_b"], spec["b_over_a"], df_a=df_a, df_b=df_b))
    return out
