"""
rotation_monitor.volume — trading-volume confirmation (§9).

Deliberately called "trading-volume confirmation", not fund flow: ETF share
volume is not the same thing as creation/redemption or AUM flow data, and
this module never claims otherwise.
"""

from typing import Dict, Optional

import pandas as pd

from rotation_monitor.returns import load_price_df
from rotation_monitor.scoring_config import VOLUME_THRESHOLDS


def classify_volume_ratio(ratio: Optional[float]) -> str:
    if ratio is None:
        return "DATA_UNAVAILABLE"
    t = VOLUME_THRESHOLDS
    if ratio >= t["very_elevated"]:
        return "VERY_ELEVATED"
    if ratio >= t["elevated"]:
        return "ELEVATED"
    if ratio < t["low"]:
        return "LOW"
    return "NORMAL"


def compute_volume(symbol: str, df: Optional[pd.DataFrame] = None) -> Dict:
    if df is None:
        df = load_price_df(symbol)

    vol = df["volume"].dropna()
    if vol.empty:
        return {"current_volume": None, "avg_volume_5d": None, "avg_volume_20d": None,
                "volume_ratio": None, "classification": "DATA_UNAVAILABLE"}

    current = float(vol.iloc[-1])
    avg5 = float(vol.tail(5).mean()) if len(vol) >= 5 else None
    avg20 = float(vol.tail(20).mean()) if len(vol) >= 20 else None

    ratio = None
    if avg20 and avg20 > 0:
        ratio = current / avg20

    return {
        "current_volume": current,
        "avg_volume_5d": avg5,
        "avg_volume_20d": avg20,
        "volume_ratio": round(ratio, 3) if ratio is not None else None,
        "classification": classify_volume_ratio(ratio),
    }
