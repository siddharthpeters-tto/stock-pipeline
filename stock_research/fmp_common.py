import math
from typing import Any, Dict, List, Optional, Sequence


def safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return (a - b) / abs(b)


def to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b


def median(values: Sequence[float]) -> Optional[float]:
    vals = [v for v in values if v is not None and not math.isnan(v)]
    if not vals:
        return None
    vals.sort()
    n = len(vals)
    mid = n // 2
    if n % 2 == 1:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0
