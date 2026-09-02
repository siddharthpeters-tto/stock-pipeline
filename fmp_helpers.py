import math
from typing import Any, Dict, Iterable, List, Optional


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if math.isnan(float(value)) or math.isinf(float(value)):
            return None
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"nan", "inf", "infinity", "-inf", "-infinity"}:
            return None
        try:
            num = float(text)
        except ValueError:
            return None
        if math.isnan(num) or math.isinf(num):
            return None
        return num
    return None


def safe_ratio(numerator: Any, denominator: Any) -> Optional[float]:
    num = safe_float(numerator)
    den = safe_float(denominator)
    if num is None or den is None or den == 0:
        return None
    return num / den


def normalize_fmp_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        rows = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            clean = {}
            for key, value in item.items():
                if value is None:
                    continue
                if isinstance(value, str):
                    value = value.strip()
                    if not value or value.lower() in {"nan", "null", "none"}:
                        continue
                if isinstance(value, (float, int)) and not math.isfinite(float(value)):
                    continue
                clean[key] = value
            if clean:
                rows.append(clean)
        return rows

    if isinstance(payload, dict):
        for key in ("data", "results", "values"):
            if key in payload and isinstance(payload[key], list):
                return normalize_fmp_rows(payload[key])
    
    return []


def normalize_symbol(raw: Any) -> Optional[str]:
    symbol = raw
    if isinstance(symbol, str):
        symbol = symbol.strip().upper()
    else:
        return None
    if not symbol or symbol in {"N/A", "NULL", "NONE", "NaN"}:
        return None
    return symbol


def first_numeric(mapping: Optional[Dict[str, Any]], keys: Iterable[str]) -> Optional[float]:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        val = safe_float(mapping.get(key))
        if val is not None:
            return val
    return None
