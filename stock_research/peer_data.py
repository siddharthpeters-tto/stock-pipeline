from typing import Any, List, Optional

from fmp_helpers import normalize_symbol


def peer_symbols_from_list(peers: List[Any], exclude_symbol: Optional[str] = None, max_peers: int = 8):
    cleaned = []
    for item in peers or []:
        if isinstance(item, dict):
            candidate = item.get("symbol")
        elif isinstance(item, str):
            candidate = item
        else:
            candidate = None
        symbol_value = normalize_symbol(candidate)
        if not symbol_value:
            continue
        if exclude_symbol and symbol_value.upper() == exclude_symbol.upper():
            continue
        cleaned.append(symbol_value)
    return cleaned[:max_peers]
