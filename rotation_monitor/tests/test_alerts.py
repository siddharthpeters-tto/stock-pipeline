"""
Tests for rotation_monitor.alerts — checks that each alert type fires under
an engineered scenario built to trigger it, and stays quiet otherwise.
"""

import pandas as pd
import pytest

from rotation_monitor import alerts, config


def make_df(prices, start="2023-01-01"):
    dates = pd.bdate_range(start=start, periods=len(prices))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


FULL_UNIVERSE = {
    "SPY": {"name": "SPY", "category": "BENCHMARK", "benchmark": None, "enabled": True, "description": ""},
    "QQQ": {"name": "QQQ", "category": "BENCHMARK", "benchmark": "SPY", "enabled": True, "description": ""},
    "SOXX": {"name": "SOXX", "category": "AI_GROWTH", "benchmark": "QQQ", "enabled": True, "description": ""},
    "IGV": {"name": "IGV", "category": "AI_GROWTH", "benchmark": "QQQ", "enabled": True, "description": ""},
    "CIBR": {"name": "CIBR", "category": "AI_GROWTH", "benchmark": "QQQ", "enabled": True, "description": ""},
    "SKYY": {"name": "SKYY", "category": "AI_GROWTH", "benchmark": "QQQ", "enabled": True, "description": ""},
    "UFO": {"name": "UFO", "category": "SPECULATIVE", "benchmark": "QQQ", "enabled": True, "description": ""},
    "DRIV": {"name": "DRIV", "category": "SPECULATIVE", "benchmark": "QQQ", "enabled": True, "description": ""},
    "XBI": {"name": "XBI", "category": "SPECULATIVE", "benchmark": "SPY", "enabled": True, "description": ""},
    "KRE": {"name": "KRE", "category": "FINANCIALS", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLF": {"name": "XLF", "category": "FINANCIALS", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLI": {"name": "XLI", "category": "CYCLICAL", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLE": {"name": "XLE", "category": "CYCLICAL", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLY": {"name": "XLY", "category": "CYCLICAL", "benchmark": "SPY", "enabled": True, "description": ""},
    "XHB": {"name": "XHB", "category": "CYCLICAL", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLV": {"name": "XLV", "category": "DEFENSIVE", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLP": {"name": "XLP", "category": "DEFENSIVE", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLU": {"name": "XLU", "category": "DEFENSIVE", "benchmark": "SPY", "enabled": True, "description": ""},
}


@pytest.fixture
def universe(monkeypatch):
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: FULL_UNIVERSE)
    monkeypatch.setattr(config, "non_benchmark_tickers", lambda enabled_only=True: [s for s in FULL_UNIVERSE if s not in ("SPY", "QQQ")])
    monkeypatch.setattr(config, "universe_tickers", lambda enabled_only=True: list(FULL_UNIVERSE.keys()))
    monkeypatch.setattr(config, "get_benchmark", lambda sym, universe=None: FULL_UNIVERSE.get(sym, {}).get("benchmark"))
    return FULL_UNIVERSE


def _all_flat(n=120):
    return {sym: make_df([100.0] * n) for sym in FULL_UNIVERSE}


def test_no_alerts_when_everything_is_flat(universe):
    price_dfs = _all_flat()
    result = alerts.compute_alerts(price_dfs=price_dfs)
    assert result == []


def test_defensive_breadth_expanding_alert_fires(universe):
    n = 100
    price_dfs = _all_flat(n)
    # defensive names flip from underperforming to strongly outperforming
    # only in roughly the last week -- 5D breadth trend should jump
    for sym in ["XLP", "XLV", "XLU"]:
        prices = [100 * (0.995 ** i) for i in range(n - 5)]
        prices += [prices[-1] * (1.03 ** i) for i in range(1, 6)]
        price_dfs[sym] = make_df(prices)

    result = alerts.compute_alerts(price_dfs=price_dfs)
    types = {a["type"] for a in result}
    assert "DEFENSIVE_BREADTH_EXPANDING" in types


def test_rotation_developing_alert_fires_for_clear_pair_move(universe):
    n = 60
    price_dfs = _all_flat(n)
    price_dfs["IGV"] = make_df([100 * (1.02 ** i) for i in range(n)])
    price_dfs["SOXX"] = make_df([100 * (0.99 ** i) for i in range(n)])

    result = alerts.compute_alerts(price_dfs=price_dfs)
    types = {a["type"] for a in result}
    assert types & {"ROTATION_DEVELOPING", "SOFTWARE_OVER_SEMIS"}


def test_alerts_never_contain_buy_sell_language(universe):
    n = 100
    price_dfs = _all_flat(n)
    for sym in ["XLP", "XLV", "XLU"]:
        price_dfs[sym] = make_df([100 * (1.02 ** i) for i in range(n)])
    for sym in ["SOXX", "IGV", "CIBR", "SKYY", "UFO", "DRIV", "XBI", "KRE"]:
        price_dfs[sym] = make_df([100 * (0.99 ** i) for i in range(n)])

    result = alerts.compute_alerts(price_dfs=price_dfs)
    banned = ["buy", "sell", "long", "short the", "price target"]
    for a in result:
        msg_lower = a["message"].lower()
        for word in banned:
            assert word not in msg_lower
