"""
Tests for rotation_monitor.regime — synthetic universes engineered to
clearly satisfy (or clearly fail to satisfy) each regime's conditions.
"""

import pandas as pd
import pytest

from rotation_monitor import config, regime


def make_df(prices, start="2024-01-01"):
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
    monkeypatch.setattr(config, "get_benchmark", lambda sym, universe=None: FULL_UNIVERSE.get(sym, {}).get("benchmark"))
    return FULL_UNIVERSE


def _all_flat(n=80):
    return {sym: make_df([100.0] * n) for sym in FULL_UNIVERSE}


def test_defensive_rotation_detected(universe):
    n = 80
    price_dfs = _all_flat(n)
    for sym in ["XLP", "XLV", "XLU"]:
        price_dfs[sym] = make_df([100 * (1.01 ** i) for i in range(n)])
    for sym in ["SOXX", "IGV", "CIBR", "SKYY"]:
        price_dfs[sym] = make_df([100 * (0.99 ** i) for i in range(n)])

    result = regime.compute_regime(price_dfs=price_dfs)
    assert result["regime"] == "DEFENSIVE_ROTATION"
    assert result["confidence"] >= 55


def test_broad_tech_risk_on_detected(universe):
    n = 80
    price_dfs = _all_flat(n)
    for sym in ["SOXX", "IGV", "CIBR", "SKYY"]:
        price_dfs[sym] = make_df([100 * (1.01 ** i) for i in range(n)])

    result = regime.compute_regime(price_dfs=price_dfs)
    assert result["regime"] == "BROAD_TECH_RISK_ON"


def test_ai_hardware_leadership_over_software(universe):
    n = 80
    price_dfs = _all_flat(n)
    # SOXX beats QQQ AND beats IGV; persistence needs a sustained edge, so
    # give SOXX a strong, steady climb throughout.
    price_dfs["SOXX"] = make_df([100 * (1.015 ** i) for i in range(n)])
    price_dfs["IGV"] = make_df([100 * (1.001 ** i) for i in range(n)])

    result = regime.compute_regime(price_dfs=price_dfs)
    assert result["regime"] == "AI_HARDWARE_LEADERSHIP"


def test_mixed_or_transition_when_nothing_stands_out(universe):
    n = 80
    price_dfs = _all_flat(n)  # nothing moves at all -- no regime should clear the bar
    result = regime.compute_regime(price_dfs=price_dfs)
    assert result["regime"] in ("MIXED_ROTATION", "TRANSITION")


def test_regime_result_always_includes_explanation(universe):
    n = 80
    price_dfs = _all_flat(n)
    result = regime.compute_regime(price_dfs=price_dfs)
    assert "explanation" in result
    assert isinstance(result["explanation"], list)
