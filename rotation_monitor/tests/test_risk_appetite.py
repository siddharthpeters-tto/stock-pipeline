"""Tests for rotation_monitor.risk_appetite on synthetic universes."""

import pandas as pd
import pytest

from rotation_monitor import config, risk_appetite


def make_df(prices, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=len(prices))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


@pytest.fixture
def universe(monkeypatch):
    u = {
        "SPY": {"name": "SPY", "category": "BENCHMARK", "benchmark": None, "description": "", "enabled": True},
        "QQQ": {"name": "QQQ", "category": "BENCHMARK", "benchmark": "SPY", "description": "", "enabled": True},
        "XBI": {"name": "XBI", "category": "SPECULATIVE", "benchmark": "SPY", "description": "", "enabled": True},
        "UFO": {"name": "UFO", "category": "SPECULATIVE", "benchmark": "QQQ", "description": "", "enabled": True},
        "DRIV": {"name": "DRIV", "category": "SPECULATIVE", "benchmark": "QQQ", "description": "", "enabled": True},
        "KRE": {"name": "KRE", "category": "FINANCIALS", "benchmark": "SPY", "description": "", "enabled": True},
        "SOXX": {"name": "SOXX", "category": "AI_GROWTH", "benchmark": "QQQ", "description": "", "enabled": True},
        "XLY": {"name": "XLY", "category": "CYCLICAL", "benchmark": "SPY", "description": "", "enabled": True},
        "XLP": {"name": "XLP", "category": "DEFENSIVE", "benchmark": "SPY", "description": "", "enabled": True},
        "XLV": {"name": "XLV", "category": "DEFENSIVE", "benchmark": "SPY", "description": "", "enabled": True},
        "XLU": {"name": "XLU", "category": "DEFENSIVE", "benchmark": "SPY", "description": "", "enabled": True},
    }
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: u)
    return u


def _flat_universe(n=40):
    return {sym: make_df([100.0] * n) for sym in
            ["SPY", "QQQ", "XBI", "UFO", "DRIV", "KRE", "SOXX", "XLY", "XLP", "XLV", "XLU"]}


def test_all_flat_gives_mixed_score(universe):
    price_dfs = _flat_universe()
    result = risk_appetite.compute_risk_appetite(price_dfs=price_dfs)
    assert result["score"] == pytest.approx(50.0, abs=1.0)
    assert result["label"] == "MIXED"


def test_speculative_strength_and_defensive_weakness_raises_score(universe):
    n = 60
    price_dfs = _flat_universe(n)
    # speculative/growth names rally hard, defensive names lag
    for sym in ["XBI", "UFO", "DRIV", "KRE", "SOXX", "XLY"]:
        price_dfs[sym] = make_df([100 * (1.01 ** i) for i in range(n)])
    for sym in ["XLP", "XLV", "XLU"]:
        price_dfs[sym] = make_df([100 * (0.995 ** i) for i in range(n)])

    result = risk_appetite.compute_risk_appetite(price_dfs=price_dfs)
    assert result["score"] > 60
    assert result["label"] in ("RISK_ON", "STRONG_RISK_ON")


def test_defensive_strength_lowers_score(universe):
    n = 60
    price_dfs = _flat_universe(n)
    for sym in ["XBI", "UFO", "DRIV", "KRE", "SOXX", "XLY"]:
        price_dfs[sym] = make_df([100 * (0.995 ** i) for i in range(n)])
    for sym in ["XLP", "XLV", "XLU"]:
        price_dfs[sym] = make_df([100 * (1.01 ** i) for i in range(n)])

    result = risk_appetite.compute_risk_appetite(price_dfs=price_dfs)
    assert result["score"] < 40
    assert result["label"] in ("RISK_OFF", "STRONG_RISK_OFF")


def test_label_bands():
    assert risk_appetite.label_for_score(90) == "STRONG_RISK_ON"
    assert risk_appetite.label_for_score(75) == "STRONG_RISK_ON"
    assert risk_appetite.label_for_score(65) == "RISK_ON"
    assert risk_appetite.label_for_score(50) == "MIXED"
    assert risk_appetite.label_for_score(30) == "RISK_OFF"
    assert risk_appetite.label_for_score(10) == "STRONG_RISK_OFF"


def test_pct_to_score_saturates_at_bounds():
    assert risk_appetite._pct_to_score(1.0) == 100.0   # +100% relative return saturates high
    assert risk_appetite._pct_to_score(-1.0) == 0.0    # -100% relative return saturates low
    assert risk_appetite._pct_to_score(None) is None
