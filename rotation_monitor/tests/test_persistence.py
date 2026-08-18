"""
Tests for rotation_monitor.persistence — built entirely on synthetic price
DataFrames (a small universe of made-up symbols + SPY), never live data.
"""

import pandas as pd
import pytest

from rotation_monitor import config, persistence


def make_df(prices, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=len(prices))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


def test_rolling_relative_return_matrix_shape():
    n = 40
    spy = make_df([100 * (1.001 ** i) for i in range(n)])
    strong = make_df([100 * (1.01 ** i) for i in range(n)])   # consistently beats SPY
    weak = make_df([100 * (0.995 ** i) for i in range(n)])    # consistently lags SPY

    price_dfs = {"SPY": spy, "STRONG": strong, "WEAK": weak}
    matrix = persistence.build_rolling_relative_return_matrix(price_dfs, ["STRONG", "WEAK"], horizon=5)

    assert list(matrix.columns) == ["STRONG", "WEAK"]
    # STRONG's relative return vs SPY should be positive throughout its
    # valid (non-NaN) rows, WEAK's should be negative
    assert (matrix["STRONG"].dropna() > 0).all()
    assert (matrix["WEAK"].dropna() < 0).all()


def test_consistent_outperformer_scores_high(monkeypatch):
    n = 80
    spy = make_df([100 * (1.0005 ** i) for i in range(n)])
    strong = make_df([100 * (1.01 ** i) for i in range(n)])  # steady, persistent outperformance

    universe = {
        "SPY": {"name": "SPY", "category": "BENCHMARK", "benchmark": None, "description": "", "enabled": True},
        "STRONG": {"name": "Strong", "category": "AI_GROWTH", "benchmark": "SPY", "description": "", "enabled": True},
    }
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: universe)
    monkeypatch.setattr(config, "non_benchmark_tickers", lambda enabled_only=True: ["STRONG"])
    monkeypatch.setattr(config, "get_benchmark", lambda sym, universe=None: "SPY")

    price_dfs = {"SPY": spy, "STRONG": strong}
    scores = persistence.compute_persistence_scores(price_dfs=price_dfs)

    assert scores["STRONG"]["score"] >= 60
    assert scores["STRONG"]["label"] in ("PERSISTENT_LEADERSHIP", "ESTABLISHED_LEADERSHIP")


def test_consistent_underperformer_scores_low(monkeypatch):
    n = 80
    spy = make_df([100 * (1.0005 ** i) for i in range(n)])
    weak = make_df([100 * (0.99 ** i) for i in range(n)])  # steady, persistent underperformance

    universe = {
        "SPY": {"name": "SPY", "category": "BENCHMARK", "benchmark": None, "description": "", "enabled": True},
        "WEAK": {"name": "Weak", "category": "DEFENSIVE", "benchmark": "SPY", "description": "", "enabled": True},
    }
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: universe)
    monkeypatch.setattr(config, "non_benchmark_tickers", lambda enabled_only=True: ["WEAK"])
    monkeypatch.setattr(config, "get_benchmark", lambda sym, universe=None: "SPY")

    price_dfs = {"SPY": spy, "WEAK": weak}
    scores = persistence.compute_persistence_scores(price_dfs=price_dfs)

    assert scores["WEAK"]["score"] <= 40
    assert scores["WEAK"]["label"] in ("PERSISTENT_WEAKNESS", "ESTABLISHED_LAGGARD")


def test_one_day_spike_penalty_reduces_score(monkeypatch):
    n = 30
    spy = make_df([100.0] * n)

    prices = [100.0] * (n - 1)
    prices.append(prices[-1] * 1.20)  # one huge day dominating the 5D window
    spiky = make_df(prices)

    universe = {
        "SPY": {"name": "SPY", "category": "BENCHMARK", "benchmark": None, "description": "", "enabled": True},
        "SPIKY": {"name": "Spiky", "category": "SPECULATIVE", "benchmark": "SPY", "description": "", "enabled": True},
    }
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: universe)
    monkeypatch.setattr(config, "non_benchmark_tickers", lambda enabled_only=True: ["SPIKY"])
    monkeypatch.setattr(config, "get_benchmark", lambda sym, universe=None: "SPY")

    price_dfs = {"SPY": spy, "SPIKY": spiky}
    scores = persistence.compute_persistence_scores(price_dfs=price_dfs)
    assert scores["SPIKY"]["one_day_spike_penalty_applied"] is True


def test_label_bands_match_spec_boundaries():
    assert persistence.label_for_score(85) == "ESTABLISHED_LEADERSHIP"
    assert persistence.label_for_score(80) == "ESTABLISHED_LEADERSHIP"
    assert persistence.label_for_score(79.9) == "PERSISTENT_LEADERSHIP"
    assert persistence.label_for_score(60) == "PERSISTENT_LEADERSHIP"
    assert persistence.label_for_score(59.9) == "MIXED_TRANSITION"
    assert persistence.label_for_score(40) == "MIXED_TRANSITION"
    assert persistence.label_for_score(20) == "PERSISTENT_WEAKNESS"
    assert persistence.label_for_score(0) == "ESTABLISHED_LAGGARD"
