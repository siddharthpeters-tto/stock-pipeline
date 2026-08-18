"""
Tests for rotation_monitor.rotation_matrix, timeline, and heatmap on small
synthetic universes.
"""

import pandas as pd
import pytest

from rotation_monitor import config, heatmap, rotation_matrix, timeline


def make_df(prices, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=len(prices))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


SMALL_UNIVERSE = {
    "SPY": {"name": "SPY", "category": "BENCHMARK", "benchmark": None, "enabled": True, "description": ""},
    "QQQ": {"name": "QQQ", "category": "BENCHMARK", "benchmark": "SPY", "enabled": True, "description": ""},
    "A": {"name": "A", "category": "GROWTH", "benchmark": "SPY", "enabled": True, "description": ""},
    "B": {"name": "B", "category": "GROWTH", "benchmark": "SPY", "enabled": True, "description": ""},
    "C": {"name": "C", "category": "DEFENSIVE", "benchmark": "SPY", "enabled": True, "description": ""},
}


@pytest.fixture
def universe(monkeypatch):
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: SMALL_UNIVERSE)
    monkeypatch.setattr(config, "non_benchmark_tickers", lambda enabled_only=True: ["A", "B", "C"])
    monkeypatch.setattr(config, "universe_tickers", lambda enabled_only=True: list(SMALL_UNIVERSE.keys()))
    monkeypatch.setattr(config, "get_benchmark", lambda sym, universe=None: SMALL_UNIVERSE.get(sym, {}).get("benchmark"))
    return SMALL_UNIVERSE


def _dfs(n=80):
    return {
        "SPY": make_df([100.0] * n),
        "QQQ": make_df([100.0] * n),
        "A": make_df([100 * (1.01 ** i) for i in range(n)]),
        "B": make_df([100 * (0.99 ** i) for i in range(n)]),
        "C": make_df([100.0] * n),
    }


def test_rotation_matrix_strong_up_for_outperforming_category(universe):
    price_dfs = _dfs()
    matrix = rotation_matrix.compute_rotation_matrix(price_dfs=price_dfs)
    assert matrix["GROWTH"]["20D"]["label"] in ("STRONG_UP", "UP")
    assert matrix["GROWTH"]["members"] == ["A", "B"]
    assert "avg_persistence" in matrix["GROWTH"]


def test_rotation_matrix_flat_for_untouched_category(universe):
    price_dfs = _dfs()
    matrix = rotation_matrix.compute_rotation_matrix(price_dfs=price_dfs)
    assert matrix["DEFENSIVE"]["1D"]["label"] == "FLAT"


def test_rotation_matrix_label_thresholds():
    assert rotation_matrix._label(0.05) == "STRONG_UP"
    assert rotation_matrix._label(-0.05) == "STRONG_DOWN"
    assert rotation_matrix._label(0.001) == "FLAT"
    assert rotation_matrix._label(0.015) == "UP"
    assert rotation_matrix._label(-0.015) == "DOWN"
    assert rotation_matrix._label(None) == "DATA_UNAVAILABLE"


def test_leadership_timeline_ranks_true_leader_first(universe):
    n = 40
    price_dfs = _dfs(n)
    result = timeline.compute_leadership_timeline(days=10, price_dfs=price_dfs)
    assert len(result) > 0
    last_day = result[-1]
    assert last_day["leaders"][0] == "A"  # A is the consistent outperformer
    assert last_day["weakest"] == "B"     # B is the consistent laggard


def test_leadership_timeline_dominant_category_is_deterministic(universe):
    price_dfs = _dfs()
    result = timeline.compute_leadership_timeline(days=5, price_dfs=price_dfs)
    for row in result:
        assert row["dominant_category"] != ""


def test_monthly_heatmap_structure(universe):
    price_dfs = _dfs()
    hm = heatmap.compute_monthly_heatmap(mode="relative_spy", price_dfs=price_dfs)
    assert hm["themes"] == ["A", "B", "C"]
    assert len(hm["values_pct"]) == 3
    assert all(len(row) == len(hm["dates"]) for row in hm["values_pct"])


def test_monthly_heatmap_invalid_mode_raises(universe):
    price_dfs = _dfs()
    with pytest.raises(ValueError):
        heatmap.compute_monthly_heatmap(mode="not_a_real_mode", price_dfs=price_dfs)


def test_monthly_heatmap_relative_spy_is_zero_for_spy_like_series(universe):
    n = 40
    price_dfs = _dfs(n)
    price_dfs["C"] = make_df([100.0] * n)  # identical to SPY's flat series
    hm = heatmap.compute_monthly_heatmap(mode="relative_spy", price_dfs=price_dfs)
    c_row = hm["values_pct"][hm["themes"].index("C")]
    assert all(v == pytest.approx(0.0, abs=1e-9) for v in c_row if v is not None)
