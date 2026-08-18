"""Tests for rotation_monitor.breadth on synthetic universes."""

import pandas as pd
import pytest

from rotation_monitor import breadth, config


def make_df(prices, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=len(prices))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


@pytest.fixture
def tiny_universe(monkeypatch):
    universe = {
        "SPY": {"name": "SPY", "category": "BENCHMARK", "benchmark": None, "description": "", "enabled": True},
        "QQQ": {"name": "QQQ", "category": "BENCHMARK", "benchmark": "SPY", "description": "", "enabled": True},
        "A": {"name": "A", "category": "GROWTH", "benchmark": "SPY", "description": "", "enabled": True},
        "B": {"name": "B", "category": "GROWTH", "benchmark": "SPY", "description": "", "enabled": True},
        "C": {"name": "C", "category": "DEFENSIVE", "benchmark": "SPY", "description": "", "enabled": True},
    }
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: universe)
    monkeypatch.setattr(config, "non_benchmark_tickers", lambda enabled_only=True: ["A", "B", "C"])
    monkeypatch.setattr(config, "universe_tickers", lambda enabled_only=True: list(universe.keys()))
    monkeypatch.setattr(config, "BENCHMARKS", ["SPY", "QQQ"])
    return universe


def test_category_map_excludes_benchmarks(tiny_universe):
    cats = breadth.category_map()
    assert "SPY" not in sum(cats.values(), [])
    assert cats["GROWTH"] == ["A", "B"]
    assert cats["DEFENSIVE"] == ["C"]


def test_breadth_time_series_all_outperforming(tiny_universe):
    n = 40
    spy = make_df([100.0] * n)
    a = make_df([100 * (1.01 ** i) for i in range(n)])
    b = make_df([100 * (1.02 ** i) for i in range(n)])

    price_dfs = {"SPY": spy, "A": a, "B": b}
    series = breadth.breadth_time_series(["A", "B"], 5, price_dfs)
    assert not series.empty
    assert series.iloc[-1] == pytest.approx(1.0)  # both outperforming => 100%


def test_breadth_time_series_all_underperforming(tiny_universe):
    n = 40
    spy = make_df([100.0] * n)
    a = make_df([100 * (0.99 ** i) for i in range(n)])
    b = make_df([100 * (0.98 ** i) for i in range(n)])

    price_dfs = {"SPY": spy, "A": a, "B": b}
    series = breadth.breadth_time_series(["A", "B"], 5, price_dfs)
    assert series.iloc[-1] == pytest.approx(0.0)


def test_compute_breadth_structure(tiny_universe):
    n = 80
    spy = make_df([100 * (1.001 ** i) for i in range(n)])
    a = make_df([100 * (1.01 ** i) for i in range(n)])
    b = make_df([100 * (0.995 ** i) for i in range(n)])
    c = make_df([100 * (1.002 ** i) for i in range(n)])
    qqq = make_df([100 * (1.001 ** i) for i in range(n)])

    price_dfs = {"SPY": spy, "QQQ": qqq, "A": a, "B": b, "C": c}
    result = breadth.compute_breadth(price_dfs=price_dfs)

    assert "overall" in result and "by_category" in result
    for key in ["outperforming_spy_1d_pct", "outperforming_spy_5d_pct", "outperforming_spy_20d_pct",
                "above_ma20_pct", "above_ma50_pct", "positive_persistence_pct"]:
        assert key in result["overall"]

    assert set(result["by_category"].keys()) == {"GROWTH", "DEFENSIVE"}


def test_breadth_trend_positive_when_improving(tiny_universe):
    n = 70
    spy = make_df([100.0] * n)

    # A: switches from underperforming to outperforming early (by day 10)
    a_prices = [100 * (0.99 ** i) for i in range(10)] + [100 * (0.99 ** 10) * (1.02 ** i) for i in range(n - 10)]
    a = make_df(a_prices)

    # B: switches late -- only within the last 20 days
    b_prices = [100 * (0.99 ** i) for i in range(50)] + [100 * (0.99 ** 50) * (1.02 ** i) for i in range(n - 50)]
    b = make_df(b_prices)

    price_dfs = {"SPY": spy, "A": a, "B": b}
    # 20 days ago: only A has turned positive (~50% breadth).
    # now: both have turned positive (100% breadth) -> trend should be positive.
    trend_val = breadth.breadth_trend(["A", "B"], 5, 20, price_dfs)
    assert trend_val is not None
    assert trend_val > 0
