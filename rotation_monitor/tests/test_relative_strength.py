"""
Fixture-based tests for rotation_monitor.relative_strength.
"""

import pandas as pd
import pytest

from rotation_monitor import relative_strength as rs


def make_df(prices, start="2025-01-01"):
    dates = pd.bdate_range(start=start, periods=len(prices))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices,
         "close": prices, "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


def test_relative_returns_subtracts_benchmark():
    etf = {"1D": -0.03, "5D": 0.02, "10D": None}
    bench = {"1D": -0.005, "5D": 0.01, "10D": 0.04}

    out = rs.relative_returns(etf, bench)
    assert out["1D"] == pytest.approx(-0.025)
    assert out["5D"] == pytest.approx(0.01)
    assert out["10D"] is None  # missing input propagates as None, never estimated


def test_relative_returns_missing_benchmark_is_none():
    etf = {"1D": 0.01}
    bench = {"1D": None}
    out = rs.relative_returns(etf, bench)
    assert out["1D"] is None


def test_normalized_ratio_series_starts_at_100():
    a = make_df([100] * 10)
    b = make_df([50] * 10)
    series = rs.normalized_ratio_series(a, b)
    assert series.iloc[0] == pytest.approx(100.0)


def test_normalized_ratio_series_reflects_relative_outperformance():
    # a doubles, b flat -> ratio should roughly double -> ends near 200
    a = make_df([100, 100, 100, 100, 200])
    b = make_df([50, 50, 50, 50, 50])
    series = rs.normalized_ratio_series(a, b)
    assert series.iloc[-1] == pytest.approx(200.0)


def test_normalized_ratio_series_window_reanchors():
    a = make_df([100, 110, 120, 130, 140])
    b = make_df([100, 100, 100, 100, 100])
    full = rs.normalized_ratio_series(a, b)
    windowed = rs.normalized_ratio_series(a, b, window=2)
    # windowed series re-anchors to 100 at its own (later) start point,
    # so it should differ from the tail of the full series. window=2 spans
    # 3 rows (2 trading days back from the last point), matching
    # returns.return_over_n_days's convention.
    assert windowed.iloc[0] == pytest.approx(100.0)
    assert len(windowed) == 3
    assert full.iloc[-1] != windowed.iloc[-1]


def test_compute_relative_returns_for_universe_benchmark_self_is_zero():
    universe_returns = {
        "SPY": {"1D": 0.01, "3D": 0.02, "5D": 0.03, "10D": 0.04, "20D": 0.05, "MTD": 0.06},
        "QQQ": {"1D": 0.02, "3D": 0.02, "5D": 0.03, "10D": 0.04, "20D": 0.05, "MTD": 0.06},
        "SOXX": {"1D": -0.03, "3D": 0.0, "5D": 0.0, "10D": 0.0, "20D": 0.0, "MTD": 0.0},
    }
    out = rs.compute_relative_returns_for_universe(universe_returns)

    assert out["SPY"]["vs_SPY"]["1D"] == 0.0
    assert out["SOXX"]["vs_SPY"]["1D"] == pytest.approx(-0.04)
    assert out["SOXX"]["vs_QQQ"]["1D"] == pytest.approx(-0.05)
