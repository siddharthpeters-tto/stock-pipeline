"""
Fixture-based tests for rotation_monitor.correlations — synthetic return
series with known correlation, so the expected rolling-correlation value is
known exactly rather than eyeballed.
"""

import numpy as np
import pandas as pd
import pytest

from rotation_monitor import correlations


def price_df_from_returns(daily_returns, start="2025-01-01"):
    dates = pd.bdate_range(start=start, periods=len(daily_returns) + 1)
    prices = [100.0]
    for r in daily_returns:
        prices.append(prices[-1] * (1 + r))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices,
         "close": prices, "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


def test_perfectly_correlated_series_gives_correlation_one():
    rng = np.random.default_rng(42)
    base_returns = rng.normal(0, 0.01, 40)

    df_a = price_df_from_returns(list(base_returns))
    df_b = price_df_from_returns(list(base_returns * 2))  # same sign/timing, different scale

    series = correlations.rolling_correlation_series(df_a, df_b, window=20)
    assert series.iloc[-1] == pytest.approx(1.0, abs=1e-6)


def test_perfectly_anti_correlated_series_gives_correlation_minus_one():
    rng = np.random.default_rng(7)
    base_returns = rng.normal(0, 0.01, 40)

    df_a = price_df_from_returns(list(base_returns))
    df_b = price_df_from_returns(list(-base_returns))

    series = correlations.rolling_correlation_series(df_a, df_b, window=20)
    assert series.iloc[-1] == pytest.approx(-1.0, abs=1e-6)


def test_insufficient_history_returns_empty_series():
    df_a = price_df_from_returns([0.01, -0.01, 0.02])
    df_b = price_df_from_returns([0.01, -0.01, 0.02])
    series = correlations.rolling_correlation_series(df_a, df_b, window=20)
    assert series.empty


def test_correlation_summary_structure():
    rng = np.random.default_rng(1)
    a = rng.normal(0, 0.01, 300)
    b = rng.normal(0, 0.01, 300)

    df_a = price_df_from_returns(list(a))
    df_b = price_df_from_returns(list(b))

    # exercise via the public per-window helper instead of hitting SQLite
    series_20 = correlations.rolling_correlation_series(df_a, df_b, 20)
    series_60 = correlations.rolling_correlation_series(df_a, df_b, 60)

    assert not series_20.empty
    assert not series_60.empty
    assert -1.0 <= series_20.iloc[-1] <= 1.0
    assert -1.0 <= series_60.iloc[-1] <= 1.0
