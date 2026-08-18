"""
Fixture-based tests for rotation_monitor.returns — no network, no SQLite.
"""

import pandas as pd
import pytest

from rotation_monitor import returns


def make_df(prices, start="2025-01-01"):
    """Build a daily-business-day price DataFrame from a plain price list."""
    dates = pd.bdate_range(start=start, periods=len(prices))
    df = pd.DataFrame(
        {
            "open": prices,
            "high": prices,
            "low": prices,
            "close": prices,
            "adj_close": prices,
            "volume": [1_000_000] * len(prices),
        },
        index=dates,
    )
    df.index.name = "date"
    return df


def test_return_over_n_days_simple():
    # 10 flat days then a jump: last close 110, close 5 days back 100 -> +10%
    prices = [100] * 5 + [110] * 5
    df = make_df(prices)
    r = returns.return_over_n_days(df, 5)
    assert r == pytest.approx(0.10)


def test_return_over_n_days_insufficient_history_is_none():
    df = make_df([100, 101, 102])
    assert returns.return_over_n_days(df, 5) is None


def test_return_1d_sign_and_magnitude():
    prices = [100, 95]  # -5% day
    df = make_df(prices)
    r = returns.return_over_n_days(df, 1)
    assert r == pytest.approx(-0.05)


def test_mtd_return_uses_first_close_of_current_month():
    # All business days within a single month: Jan 1 -> Jan 10 (approx)
    prices = list(range(100, 110))  # 100..109, 10 points
    df = make_df(prices, start="2025-01-01")
    r = returns.mtd_return(df)
    expected = (df["adj_close"].iloc[-1] / df["adj_close"].iloc[0]) - 1.0
    assert r == pytest.approx(expected)


def test_ytd_return_matches_first_close_of_year():
    prices = list(range(200, 220))
    df = make_df(prices, start="2025-01-01")
    r = returns.ytd_return(df)
    expected = (df["adj_close"].iloc[-1] / df["adj_close"].iloc[0]) - 1.0
    assert r == pytest.approx(expected)


def test_empty_df_returns_none_everywhere():
    df = make_df([])
    assert returns.return_over_n_days(df, 1) is None
    assert returns.mtd_return(df) is None
    assert returns.qtd_return(df) is None
    assert returns.ytd_return(df) is None


def test_compute_returns_shape(monkeypatch):
    prices = [100 + i for i in range(300)]  # plenty of history
    df = make_df(prices, start="2024-06-01")

    result = returns.compute_returns("FAKE", df=df)
    for key in ["1D", "3D", "5D", "10D", "20D", "MTD", "QTD", "YTD"]:
        assert key in result
