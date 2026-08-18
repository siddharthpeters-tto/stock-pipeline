"""Tests for rotation_monitor.backtest on synthetic pairs."""

import pandas as pd
import pytest

from rotation_monitor import backtest


def price_df_from_returns(rets, start="2023-01-01"):
    dates = pd.bdate_range(start=start, periods=len(rets) + 1)
    prices = [100.0]
    for r in rets:
        prices.append(prices[-1] * (1 + r))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


def test_analyze_pair_covers_all_windows():
    n = 300
    rets = [0.005 if i % 2 == 0 else -0.005 for i in range(n)]
    df_a = price_df_from_returns(rets)
    df_b = price_df_from_returns([0.0] * n)

    result = backtest.analyze_pair("A", "B", df_a=df_a, df_b=df_b)
    assert set(result["windows"].keys()) == set(backtest.BACKTEST_WINDOWS.keys())
    for label, stats in result["windows"].items():
        assert stats["window_days"] == backtest.BACKTEST_WINDOWS[label]
        assert stats["flip_count"] is not None
        assert "correlation_drift" in stats
        assert "classification" in stats


def test_established_leader_detected_when_one_side_dominates():
    n = 300
    # A leads on ~90% of days across the full history
    rets = [0.01 if i % 10 != 0 else -0.01 for i in range(n)]
    df_a = price_df_from_returns(rets)
    df_b = price_df_from_returns([0.0] * n)

    result = backtest.analyze_pair("A", "B", df_a=df_a, df_b=df_b)
    assert result["established_leader_longest_window"] == "A"


def test_no_established_leader_when_evenly_split():
    n = 300
    rets = [0.005 if i % 2 == 0 else -0.005 for i in range(n)]
    df_a = price_df_from_returns(rets)
    df_b = price_df_from_returns([0.0] * n)

    result = backtest.analyze_pair("A", "B", df_a=df_a, df_b=df_b)
    assert result["established_leader_longest_window"] is None


def test_analyze_pairs_multiple():
    n = 300
    price_dfs = {
        "A": price_df_from_returns([0.005] * n),
        "B": price_df_from_returns([0.0] * n),
        "C": price_df_from_returns([-0.003] * n),
    }
    result = backtest.analyze_pairs([("A", "B"), ("B", "C")], price_dfs=price_dfs)
    assert set(result.keys()) == {"A_B", "B_C"}
