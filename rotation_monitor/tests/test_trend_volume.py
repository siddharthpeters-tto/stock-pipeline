"""Fixture-based tests for rotation_monitor.trend and rotation_monitor.volume."""

import pandas as pd
import pytest

from rotation_monitor import trend, volume


def make_df(prices, volumes=None, start="2025-01-01"):
    dates = pd.bdate_range(start=start, periods=len(prices))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": volumes or [1_000_000] * len(prices)},
        index=dates,
    )


def test_distance_from_ma_positive_when_above():
    prices = [100] * 25 + [130]  # last close well above its own 20D MA
    df = make_df(prices)
    dist = trend.distance_from_ma(df, 20)
    assert dist > 0


def test_distance_from_ma_none_when_insufficient_history():
    df = make_df([100, 101, 102])
    assert trend.distance_from_ma(df, 20) is None


def test_classify_trend_strong_uptrend():
    state = trend.classify_trend(dist_ma20=0.05, slope_ma20=0.01)
    assert state == "STRONG_UPTREND"


def test_classify_trend_strong_downtrend():
    state = trend.classify_trend(dist_ma20=-0.05, slope_ma20=-0.01)
    assert state == "STRONG_DOWNTREND"


def test_classify_trend_flat():
    state = trend.classify_trend(dist_ma20=0.001, slope_ma20=0.0005)
    assert state == "FLAT"


def test_classify_trend_missing_data():
    assert trend.classify_trend(None, None) == "DATA_UNAVAILABLE"


def test_volume_classification_bands():
    assert volume.classify_volume_ratio(0.5) == "LOW"
    assert volume.classify_volume_ratio(1.0) == "NORMAL"
    assert volume.classify_volume_ratio(1.5) == "ELEVATED"
    assert volume.classify_volume_ratio(2.5) == "VERY_ELEVATED"
    assert volume.classify_volume_ratio(None) == "DATA_UNAVAILABLE"


def test_compute_volume_ratio_matches_manual_calc():
    volumes = [1_000_000] * 19 + [3_000_000]  # last day 3x the flat 20D average
    df = make_df([100] * 20, volumes=volumes)
    result = volume.compute_volume("TEST", df=df)
    assert result["volume_ratio"] == pytest.approx(3_000_000 / df["volume"].mean(), abs=1e-3)
    assert result["classification"] == "VERY_ELEVATED"


def test_compute_volume_handles_empty():
    df = make_df([])
    result = volume.compute_volume("TEST", df=df)
    assert result["classification"] == "DATA_UNAVAILABLE"
