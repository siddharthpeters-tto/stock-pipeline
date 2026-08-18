"""
Tests for rotation_monitor.wave_detection — the "actively try to disprove"
engine. All scenarios use hand-constructed, fully deterministic daily-return
series (via a flat symbol B, so the spread equals symbol A's own daily
return exactly) rather than randomness, so expected outcomes are exact.
"""

import pandas as pd
import pytest

from rotation_monitor import wave_detection as wd


def price_df_from_returns(rets, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=len(rets) + 1)
    prices = [100.0]
    for r in rets:
        prices.append(prices[-1] * (1 + r))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


def flat_df(n, start="2024-01-01"):
    return price_df_from_returns([0.0] * n, start=start)


# --- low-level helpers -------------------------------------------------

def test_runs_counts_consecutive_signs():
    import numpy as np
    signs = np.array([1, 1, 1, -1, -1, 1, 1, 1, 1])
    assert wd._runs(signs) == [3, 2, 4]


def test_flip_count():
    import numpy as np
    signs = np.array([1, 1, -1, -1, 1])
    assert wd._flip_count(signs) == 2


def test_binomial_z_matches_hand_calc():
    # 10 flips out of 20 interior comparisons, baseline p=0.3
    z = wd._binomial_z(observed_flips=10, n_interior=20, p_baseline=0.3)
    expected = (10 - 20 * 0.3) / (20 * 0.3 * 0.7) ** 0.5
    assert z == pytest.approx(expected)


def test_binomial_z_none_when_no_baseline():
    assert wd._binomial_z(5, 10, None) is None
    assert wd._binomial_z(5, 0, 0.5) is None


# --- pair_wave_summary scenarios ---------------------------------------

def test_insufficient_history_flagged():
    df_a = price_df_from_returns([0.01, -0.01, 0.02])
    df_b = flat_df(3)
    result = wd.pair_wave_summary("A", "B", df_a=df_a, df_b=df_b)
    assert result["classification"] == "INSUFFICIENT_HISTORY"


def test_stable_flip_rate_is_no_pattern():
    # strictly alternating sign, constant magnitude, for the ENTIRE history:
    # recent behavior is identical to baseline behavior -> nothing "unusual".
    # (this is also a degenerate case for the binomial z-test itself: a
    # baseline flip probability of 1.0 gives zero variance, so z is
    # undefined rather than 0 -- and that must still resolve to NO_PATTERN,
    # not be mistaken for "maximally significant".)
    rets = [0.01 if i % 2 == 0 else -0.01 for i in range(150)]
    df_a = price_df_from_returns(rets)
    df_b = flat_df(150)

    result = wd.pair_wave_summary("A", "B", df_a=df_a, df_b=df_b)
    assert result["classification"] == "NO_PATTERN"
    assert result["z_score"] is None


def test_persistent_one_side_leadership():
    # A leads (positive spread) on ~95% of the last 20 days
    baseline = [0.001 if i % 2 == 0 else -0.001 for i in range(100)]
    recent = [0.01] * 19 + [-0.01]  # 19/20 positive days
    df_a = price_df_from_returns(baseline + recent)
    df_b = flat_df(len(baseline + recent))

    result = wd.pair_wave_summary("A", "B", df_a=df_a, df_b=df_b)
    assert result["classification"] == "PERSISTENT_ONE_SIDE_LEADERSHIP"
    assert "A led on 95.0%" in result["evidence"][0]


def test_low_magnitude_flips_stay_no_pattern(monkeypatch):
    # baseline has a low flip rate, so a fully-alternating recent window
    # would normally look "unusual" by flip-rate alone -- but every recent
    # move is tiny (well under the noise floor), so it must NOT be called a
    # pattern.
    baseline = []
    sign = 1
    for i in range(90):
        if i % 5 == 0 and i > 0:
            sign *= -1
        baseline.append(sign * 0.01)  # baseline moves are NOT tiny

    recent = [0.0001, -0.0001, 0.0001, -0.0001, 0.0001, -0.0001]  # tiny, alternating
    df_a = price_df_from_returns(baseline + recent)
    df_b = flat_df(len(baseline + recent))

    monkeypatch.setattr(wd, "WAVE_RECENT_WINDOW", 6)
    monkeypatch.setattr(wd, "WAVE_BASELINE_WINDOW", 96)

    result = wd.pair_wave_summary("A", "B", df_a=df_a, df_b=df_b)
    assert result["classification"] == "NO_PATTERN"
    assert any("noise floor" in c for c in result["counter_evidence"])


def test_single_day_dependent_pattern_is_downgraded(monkeypatch):
    """
    A recent window that flips every day looks like REPEATED_ROTATION at
    first glance -- but almost all of that flipping is trivial-magnitude,
    and the one day that isn't trivial is also the day whose removal
    collapses the flip-rate deviation. The engine must catch this and
    downgrade rather than reporting REPEATED_ROTATION at face value.
    """
    baseline = []
    sign = 1
    for i in range(90):
        if i % 3 == 0 and i > 0:
            sign *= -1
        baseline.append(sign * 0.001)

    recent = [0.001, -0.001, 0.05, -0.001, 0.001, -0.001]  # one outlier day
    df_a = price_df_from_returns(baseline + recent)
    df_b = flat_df(len(baseline + recent))

    monkeypatch.setattr(wd, "WAVE_RECENT_WINDOW", 6)
    monkeypatch.setattr(wd, "WAVE_BASELINE_WINDOW", 96)

    result = wd.pair_wave_summary("A", "B", df_a=df_a, df_b=df_b)

    assert result["single_day_dependent"] is True
    # must NOT be reported as the strongest, unqualified classification
    assert result["classification"] != "REPEATED_ROTATION"
    assert any("largest-magnitude session" in c for c in result["counter_evidence"])


def test_all_wave_summaries_covers_configured_pairs():
    from rotation_monitor import config
    price_dfs = {}
    symbols = {s for pair in config.WAVE_PAIRS for s in pair}
    for s in symbols:
        price_dfs[s] = price_df_from_returns([0.001, -0.001] * 100)

    summaries = wd.all_wave_summaries(price_dfs=price_dfs)
    expected_keys = {f"{a}_{b}" for a, b in config.WAVE_PAIRS}
    assert set(summaries.keys()) == expected_keys
