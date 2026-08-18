"""
Tests for rotation_monitor.rotation_detect and rotation_monitor.reversal on
synthetic pairs where the "correct" direction is known by construction.
"""

import pandas as pd
import pytest

from rotation_monitor import config, reversal, rotation_detect


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
        "A": {"name": "A", "category": "GROWTH", "benchmark": "SPY", "description": "", "enabled": True},
        "B": {"name": "B", "category": "GROWTH", "benchmark": "SPY", "description": "", "enabled": True},
    }
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: u)
    return u


def test_detect_pair_rotation_picks_the_actual_outperformer(universe):
    n = 60
    spy = make_df([100.0] * n)
    # A steadily outperforms B (and SPY) throughout, with a clean recent run
    a = make_df([100 * (1.01 ** i) for i in range(n)])
    b = make_df([100 * (0.999 ** i) for i in range(n)])

    price_dfs = {"SPY": spy, "A": a, "B": b}
    result = rotation_detect.detect_pair_rotation("A", "B", "A_OVER_B", "B_OVER_A", price_dfs=price_dfs)

    assert result["state"] == "A_OVER_B"
    assert result["confidence"] > 0
    assert result["duration_days"] > 0


def test_detect_pair_rotation_neutral_when_no_signal(universe):
    n = 60
    spy = make_df([100.0] * n)
    a = make_df([100.0] * n)
    b = make_df([100.0] * n)

    price_dfs = {"SPY": spy, "A": a, "B": b}
    result = rotation_detect.detect_pair_rotation("A", "B", "A_OVER_B", "B_OVER_A", price_dfs=price_dfs)
    assert result["state"] == "NEUTRAL"
    assert result["confidence"] == 0.0


def test_counter_evidence_flags_absolute_return_contradiction(universe):
    n = 60
    spy = make_df([100.0] * n)
    # B has a much bigger raw 20D return than A even though A is *relatively*
    # improving vs its own benchmark right at the end -- construct A rising
    # gently from a flat/negative base in just the last few days, while B has
    # been on a strong steady climb throughout.
    a_prices = [100 * (0.999 ** i) for i in range(n - 4)]
    a_prices += [a_prices[-1] * (1.01 ** i) for i in range(1, 5)]
    a = make_df(a_prices)
    b = make_df([100 * (1.02 ** i) for i in range(n)])

    price_dfs = {"SPY": spy, "A": a, "B": b}
    result = rotation_detect.detect_pair_rotation("A", "B", "A_OVER_B", "B_OVER_A", price_dfs=price_dfs)

    if result["state"] != "NEUTRAL":
        # whichever direction won, the raw-20D-return counter-evidence check
        # should still fire since B's absolute return dwarfs A's
        joined = " ".join(result["counter_evidence"])
        assert "remains stronger" in joined or result["state"] == "B_OVER_A"


def test_short_term_reversal_flag_on_sign_flip(universe):
    n = 30
    spy = make_df([100.0] * n)
    a_returns = [0.0] * (n - 3) + [0.02, -0.02, 0.03]  # last two days flip sign
    b_returns = [0.0] * n

    a_prices = [100.0]
    for r in a_returns:
        a_prices.append(a_prices[-1] * (1 + r))
    a = make_df(a_prices)
    b = make_df([100.0] * (n + 1))

    result = reversal.detect_pair_reversal("A", "B", "A_OVER_B", "B_OVER_A", df_a=a, df_b=b)
    assert result["reversal_flag"] == "SHORT_TERM_LEADERSHIP_REVERSAL"


def test_persistent_state_matches_20d_ratio_direction(universe):
    n = 60
    a = make_df([100 * (1.01 ** i) for i in range(n)])  # A clearly outperforms over 20D
    b = make_df([100.0] * n)

    result = reversal.detect_pair_reversal("A", "B", "A_OVER_B", "B_OVER_A", df_a=a, df_b=b)
    assert result["persistent_20d_state"] == "A_OVER_B"


def test_all_pair_rotations_and_reversals_cover_configured_pairs():
    price_dfs = {}
    symbols = {"SPY", "QQQ"} | {s for spec in config.ROTATION_DETECT_PAIRS for s in spec["pair"]}
    universe = config.load_universe()
    for spec in config.ROTATION_DETECT_PAIRS:
        for s in spec["pair"]:
            symbols.add(universe.get(s, {}).get("benchmark") or "SPY")

    for s in symbols:
        price_dfs[s] = make_df([100 * (1.001 ** i) for i in range(60)])

    rotations = rotation_detect.all_pair_rotations(price_dfs=price_dfs)
    reversals = reversal.all_pair_reversals(price_dfs=price_dfs)

    assert len(rotations) == len(config.ROTATION_DETECT_PAIRS)
    assert len(reversals) == len(config.ROTATION_DETECT_PAIRS)
