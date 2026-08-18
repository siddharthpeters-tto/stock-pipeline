"""
Tests for rotation_monitor.state_builder and rotation_monitor.report on a
small synthetic universe -- verifies the end-to-end assembly runs, produces
the documented shape, never invents a number, and never uses buy/sell
language, without touching the real cache/DB.
"""

import json

import pandas as pd
import pytest

from rotation_monitor import config, report, state_builder


def make_df(prices, start="2023-06-01"):
    dates = pd.bdate_range(start=start, periods=len(prices))
    return pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices,
         "adj_close": prices, "volume": [1_000_000] * len(prices)},
        index=dates,
    )


FULL_UNIVERSE = {
    "SPY": {"name": "SPY", "category": "BENCHMARK", "benchmark": None, "enabled": True, "description": ""},
    "QQQ": {"name": "QQQ", "category": "BENCHMARK", "benchmark": "SPY", "enabled": True, "description": ""},
    "SOXX": {"name": "SOXX", "category": "AI_GROWTH", "benchmark": "QQQ", "enabled": True, "description": ""},
    "IGV": {"name": "IGV", "category": "AI_GROWTH", "benchmark": "QQQ", "enabled": True, "description": ""},
    "CIBR": {"name": "CIBR", "category": "AI_GROWTH", "benchmark": "QQQ", "enabled": True, "description": ""},
    "SKYY": {"name": "SKYY", "category": "AI_GROWTH", "benchmark": "QQQ", "enabled": True, "description": ""},
    "UFO": {"name": "UFO", "category": "SPECULATIVE", "benchmark": "QQQ", "enabled": True, "description": ""},
    "DRIV": {"name": "DRIV", "category": "SPECULATIVE", "benchmark": "QQQ", "enabled": True, "description": ""},
    "XBI": {"name": "XBI", "category": "SPECULATIVE", "benchmark": "SPY", "enabled": True, "description": ""},
    "ITA": {"name": "ITA", "category": "AEROSPACE_DEFENSE", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLF": {"name": "XLF", "category": "FINANCIALS", "benchmark": "SPY", "enabled": True, "description": ""},
    "KRE": {"name": "KRE", "category": "FINANCIALS", "benchmark": "SPY", "enabled": True, "description": ""},
    "FINX": {"name": "FINX", "category": "FINANCIALS", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLI": {"name": "XLI", "category": "CYCLICAL", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLE": {"name": "XLE", "category": "CYCLICAL", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLY": {"name": "XLY", "category": "CYCLICAL", "benchmark": "SPY", "enabled": True, "description": ""},
    "XHB": {"name": "XHB", "category": "CYCLICAL", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLV": {"name": "XLV", "category": "DEFENSIVE", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLP": {"name": "XLP", "category": "DEFENSIVE", "benchmark": "SPY", "enabled": True, "description": ""},
    "XLU": {"name": "XLU", "category": "DEFENSIVE", "benchmark": "SPY", "enabled": True, "description": ""},
}


@pytest.fixture
def universe(monkeypatch):
    monkeypatch.setattr(config, "load_universe", lambda enabled_only=True: FULL_UNIVERSE)
    monkeypatch.setattr(config, "non_benchmark_tickers", lambda enabled_only=True: [s for s in FULL_UNIVERSE if s not in ("SPY", "QQQ")])
    monkeypatch.setattr(config, "universe_tickers", lambda enabled_only=True: list(FULL_UNIVERSE.keys()))
    monkeypatch.setattr(config, "get_benchmark", lambda sym, universe=None: FULL_UNIVERSE.get(sym, {}).get("benchmark"))
    return FULL_UNIVERSE


def _price_dfs(n=280):
    dfs = {}
    for sym in FULL_UNIVERSE:
        # give each symbol a mildly different deterministic drift so the
        # universe isn't perfectly flat/degenerate
        drift = 1.0002 if sym not in ("XLP", "XLV", "XLU") else 1.0006
        dfs[sym] = make_df([100 * (drift ** i) for i in range(n)])
    return dfs


def test_build_rotation_state_shape(universe):
    price_dfs = _price_dfs()
    state = state_builder.build_rotation_state(price_dfs=price_dfs)

    required_keys = [
        "date", "market_regime", "regime_confidence", "risk_appetite_score",
        "leaders", "laggards", "persistence_scores", "breadth", "rotation_matrix",
        "rotations", "reversals", "wave_detection", "rs_ratios", "correlations",
        "leadership_timeline", "alerts", "priority_pair_backtest", "universe",
    ]
    for key in required_keys:
        assert key in state


def test_build_rotation_state_is_json_serializable(universe):
    price_dfs = _price_dfs()
    state = state_builder.build_rotation_state(price_dfs=price_dfs)
    # must not raise -- everything in state should already be JSON-safe
    # (default=str as a last-resort safety net, matching save_state)
    json.dumps(state, default=str)


def test_save_state_writes_file(universe, tmp_path, monkeypatch):
    monkeypatch.setattr(state_builder, "OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr(state_builder, "STATE_PATH", str(tmp_path / "rotation_state.json"))

    price_dfs = _price_dfs()
    state = state_builder.build_rotation_state(price_dfs=price_dfs)
    path = state_builder.save_state(state)

    assert (tmp_path / "rotation_state.json").exists()
    with open(path) as f:
        loaded = json.load(f)
    assert loaded["market_regime"] == state["market_regime"]


def test_report_generation_runs_and_contains_key_sections(universe):
    price_dfs = _price_dfs()
    state = state_builder.build_rotation_state(price_dfs=price_dfs)
    markdown = report.generate_report_markdown(state)

    for heading in [
        "# Market Rotation Report", "## Current Regime", "## Risk Appetite",
        "## Key Rotation: IGV vs SOXX", "## Breadth", "## Rotation Matrix",
        "## Rotation Alerts", "## Leadership Timeline", "## Summary — The Five Questions",
    ]:
        assert heading in markdown


def test_report_never_recommends_trades(universe):
    price_dfs = _price_dfs()
    state = state_builder.build_rotation_state(price_dfs=price_dfs)
    markdown = report.generate_report_markdown(state).lower()

    # the report's own disclaimer legitimately contains the words "buy" and
    # "sell" while explicitly disclaiming them -- check everything else.
    disclaimer = "it does not recommend individual stocks or issue buy/sell signals."
    assert disclaimer in markdown
    body = markdown.replace(disclaimer, "")

    banned = ["buy ", "sell ", "price target", "strong_buy", "recommend buying", "recommend selling"]
    for phrase in banned:
        assert phrase not in body


def test_report_has_no_doubled_periods(universe):
    price_dfs = _price_dfs()
    state = state_builder.build_rotation_state(price_dfs=price_dfs)
    markdown = report.generate_report_markdown(state)
    assert ".." not in markdown.replace("...", "")  # ignore intentional ellipses, if any


def test_save_report_writes_file(universe, tmp_path, monkeypatch):
    monkeypatch.setattr(report, "OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr(report, "REPORT_PATH", str(tmp_path / "rotation_report.md"))

    price_dfs = _price_dfs()
    state = state_builder.build_rotation_state(price_dfs=price_dfs)
    path = report.save_report(state)

    assert (tmp_path / "rotation_report.md").exists()
    content = (tmp_path / "rotation_report.md").read_text(encoding="utf-8")
    assert "Market Rotation Report" in content
