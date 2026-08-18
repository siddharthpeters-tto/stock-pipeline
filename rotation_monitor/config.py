"""
rotation_monitor.config — ETF universe configuration.

Loads `rotation_config/rotation_universe.json` at the repo root. New ETFs
can be added there without touching any code (per the build spec's
"configuration, not code changes" requirement).

Each entry looks like:
    {
      "SOXX": {
        "name": "Semiconductors",
        "category": "AI_GROWTH",
        "benchmark": "QQQ",
        "description": "...",
        "enabled": true
      },
      ...
    }
"""

import json
import os
from typing import Dict, List, Optional

# repo root = parent of this package's directory
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNIVERSE_PATH = os.path.join(_REPO_ROOT, "rotation_config", "rotation_universe.json")

# Rolling / lookback windows used throughout the module (trading days)
LOOKBACK_WINDOWS = [1, 3, 5, 10, 20, 60, 120, 252]

# Minimum trading days of history to keep cached locally
HISTORY_DAYS_TARGET = 730  # ~2 years of calendar days (buffer for weekends/holidays)

# Pairs used for relative-strength ratios (§7) and correlation tracking (§10)
RS_RATIO_PAIRS = [
    ("IGV", "SOXX"),
    ("SOXX", "QQQ"),
    ("IGV", "QQQ"),
    ("XLF", "QQQ"),
    ("KRE", "XLF"),
    ("XLY", "XLP"),
    ("XBI", "XLV"),
    ("UFO", "QQQ"),
    ("XLI", "QQQ"),
]

CORRELATION_PAIRS = [
    ("SOXX", "IGV"),
    ("SOXX", "QQQ"),
    ("IGV", "QQQ"),
    ("SOXX", "XLF"),
    ("IGV", "XBI"),
    ("UFO", "XBI"),
    ("KRE", "XLF"),
    ("XLY", "XLP"),
    ("XLI", "XLE"),
    ("XBI", "XLV"),
]

BENCHMARKS = ["SPY", "QQQ"]

# Pairs analyzed for wave/oscillation behavior (§21)
WAVE_PAIRS = [
    ("SOXX", "IGV"),
    ("XLY", "XLP"),
    ("XBI", "XLV"),
    ("KRE", "XLF"),
]

# Pairs the rotation-detection engine actively looks for a named rotation
# story on (§13), with the state label used for each direction.
ROTATION_DETECT_PAIRS = [
    {"pair": ("IGV", "SOXX"), "a_over_b": "SOFTWARE_OVER_SEMIS", "b_over_a": "SEMIS_OVER_SOFTWARE"},
    {"pair": ("XLY", "XLP"), "a_over_b": "CYCLICAL_OVER_DEFENSIVE", "b_over_a": "DEFENSIVE_OVER_CYCLICAL"},
    {"pair": ("KRE", "XLF"), "a_over_b": "REGIONAL_BANKS_OVER_BROAD_FINANCIALS", "b_over_a": "BROAD_FINANCIALS_OVER_REGIONAL_BANKS"},
    {"pair": ("XBI", "XLV"), "a_over_b": "SPECULATIVE_BIOTECH_OVER_DEFENSIVE_HEALTHCARE", "b_over_a": "DEFENSIVE_HEALTHCARE_OVER_SPECULATIVE_BIOTECH"},
    {"pair": ("UFO", "QQQ"), "a_over_b": "SPECULATIVE_SPACE_OVER_MARKET", "b_over_a": "MARKET_OVER_SPECULATIVE_SPACE"},
]


def load_universe(enabled_only: bool = True) -> Dict[str, dict]:
    """Load the ETF universe config, keyed by ticker."""
    with open(UNIVERSE_PATH, "r") as f:
        universe = json.load(f)

    if enabled_only:
        universe = {t: cfg for t, cfg in universe.items() if cfg.get("enabled", True)}

    return universe


def universe_tickers(enabled_only: bool = True) -> List[str]:
    return list(load_universe(enabled_only).keys())


def non_benchmark_tickers(enabled_only: bool = True) -> List[str]:
    """Themes only — excludes SPY/QQQ, which serve as benchmarks, not themes to rank."""
    return [t for t in universe_tickers(enabled_only) if t not in BENCHMARKS]


def get_benchmark(ticker: str, universe: Optional[Dict[str, dict]] = None) -> Optional[str]:
    """The configured primary benchmark for a ticker (may be None for SPY itself)."""
    universe = universe or load_universe()
    cfg = universe.get(ticker)
    return cfg.get("benchmark") if cfg else None
