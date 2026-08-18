"""
rotation_monitor.scoring_config — every tunable weight/threshold used by
Phase 2+, in one place, documented. Nothing in persistence.py, regime.py,
risk_appetite.py, rotation_detect.py, wave_detection.py, or alerts.py
should hard-code a number that isn't defined (and explained) here.

All scores in this module are deterministic functions of retrieved/derived
market data (§28 — "do not use an LLM to invent confidence"). Any text
explanation built on top of these numbers in report.py is generated from
the same deterministic inputs, not invented separately.
"""

# ---------------------------------------------------------------------------
# Regime read horizon
# ---------------------------------------------------------------------------
# Most "current state" reads (regime, rotation detection, risk appetite) use
# 5D relative return as the primary short-term signal and 20D as the
# structural/persistent cross-check, per the spec's short-term vs
# persistent-state distinction (§20).
SHORT_TERM_HORIZON = "5D"
PERSISTENT_HORIZON = "20D"

# ---------------------------------------------------------------------------
# Trend descriptors (§8)
# ---------------------------------------------------------------------------
TREND_THRESHOLDS = {
    # distance from MA, as a fraction (0.03 == 3%), combined with MA slope
    # sign to classify trend state.
    "strong_distance": 0.03,
    "flat_distance": 0.01,     # within +/-1% of the MA and flat slope => FLAT
    "flat_slope": 0.002,       # MA slope magnitude below this counts as "flat"
}

# ---------------------------------------------------------------------------
# Volume classification (§9)
# ---------------------------------------------------------------------------
VOLUME_THRESHOLDS = {
    "low": 0.7,           # ratio < 0.7x 20D avg => LOW
    "elevated": 1.3,      # ratio >= 1.3x => ELEVATED
    "very_elevated": 2.0,  # ratio >= 2.0x => VERY_ELEVATED
}

# ---------------------------------------------------------------------------
# Rotation Persistence Score (§12) — 0-100, weights sum to 100
# ---------------------------------------------------------------------------
PERSISTENCE_WEIGHTS = {
    "pct_positive_rel_20d": 30,     # % of last 20 trading days with positive
                                     # rolling 5D relative return vs SPY
    "pct_top_quartile_20d": 30,     # % of last 20 days ranked top-quartile
                                     # among the universe on that measure
    "trend_component": 20,          # above MA20/MA50 (1.0), above one (0.5), neither (0)
    "rs_ratio_trend": 20,           # primary-benchmark RS ratio rising over 20D
}
# Penalty: subtracted after the weighted sum, for a move that is mostly one
# extreme session rather than a gradual, persistent move.
PERSISTENCE_ONE_DAY_SPIKE_PENALTY = 15
PERSISTENCE_ONE_DAY_SPIKE_RATIO = 0.7  # |1D return| / |5D return| above this triggers the penalty

PERSISTENCE_BANDS = [
    (80, 100, "ESTABLISHED_LEADERSHIP"),
    (60, 80, "PERSISTENT_LEADERSHIP"),
    (40, 60, "MIXED_TRANSITION"),
    (20, 40, "PERSISTENT_WEAKNESS"),
    (0, 20, "ESTABLISHED_LAGGARD"),
]

# ---------------------------------------------------------------------------
# Risk Appetite Score (§15) — 0-100, weights sum to 100
# ---------------------------------------------------------------------------
RISK_APPETITE_WEIGHTS = {
    "XBI": 20,
    "UFO": 15,
    "DRIV": 15,
    "KRE": 10,
    "XLY_XLP_ratio": 15,
    "SOXX": 15,
    "defensive_weakness": 10,  # inverse of avg(XLV, XLP, XLU) relative strength
}
# Percentage-point relative return is mapped onto the 0-100 score via:
#   score = clamp(50 + relative_return_pct * SCALE, 0, 100)
# i.e. a relative return of +-(100/SCALE) percentage points saturates the
# component at 100/0. SCALE=5 => +-10pp relative return saturates.
RISK_APPETITE_PCT_TO_SCORE_SCALE = 5
RISK_APPETITE_HORIZON = "20D"

RISK_APPETITE_BANDS = [
    (75, 100, "STRONG_RISK_ON"),
    (60, 75, "RISK_ON"),
    (40, 60, "MIXED"),
    (25, 40, "RISK_OFF"),
    (0, 25, "STRONG_RISK_OFF"),
]

# ---------------------------------------------------------------------------
# Pairwise rotation detection (§13) — weights sum to 100
# ---------------------------------------------------------------------------
ROTATION_DETECT_WEIGHTS = {
    "rs_ratio_5d_direction": 20,     # short-term RS ratio moving the proposed direction
    "rs_ratio_20d_direction": 20,    # structural RS ratio moving the proposed direction
    "leader_relative_improving": 15,  # proposed leader's rel-vs-benchmark improving (5D vs prior 5D)
    "laggard_relative_deteriorating": 15,  # proposed laggard's rel-vs-benchmark deteriorating
    "correlation_declining": 15,     # 20D correlation below its 60D value / falling
    "volume_confirmation": 15,       # either side showing ELEVATED+ volume
}
ROTATION_DETECT_MIN_CONFIDENCE = 40  # below this, no rotation flag is emitted at all
ROTATION_DETECT_ESTABLISHED_MIN_DURATION = 3  # trading days a run must persist before
                                                 # being called more than "developing"

# ---------------------------------------------------------------------------
# Wave / oscillation detection (§21, §32)
# ---------------------------------------------------------------------------
WAVE_RECENT_WINDOW = 20     # trading days treated as "recent" behavior
WAVE_BASELINE_WINDOW = 252  # trading days (≈1yr) treated as the historical baseline
WAVE_MIN_BASELINE_DAYS = 60  # minimum history required before any baseline comparison is trusted

# A flip-rate deviation is expressed as a z-score against the baseline flip
# probability (binomial approximation). These are the "is this actually
# unusual" gates — everything below the lower bound is left as NO_PATTERN.
WAVE_Z_POSSIBLE_OSCILLATION = 1.5
WAVE_Z_REPEATED_ROTATION = 2.5

# Magnitude floor: average |daily spread| on flip days must exceed this
# multiple of the full-history spread's standard deviation, or the "pattern"
# is judged to be noise-level and not meaningful even if flips are frequent.
WAVE_MAGNITUDE_FLOOR_STD_MULTIPLE = 0.5

# One-side dominance: fraction of recent-window days a single side led,
# above which we call it persistent one-sided leadership rather than a wave.
WAVE_ONE_SIDE_DOMINANCE = 0.75

# ---------------------------------------------------------------------------
# Market regime classification (§14)
# ---------------------------------------------------------------------------
REGIME_MIN_CONFIDENCE = 55   # a regime must score at least this to be declared
                              # outright; otherwise MIXED_ROTATION / TRANSITION
REGIME_TRANSITION_FLIP_THRESHOLD = 2  # # of tracked pairs currently flagged
                                        # POSSIBLE_OSCILLATION/REPEATED_ROTATION
                                        # required to call TRANSITION over MIXED_ROTATION

# ---------------------------------------------------------------------------
# Breadth (§16)
# ---------------------------------------------------------------------------
BREADTH_HORIZONS = ["1D", "5D", "20D"]

# ---------------------------------------------------------------------------
# Rotation matrix (§22) — average category relative return vs SPY, by
# horizon, mapped onto a consistent 5-state label at every horizon (the
# spec's own worked example mixes direction labels and a strength label
# within one row; this module normalizes that into one consistent scale).
# ---------------------------------------------------------------------------
ROTATION_MATRIX_THRESHOLDS = {
    "strong": 0.03,  # avg category relative return vs SPY >= 3pp => STRONG_UP / STRONG_DOWN
    "flat": 0.005,   # within +/-0.5pp => FLAT
}
ROTATION_MATRIX_HORIZONS = ["1D", "5D", "10D", "20D"]

# ---------------------------------------------------------------------------
# Alerts (§29) — require persistence/confirmation before a stronger alert fires
# ---------------------------------------------------------------------------
ALERT_THRESHOLDS = {
    "rotation_developing_min_confidence": 40,
    "rotation_established_min_confidence": 65,
    "rotation_established_min_duration": 3,
    "correlation_breakdown_percentile": 20,   # 20D correlation below this trailing-1y percentile
    "risk_appetite_trend_pts": 8,             # +/- points over 5D to call expanding/contracting
    "breadth_trend_pts": 15,                  # +/- percentage points over 5D to call breadth shift
    "trend_lookback_days": 5,
}
