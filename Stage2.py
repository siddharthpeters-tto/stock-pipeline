import json
import math
from datetime import datetime, UTC

# ==========================================
# CONFIG
# ==========================================

STAGE1_FILE = "stage1_output.json"
STAGE2_FILE = "stage2_output.json"

MIN_REVENUE = 150_000_000          # raised slightly vs 100M to reduce micro-base spikes
MIN_GROSS_MARGIN = 0.25
MAX_NET_DEBT_EBITDA = 4

# Caps are only used FOR SCORING (we still report raw deltas)
DELTA_CAP_FOR_SCORE = 0.35         # tighter than 0.50 to reduce saturation
ACCEL_CAP_FOR_SCORE = 0.40         # tighter than 1.00 to reduce saturation

# Turnaround quality: require revenue scale for “flip bonuses”
TURNAROUND_MIN_REVENUE = 300_000_000

# Penalize saturation (hitting caps is often base-effect distortion)
SATURATION_PENALTY = 1.5           # applied per saturated metric

PREVIEW_TOP_N = 20


# ==========================================
# LOAD DATA
# ==========================================

with open(STAGE1_FILE, "r") as f:
    stage1 = json.load(f)

tickers = stage1.get("tickers", {})


# ==========================================
# HELPERS
# ==========================================

def cap_for_score(value, limit):
    return max(min(value, limit), -limit)

def is_saturated(raw_value, capped_value, eps=1e-12):
    return abs(raw_value - capped_value) > eps


# ==========================================
# SCORING ENGINE
# ==========================================

def score_company(m):
    revenue = m.get("revenue", 0) or 0
    cagr_2y = m.get("revenue_cagr_2y", 0) or 0
    growth_1y = m.get("revenue_1y_growth", 0) or 0

    op_margin = m.get("operating_margin", 0) or 0
    op_margin_2y = m.get("operating_margin_2y", 0) or 0

    fcf_margin = m.get("fcf_margin", 0) or 0
    fcf_margin_2y = m.get("fcf_margin_2y", 0) or 0

    gross_margin = m.get("gross_margin", 0) or 0
    net_debt = m.get("net_debt_to_ebitda")

    # ---------------------------------
    # HARD QUALITY FILTERS
    # ---------------------------------
    if revenue < MIN_REVENUE:
        return None
    if gross_margin < MIN_GROSS_MARGIN:
        return None
    if net_debt is not None and net_debt > MAX_NET_DEBT_EBITDA:
        return None

    # ---------------------------------
    # RAW METRICS (TRUTH)
    # ---------------------------------
    raw_accel = growth_1y - cagr_2y
    raw_margin_delta = op_margin - op_margin_2y
    raw_fcf_delta = fcf_margin - fcf_margin_2y

    # ---------------------------------
    # MULTI-SIGNAL INFLECTION FILTER
    # ---------------------------------
    signal_count = 0

    if raw_accel > 0.05:
        signal_count += 1

    if raw_margin_delta > 0.03:
        signal_count += 1

    if raw_fcf_delta > 0.03:
        signal_count += 1

    if signal_count < 2:
        print("Filtered Out:", raw_accel, raw_margin_delta, raw_fcf_delta)
        return None
    # ---------------------------------
    # If Multisignal not met, it doesn't pass the ticker
    # ---------------------------------

    # ---------------------------------
    # CAPPED METRICS (FOR SCORING)
    # ---------------------------------
    accel = cap_for_score(raw_accel, ACCEL_CAP_FOR_SCORE)
    margin_delta = cap_for_score(raw_margin_delta, DELTA_CAP_FOR_SCORE)
    fcf_delta = cap_for_score(raw_fcf_delta, DELTA_CAP_FOR_SCORE)

    # Track saturation
    sat_accel = is_saturated(raw_accel, accel)
    sat_margin = is_saturated(raw_margin_delta, margin_delta)
    sat_fcf = is_saturated(raw_fcf_delta, fcf_delta)

    # ---------------------------------
    # SUBSCORES
    # ---------------------------------
    score = 0
    s_growth = 0
    s_accel = 0
    s_cagr = 0
    s_margin = 0
    s_margin_flip = 0
    s_op_quality = 0
    s_fcf = 0
    s_fcf_flip = 0
    s_fcf_quality = 0
    s_balance = 0
    s_scale = 0
    s_saturation = 0

    # ---------------------------------
    # REVENUE MOMENTUM + DURABILITY
    # ---------------------------------
    if growth_1y > 0.20:
        s_growth += 3
    elif growth_1y > 0.10:
        s_growth += 2
    elif growth_1y > 0.05:
        s_growth += 1
    elif growth_1y < -0.05:
        s_growth -= 2

    if accel > 0.05:
        s_accel += 2
    elif accel < -0.05:
        s_accel -= 1

    if cagr_2y > 0.15:
        s_cagr += 2
    elif cagr_2y > 0.07:
        s_cagr += 1
    elif cagr_2y < 0:
        s_cagr -= 2

    # ---------------------------------
    # MARGIN INFLECTION
    # ---------------------------------
    if margin_delta > 0.10:
        s_margin += 3
    elif margin_delta > 0.05:
        s_margin += 2
    elif margin_delta > 0.02:
        s_margin += 1
    elif margin_delta < -0.05:
        s_margin -= 2

    # Turnaround bonus (only if revenue scale is meaningful)
    if revenue >= TURNAROUND_MIN_REVENUE and op_margin_2y < 0 and op_margin > 0 and growth_1y >= 0:
        s_margin_flip += 3

    # High margin quality
    if op_margin > 0.20:
        s_op_quality += 2
    elif op_margin > 0.12:
        s_op_quality += 1

    # ---------------------------------
    # FCF INFLECTION
    # ---------------------------------
    if fcf_delta > 0.10:
        s_fcf += 3
    elif fcf_delta > 0.05:
        s_fcf += 2
    elif fcf_delta > 0.02:
        s_fcf += 1
    elif fcf_delta < -0.05:
        s_fcf -= 2

    # FCF turnaround bonus (only if revenue scale is meaningful)
    if revenue >= TURNAROUND_MIN_REVENUE and fcf_margin_2y < 0 and fcf_margin > 0 and growth_1y >= 0:
        s_fcf_flip += 2

    # Strong FCF quality
    if fcf_margin > 0.15:
        s_fcf_quality += 2
    elif fcf_margin > 0.08:
        s_fcf_quality += 1
    elif fcf_margin < 0:
        s_fcf_quality -= 1

    # ---------------------------------
    # Balance sheet
    # ---------------------------------
    if net_debt is not None:
        try:
            nd = float(net_debt)
            if nd < 1:
                s_balance += 2
            elif nd < 2:
                s_balance += 1
            elif nd > 3.5:
                s_balance -= 1
        except (TypeError, ValueError):
            pass

    # ---------------------------------
    # Scale bonus (log, controlled)
    # ---------------------------------
    # $100M = 0, $1B ≈ +1, $10B ≈ +2
    s_scale += max(0, min(2, math.log10(revenue) - 8))

    # ---------------------------------
    # Saturation penalty
    # ---------------------------------
    # If you’re hitting caps, we want to discount it (often base effects)
    if sat_accel:
        s_saturation -= SATURATION_PENALTY
    if sat_margin:
        s_saturation -= SATURATION_PENALTY
    if sat_fcf:
        s_saturation -= SATURATION_PENALTY

    score = (
        s_growth + s_accel + s_cagr +
        s_margin + s_margin_flip + s_op_quality +
        s_fcf + s_fcf_flip + s_fcf_quality +
        s_balance + s_scale + s_saturation
    )

    return {
        "total_score": round(score, 2),

        # CAPPED (used for scoring, stable for ranking)
        "acceleration": accel,
        "margin_delta": margin_delta,
        "fcf_delta": fcf_delta,

        # RAW (truth for human review)
        "raw_acceleration": raw_accel,
        "raw_margin_delta": raw_margin_delta,
        "raw_fcf_delta": raw_fcf_delta,

        "revenue": revenue,

        "subs": {
            "growth_1y": round(s_growth, 2),
            "accel": round(s_accel, 2),
            "cagr_2y": round(s_cagr, 2),
            "margin_delta": round(s_margin, 2),
            "margin_flip": round(s_margin_flip, 2),
            "op_quality": round(s_op_quality, 2),
            "fcf_delta": round(s_fcf, 2),
            "fcf_flip": round(s_fcf_flip, 2),
            "fcf_quality": round(s_fcf_quality, 2),
            "balance": round(s_balance, 2),
            "scale": round(s_scale, 2),
            "saturation": round(s_saturation, 2),
        }
    }


# ==========================================
# RUN
# ==========================================
results = []

for ticker, metrics in tickers.items():
    scored = score_company(metrics)
    if scored:
        results.append({"ticker": ticker, **scored})

results = sorted(
    results,
    key=lambda x: (
        x["total_score"],
        x["margin_delta"],
        x["fcf_delta"],
        x["acceleration"],
        x["revenue"]
    ),
    reverse=True
)

for i, r in enumerate(results):
    r["rank"] = i + 1

output = {
    "generated_at": datetime.now(UTC).isoformat(),
    "total_ranked": len(results),
    "config": {
        "MIN_REVENUE": MIN_REVENUE,
        "MIN_GROSS_MARGIN": MIN_GROSS_MARGIN,
        "MAX_NET_DEBT_EBITDA": MAX_NET_DEBT_EBITDA,
        "DELTA_CAP_FOR_SCORE": DELTA_CAP_FOR_SCORE,
        "ACCEL_CAP_FOR_SCORE": ACCEL_CAP_FOR_SCORE,
        "TURNAROUND_MIN_REVENUE": TURNAROUND_MIN_REVENUE,
        "SATURATION_PENALTY": SATURATION_PENALTY
    },
    "results": results
}

with open(STAGE2_FILE, "w") as f:
    json.dump(output, f, indent=4)

print("\nStage 2 Complete")
print(f"Ranked companies: {len(results)}\n")

print(f"Top {PREVIEW_TOP_N} Opportunities:\n")
for r in results[:PREVIEW_TOP_N]:
    print(
        f"{r['rank']:>3}. {r['ticker']} | "
        f"Score: {r['total_score']} | "
        f"Accel: {r['acceleration']:.2%} (raw {r['raw_acceleration']:.2%}) | "
        f"OpΔ: {r['margin_delta']:.2%} (raw {r['raw_margin_delta']:.2%}) | "
        f"FCFΔ: {r['fcf_delta']:.2%} (raw {r['raw_fcf_delta']:.2%})"
    )
