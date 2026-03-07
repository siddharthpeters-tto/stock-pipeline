import os
import json
import time
import requests
from datetime import datetime, UTC
from dotenv import load_dotenv
from tqdm import tqdm


# ==========================================
# CONFIG
# ==========================================

BASE_URL = "https://financialmodelingprep.com/stable"
STAGE2_FILE = "stage2_output.json"
STAGE3_FILE = "stage3_output.json"

CALLS_PER_MINUTE = 300
BATCH_SIZE = 250
SLEEP_SECONDS = 60

# --- Reduction Filters ---
MIN_SCORE = 12
MIN_REVENUE = 200_000_000
MAX_SATURATION_PENALTY = -3

# --- Sector / Industry Exclusions ---
EXCLUDE_REITS = True
EXCLUDE_MINERS = True
EXCLUDE_CRYPTO_MINERS = True
EXCLUDE_BDCS = True
EXCLUDE_MLPS = True

EXCLUDE_INDUSTRIES = [
    "Banks",
    "Asset Management",
    "Capital Markets",
    "Oil & Gas Exploration",
    "Oil & Gas Midstream",
    "Regulated Electric",
    "Utilities"
]


# ==========================================
# ENV
# ==========================================

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")
if not API_KEY:
    raise ValueError("FMP_API_KEY missing")

session = requests.Session()


def fetch_profile(ticker):
    url = f"{BASE_URL}/profile?symbol={ticker}&apikey={API_KEY}"
    try:
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return {}
    except:
        return {}


def norm(x):
    return (x or "").lower()


def industry_excluded(industry):
    i = norm(industry)
    for keyword in EXCLUDE_INDUSTRIES:
        if keyword.lower() in i:
            return True
    return False


def is_reit(sector, industry):
    return "reit" in norm(industry) or norm(sector) == "real estate"


def is_miner(industry):
    keywords = ["mining", "gold", "silver", "copper", "coal", "metals"]
    return any(k in norm(industry) for k in keywords)


def is_crypto_miner(industry, name):
    return "bitcoin" in norm(industry) or "crypto" in norm(name)


def is_bdc(industry):
    return "business development" in norm(industry)


def is_mlp(industry):
    return "master limited" in norm(industry)


# ==========================================
# LOAD STAGE 2
# ==========================================

with open(STAGE2_FILE, "r") as f:
    stage2 = json.load(f)

results = stage2.get("results", [])

print(f"Loaded {len(results)} Stage 2 candidates")

# ==========================================
# FETCH PROFILES
# ==========================================

profiles = {}

total_batches = (len(results) // BATCH_SIZE) + 1

for b in range(total_batches):
    batch = results[b * BATCH_SIZE:(b + 1) * BATCH_SIZE]
    if not batch:
        continue

    print(f"\nProfile Batch {b+1}/{total_batches}")

    for r in tqdm(batch):
        ticker = r["ticker"]
        profiles[ticker] = fetch_profile(ticker)

    if b < total_batches - 1:
        time.sleep(SLEEP_SECONDS)

# ==========================================
# FILTER + BUCKET
# ==========================================

kept = []
excluded = []

buckets = {
    "high_conviction_growth": [],
    "operational_turnaround": [],
    "speculative_asymmetric": []
}

for r in results:

    ticker = r["ticker"]
    profile = profiles.get(ticker, {})

    sector = profile.get("sector")
    industry = profile.get("industry")
    name = profile.get("companyName", "")

    score = r.get("total_score", 0)
    revenue = r.get("revenue", 0)
    saturation = r.get("subs", {}).get("saturation", 0)
    accel = r.get("acceleration", 0)
    margin_delta = r.get("margin_delta", 0)
    fcf_delta = r.get("fcf_delta", 0)

    reasons = []

    # --- Sector exclusions ---
    if EXCLUDE_REITS and is_reit(sector, industry):
        reasons.append("reit")

    if EXCLUDE_MINERS and is_miner(industry):
        reasons.append("miner")

    if EXCLUDE_CRYPTO_MINERS and is_crypto_miner(industry, name):
        reasons.append("crypto_miner")

    if EXCLUDE_BDCS and is_bdc(industry):
        reasons.append("bdc")

    if EXCLUDE_MLPS and is_mlp(industry):
        reasons.append("mlp")

    if industry_excluded(industry):
        reasons.append("industry_excluded")

    # --- Structural filters ---
    if score < MIN_SCORE:
        reasons.append("low_score")

    if revenue < MIN_REVENUE:
        reasons.append("low_revenue")

    if saturation <= MAX_SATURATION_PENALTY:
        reasons.append("high_distortion_risk")

    if margin_delta <= 0 and fcf_delta <= 0:
        reasons.append("no_operating_inflection")

    if reasons:
        excluded.append({
            "ticker": ticker,
            "rank": r.get("rank"),
            "score": score,
            "reasons": reasons
        })
        continue

    # --- Bucket classification ---
    if revenue >= 1_000_000_000 and margin_delta > 0.05 and fcf_delta > 0.05:
        buckets["high_conviction_growth"].append(ticker)
    elif margin_delta > 0.08 and accel >= 0:
        buckets["operational_turnaround"].append(ticker)
    else:
        buckets["speculative_asymmetric"].append(ticker)

    kept.append({
        "ticker": ticker,
        "companyName": name,
        "sector": sector,
        "industry": industry,
        "rank": r.get("rank"),
        "score": score,
        "revenue": revenue,
        "acceleration": accel,
        "margin_delta": margin_delta,
        "fcf_delta": fcf_delta,
        "saturation": saturation
    })



# ==========================================
# OUTPUT
# ==========================================

output = {
    "generated_at": datetime.now(UTC).isoformat(),
    "input_total": len(results),
    "kept_total": len(kept),
    "excluded_total": len(excluded),
    "buckets": buckets,
    "kept": kept,
    "excluded": excluded
}

with open(STAGE3_FILE, "w") as f:
    json.dump(output, f, indent=2)

print("\nStage 3 Complete")
print(f"Kept: {len(kept)}")
print(f"Excluded: {len(excluded)}")

for k, v in buckets.items():
    print(f"{k}: {len(v)}")

print(f"\nOutput overwritten -> {STAGE3_FILE}")
