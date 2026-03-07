import json
from datetime import datetime, UTC

STAGE3_FILE = "stage3_output.json"
STAGE4_FILE = "prime_candidates.json"

TOP_HC = 18
TOP_TA = 12
TOP_SPEC = 30


# ==========================================
# LOAD STAGE 3
# ==========================================

with open(STAGE3_FILE, "r") as f:
    stage3 = json.load(f)

kept = stage3.get("kept", [])
buckets = stage3.get("buckets", {})

lookup = {k["ticker"]: k for k in kept}


def rank_bucket(bucket_list):
    return sorted(
        bucket_list,
        key=lambda t: lookup.get(t, {}).get("score", 0),
        reverse=True
    )


def generate_thesis(data, bucket):
    revenue = data.get("revenue", 0)
    accel = data.get("acceleration", 0)
    margin_delta = data.get("margin_delta", 0)
    fcf_delta = data.get("fcf_delta", 0)

    if bucket == "high_conviction_growth":
        return "Scaled growth business with expanding operating leverage and improving free cash flow."

    if bucket == "operational_turnaround":
        return "Operational turnaround showing margin recovery and improving underlying economics."

    if accel > 0.20:
        return "Smaller-cap growth company with accelerating revenue and improving profitability trends."

    return "Growth-oriented company with improving financial momentum."


def build_output(bucket_name, ranked_list, top_n):
    result = []

    for ticker in ranked_list[:top_n]:
        data = lookup.get(ticker, {})

        result.append({
            "ticker": ticker,
            "company_name": data.get("companyName"),
            "sector": data.get("sector"),
            "industry": data.get("industry"),
            "score": data.get("score"),
            "revenue": data.get("revenue"),
            "thesis": generate_thesis(data, bucket_name)
        })

    return result


# ==========================================
# RANK BUCKETS
# ==========================================

hc_ranked = rank_bucket(buckets.get("high_conviction_growth", []))
ta_ranked = rank_bucket(buckets.get("operational_turnaround", []))
spec_ranked = rank_bucket(buckets.get("speculative_asymmetric", []))

prime = {
    "high_conviction_growth": build_output("high_conviction_growth", hc_ranked, TOP_HC),
    "operational_turnaround": build_output("operational_turnaround", ta_ranked, TOP_TA),
    "speculative_asymmetric": build_output("speculative_asymmetric", spec_ranked, TOP_SPEC)
}

total_prime = sum(len(v) for v in prime.values())

output = {
    "generated_at": datetime.now(UTC).isoformat(),
    "input_total_stage3": stage3.get("kept_total"),
    "prime_total": total_prime,
    "prime_candidates": prime
}

with open(STAGE4_FILE, "w") as f:
    json.dump(output, f, indent=2)

print("\nStage 4 Complete")
print(f"Prime Candidates Total: {total_prime}")
print(f"Output written -> {STAGE4_FILE}")
