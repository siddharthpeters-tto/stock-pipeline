"""
Stage5-1.py — Structural Quality & Stability Engine

Purpose:
    Identifies financially durable businesses using multi-year structural analysis.
    Stage5-1 evaluates quality independent of valuation.

Inputs:
    - Raw financial statements from FMP:
        • Income Statement (5Y)
        • Cash Flow Statement (5Y)
        • Ratios (5Y)
        • Key Metrics (5Y)

Outputs:
    - level1_results.json

What It Does:

    1) Pulls 5 years of financial history per ticker.

    2) Computes structural durability metrics:
        • 5Y Revenue CAGR
        • Gross margin change (latest vs oldest)
        • Operating margin change
        • Latest ROIC
        • Latest FCF margin
        • 5Y dilution rate

    3) Builds a multi-year stability profile:
        • 5Y median FCF margin
        • 5Y FCF margin volatility (std dev)
        • 5Y ROIC volatility (std dev)

    4) Detects potential cyclical distortion:
        • Flags companies where current margins are significantly above
          historical norms.
        • Flags companies with high ROIC volatility.

    5) Produces a structural quality score (Level 1 score).

Design Philosophy:

    - Focuses on durability, not valuation.
    - Separates structural quality from price.
    - Identifies potential cyclical peaks without penalizing them.
    - Designed to surface resilient compounders before valuation is applied.

Stage5-1 answers:
    “Is this business structurally strong and stable?”

Stage5-2 answers:
    “Is this structurally strong business mispriced today?”
"""

import os
import json
import time
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import requests
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime
current_year = datetime.now().year
load_dotenv()



# =========================
# CONFIG
# =========================

INPUT_PATH = "prime_candidates.json"   # point this to /mnt/data/prime_candidates.json if running locally here
OUTPUT_JSON = "level1_results.json"
OUTPUT_CSV = "level1_results.csv"

BASE_URL = "https://financialmodelingprep.com/stable"
API_KEY = os.getenv("FMP_API_KEY")  # <-- set this in your .env or environment variables

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # or whatever you prefer
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# 5 API calls per ticker
# 300 calls per minute limit
# Safe rate ≈ 42 tickers per minute max
# 60 tickers should take ≥ 90 seconds
SLEEP_BETWEEN_TICKERS = 1.5


# ---------------------------------------
# EDIT ONCE: Map your actual endpoints here
# ---------------------------------------
ENDPOINTS = {
    "income_statement":     "/income-statement",          # FY/quarter available in stable (not -ttm)

    "cash_flow":            "/cash-flow-statement",

    "balance_sheet":        "/balance-sheet-statement",

    "ratios":               "/ratios",

    "key_metrics":          "/key-metrics",

    "transcript_latest":    "/earning-call-transcript",   # NOTE: confirm exact slug in your docs
}



# =========================
# HELPERS
# =========================

def safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Return (a-b)/abs(b) safely."""
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return (a - b) / abs(b)

def to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


# =========================
# API CLIENT
# =========================

@dataclass
class ApiClient:
    base_url: str
    api_key: str

    def get(self, path: str, params: Dict[str, Any]) -> Any:
        url = self.base_url.rstrip("/") + path
        params = params.copy()
        params["apikey"] = self.api_key

        last_err = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    return r.json()
                last_err = f"HTTP {r.status_code}: {r.text[:2000]}"
            except Exception as e:
                last_err = str(e)

            time.sleep(0.5 * attempt)

        raise RuntimeError(f"API GET failed for {url} params={params}. Last error: {last_err}")


# =========================
# DATA PULLS (edit params as needed)
# =========================

def fetch_quant_bundle(api: ApiClient, symbol: str) -> Dict[str, Any]:
    out = {}

    # 5-year annual data (primary structural source)
    out["income"] = api.get(
        "/income-statement",
        {"symbol": symbol, "period": "FY", "limit": 5}
    )

    out["cashflow"] = api.get(
        "/cash-flow-statement",
        {"symbol": symbol, "period": "FY", "limit": 5}
    )

    out["balance"] = api.get(
        "/balance-sheet-statement",
        {"symbol": symbol, "period": "FY", "limit": 5}
    )

    out["ratios"] = api.get(
        "/ratios",
        {"symbol": symbol, "period": "FY", "limit": 5}
    )

    out["key_metrics"] = api.get(
        "/key-metrics",
        {"symbol": symbol, "period": "FY", "limit": 5}
    )

    #print("SEG RAW:", out["segmentation"])


    return out


def fetch_text_bundle(api: ApiClient, symbol: str) -> Dict[str, Any]:
    out = {}

    out["profile"] = api.get(
        "/profile",
        {"symbol": symbol}
    )

    return out

# =========================
# QUANT DERIVATIONS
# =========================

def extract_revenue_series(income_json: Any) -> List[Tuple[str, float]]:
    """
    Return list of (date, revenue) newest->oldest. Adjust key names for your API format.
    """
    series = []
    if isinstance(income_json, list):
        for row in income_json:
            date = row.get("date") or row.get("fiscalDateEnding") or row.get("calendarYear")
            rev = to_float(row.get("revenue") or row.get("revenueTotal") or row.get("totalRevenue"))
            if date and rev is not None:
                series.append((str(date), rev))
    return series

def cagr(new: float, old: float, years: float) -> Optional[float]:
    if new is None or old is None or years <= 0 or old <= 0 or new <= 0:
        return None
    return (new / old) ** (1 / years) - 1

def compute_quant_features(bundle: Dict[str, Any]) -> Dict[str, Any]:
    import statistics

    income = bundle.get("income", [])
    cashflow = bundle.get("cashflow", [])
    ratios = bundle.get("ratios", [])
    keym = bundle.get("key_metrics", [])

    # Ensure newest → oldest ordering
    if isinstance(income, list):
        income = sorted(
            income,
            key=lambda x: x.get("date") or x.get("calendarYear"),
            reverse=True
        )

    if isinstance(ratios, list):
        ratios = sorted(
            ratios,
            key=lambda x: x.get("date") or x.get("calendarYear"),
            reverse=True
        )

    if isinstance(keym, list):
        keym = sorted(
            keym,
            key=lambda x: x.get("date") or x.get("calendarYear"),
            reverse=True
        )

    # -------------------------
    # Revenue CAGR (5Y)
    # -------------------------
    rev_series = []
    if isinstance(income, list):
        for row in income:
            date = row.get("date") or row.get("calendarYear")
            rev = to_float(row.get("revenue"))
            if date and rev:
                rev_series.append((date, rev))

    rev_cagr_5y = None
    if len(rev_series) >= 5:
        rev_new = rev_series[0][1]
        rev_old = rev_series[4][1]
        if rev_old > 0:
            rev_cagr_5y = (rev_new / rev_old) ** (1 / 4) - 1

    # -------------------------
    # Margins (Absolute Change)
    # -------------------------
    gm_latest = to_float(ratios[0].get("grossProfitMargin")) if ratios else None
    gm_old = to_float(ratios[-1].get("grossProfitMargin")) if ratios else None
    gm_delta = (
        gm_latest - gm_old
        if gm_latest is not None and gm_old is not None
        else None
    )

    opm_latest = to_float(ratios[0].get("operatingProfitMargin")) if ratios else None
    opm_old = to_float(ratios[-1].get("operatingProfitMargin")) if ratios else None
    opm_delta = (
        opm_latest - opm_old
        if opm_latest is not None and opm_old is not None
        else None
    )

    # -------------------------
    # ROIC
    # -------------------------
    roic_latest = (
        to_float(keym[0].get("returnOnInvestedCapital"))
        if isinstance(keym, list) and keym
        else None
    )

    # -------------------------
    # FCF Margin (Latest)
    # -------------------------
    fcf_latest = None
    if isinstance(cashflow, list) and cashflow:
        fcf_latest = to_float(cashflow[0].get("freeCashFlow"))

    rev_latest = rev_series[0][1] if rev_series else None

    fcf_margin_latest = (
        fcf_latest / rev_latest
        if fcf_latest is not None and rev_latest
        else None
    )

    # -------------------------
    # Dilution (5Y)
    # -------------------------
    shares_latest = (
        to_float(income[0].get("weightedAverageShsOut"))
        if isinstance(income, list) and income
        else None
    )

    shares_old = (
        to_float(income[4].get("weightedAverageShsOut"))
        if isinstance(income, list) and len(income) >= 5
        else None
    )

    dilution_5y = (
        (shares_latest - shares_old) / shares_old
        if shares_latest and shares_old and shares_old != 0
        else None
    )

    # =====================================================
    # CYCLE PROFILE ANALYSIS (5Y STABILITY & DISTORTION)
    # =====================================================

    # ----- 5Y FCF Margin Series -----
    fcf_margins = []

    if isinstance(cashflow, list) and isinstance(income, list):
        for i in range(min(len(cashflow), len(income))):
            fcf_val = to_float(cashflow[i].get("freeCashFlow"))
            rev_val = to_float(income[i].get("revenue"))

            if fcf_val is not None and rev_val and rev_val != 0:
                fcf_margins.append(fcf_val / rev_val)

    fcf_margin_median_5y = None
    fcf_margin_std_5y = None

    if len(fcf_margins) >= 3:
        fcf_margin_median_5y = statistics.median(fcf_margins)
        fcf_margin_std_5y = statistics.pstdev(fcf_margins)

    # ----- 5Y ROIC Volatility -----
    roic_series = []

    if isinstance(keym, list):
        for row in keym:
            r = to_float(row.get("returnOnInvestedCapital"))
            if r is not None:
                roic_series.append(r)

    roic_std_5y = None

    if len(roic_series) >= 3:
        roic_std_5y = statistics.pstdev(roic_series)

    # ----- Cycle Distortion Flag -----
    cycle_distortion_flag = False

    # Rule 1: Current FCF margin much higher than historical median
    if (
        fcf_margin_latest is not None and
        fcf_margin_median_5y is not None and
        fcf_margin_median_5y > 0
    ):
        if fcf_margin_latest > (fcf_margin_median_5y * 2):
            cycle_distortion_flag = True

    # Rule 2: High ROIC volatility over 5Y
    if roic_std_5y is not None and roic_std_5y > 0.15:
        cycle_distortion_flag = True

    return {
        "rev_cagr_5y": rev_cagr_5y,
        "gross_margin_latest": gm_latest,
        "gross_margin_delta": gm_delta,
        "op_margin_latest": opm_latest,
        "op_margin_delta": opm_delta,
        "roic_latest": roic_latest,
        "fcf_margin_latest": fcf_margin_latest,
        "dilution_5y": dilution_5y,

        # Cycle transparency fields
        "fcf_margin_median_5y": fcf_margin_median_5y,
        "fcf_margin_std_5y": fcf_margin_std_5y,
        "roic_std_5y": roic_std_5y,
        "cycle_distortion_flag": cycle_distortion_flag,
    }

# =========================
# SEGMENTATION NORMALIZATION
# =========================

def normalize_segmentation(segmentation: Any) -> Dict[str, Any]:
    if not isinstance(segmentation, list) or not segmentation:
        return {
            "largest_segment_name": None,
            "largest_segment_share": None,
            "segment_notes": None,
        }

    # Sort newest fiscal year first
    segmentation = sorted(
        segmentation,
        key=lambda x: x.get("fiscalYear", 0),
        reverse=True
    )

    latest = segmentation[0]
    data = latest.get("data", {})

    if not isinstance(data, dict) or not data:
        return {
            "largest_segment_name": None,
            "largest_segment_share": None,
            "segment_notes": None,
        }

    total = sum(to_float(v) or 0 for v in data.values())

    if total == 0:
        return {
            "largest_segment_name": None,
            "largest_segment_share": None,
            "segment_notes": None,
        }

    largest_segment = max(data.items(), key=lambda x: to_float(x[1]) or 0)

    return {
        "largest_segment_name": largest_segment[0],
        "largest_segment_share": (to_float(largest_segment[1]) or 0) / total,
        "segment_notes": None,
    }



# =========================
# GPT EXTRACTION
# =========================

GPT_SCHEMA = """
Return JSON ONLY (no markdown, no commentary) with this exact schema:

{
  "business_one_liner": "string",
  "primary_revenue_model": "string",
  "core_customer_type": "string",

  "structural_strengths": ["string", "string"],
  "structural_weaknesses": ["string", "string"],

  "industry_characteristics": "asset_heavy|asset_light|regulated|commodity_exposed|platform|recurring_revenue|cyclical|unclear",

  "demand_visibility": "high|moderate|low|unclear",
  "pricing_power": "strong|moderate|weak|unclear",
  "competitive_intensity": "high|medium|low|unclear",

  "key_business_risks": ["string", "string"]
}

Rules:
- Use ONLY the company description and basic profile information provided.
- Do NOT use valuation metrics.
- Do NOT make investment recommendations.
- Do NOT classify quality level.
- Do NOT infer financial strength beyond description.
- Keep each string concise (<= 20 words).
- If information is not available, use "unclear".
"""


def call_gpt_for_level1(symbol: str, combined_text: str) -> Dict[str, Any]:

    prompt = f"""
You are an institutional equity analyst doing Level-1 triage.
Use ONLY the provided company description and earnings transcripts.

{GPT_SCHEMA}

COMPANY: {symbol}
If the provided material does not clearly support an answer, return "unclear".
Do NOT infer or assume information not explicitly stated.


Source Material:
{combined_text}
""".strip()

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    raw = resp.choices[0].message.content.strip()

    try:
        return json.loads(raw)
    except Exception:
        return {"_parse_error": True, "_raw": raw}


def build_gpt_input(symbol, text_bundle):
    profile_text = ""

    profile = text_bundle.get("profile")
    if isinstance(profile, list) and profile:
        profile_text = profile[0].get("description", "")

    return f"""
Company: {symbol}

Business Description:
{profile_text}
"""



def extract_transcript_texts(transcripts_json: Any) -> List[str]:
    """
    Your transcript endpoint example showed fields:
    {"symbol": "...", "period": "Q3", "year": 2020, "date": "...", "content": "..."}
    """
    texts = []
    if isinstance(transcripts_json, list):
        for t in transcripts_json:
            if isinstance(t, dict):
                c = t.get("content") or t.get("text") or ""
                if isinstance(c, str) and c.strip():
                    texts.append(c.strip())
    elif isinstance(transcripts_json, dict):
        c = transcripts_json.get("content") or transcripts_json.get("text") or ""
        if isinstance(c, str) and c.strip():
            texts.append(c.strip())
    return texts


# =========================
# SCORING
# =========================

def compute_kill_flags(quant: Dict[str, Any]) -> List[str]:
    flags = []

    if quant.get("rev_cagr_5y") is not None and quant["rev_cagr_5y"] < 0.05:
        flags.append("Low 5Y revenue CAGR (<5%)")

    if quant.get("roic_latest") is not None and quant["roic_latest"] < 0.08:
        flags.append("Low ROIC (<8%)")

    if quant.get("gross_margin_delta") is not None and quant["gross_margin_delta"] <= -0.10:
        flags.append("Gross margin deterioration (>=10 pts)")

    if quant.get("op_margin_delta") is not None and quant["op_margin_delta"] <= -0.10:
        flags.append("Operating margin deterioration (>=10 pts)")

    if quant.get("fcf_margin_latest") is not None and quant["fcf_margin_latest"] < 0:
        flags.append("Negative FCF margin")

    if quant.get("dilution_5y") is not None and quant["dilution_5y"] > 0.20:
        flags.append("High dilution (>20% over period)")


    return flags

def score_company(quant: Dict[str, Any], gpt: Dict[str, Any]) -> float:
    """
    Simple, explainable scoring. Tune weights over time.
    """
    score = 0.0

    # Growth
    rc = quant.get("rev_cagr_5y")
    if rc is not None:
        if rc >= 0.20: score += 5
        elif rc >= 0.12: score += 3
        elif rc >= 0.07: score += 1
        else: score -= 2

    # ROIC
    roic = quant.get("roic_latest")
    if roic is not None:
        if roic >= 0.18: score += 5
        elif roic >= 0.12: score += 3
        elif roic >= 0.08: score += 1
        else: score -= 3

    # FCF margin
    fcfm = quant.get("fcf_margin_latest")
    if fcfm is not None:
        if fcfm >= 0.15: score += 4
        elif fcfm >= 0.08: score += 2
        elif fcfm >= 0.00: score += 0
        else: score -= 3

    # GPT signals (kept smaller than quant)
    if isinstance(gpt, dict) and not gpt.get("_parse_error"):
        if gpt.get("pricing_power_signal") == "strong": score += 2
        if gpt.get("margin_direction_signal") == "expanding": score += 2
        if gpt.get("competitive_intensity") == "high": score -= 1
        if gpt.get("customer_concentration_risk") == "high": score -= 2
        mcs = gpt.get("management_credibility_score")
        if isinstance(mcs, int):
            score += (mcs - 3) * 0.75  # 1..5 -> -1.5 .. +1.5

    if quant.get("cycle_distortion_flag"):
        score -= 2

    return float(score)


# =========================
# MAIN
# =========================

def load_prime60(path: str) -> List[Dict[str, Any]]:
    with open(path, "r") as f:
        data = json.load(f)

    pc = data["prime_candidates"]
    out = []
    for bucket, items in pc.items():
        for it in items:
            out.append({
                "bucket": bucket,
                "ticker": it["ticker"],
                "company_name": it.get("company_name"),
                "sector": it.get("sector"),
                "industry": it.get("industry"),
                "stage4_score": it.get("score"),
                "stage4_thesis": it.get("thesis"),
                "revenue_stage4": it.get("revenue"),
            })
    return out

def write_csv(rows: List[Dict[str, Any]], path: str):
    import csv
    if not rows:
        return
    # Flatten keys union
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    api = ApiClient(BASE_URL, API_KEY)
    universe = load_prime60(INPUT_PATH)

    # ----------------------------------
    # Resume support: load existing file
    # ----------------------------------
    results = []
    processed_symbols = set()

    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                results = json.load(f)
                processed_symbols = {r["ticker"] for r in results if "ticker" in r}
                print(f"Resuming. Found {len(processed_symbols)} already processed.")
        except Exception:
            print("Existing output file unreadable. Starting fresh.")
            results = []
            processed_symbols = set()

    total = len(universe)

    for i, row in enumerate(universe, 1):
        symbol = row["ticker"]

        if symbol in processed_symbols:
            print(f"[{i}/{total}] {symbol} already processed. Skipping.")
            continue

        print(f"[{i}/{total}] Processing {symbol}...")

        try:
            # -----------------------
            # Quant Pull
            # -----------------------
            quant_bundle = fetch_quant_bundle(api, symbol)
            quant = compute_quant_features(quant_bundle)
            
            gpt_out = {}


            # -----------------------
            # Scoring
            # -----------------------
            flags = compute_kill_flags(quant)
            lvl1_score = score_company(quant, gpt_out)

            result_row = {
                **row,
                **quant,
                "kill_flags": flags,
                "level1_score": lvl1_score,
                "gpt": gpt_out,
            }

        except Exception as e:
            result_row = {
                **row,
                "error": str(e),
            }

        results.append(result_row)

        # ----------------------------------
        # Incremental Save After Each Ticker
        # ----------------------------------
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        time.sleep(SLEEP_BETWEEN_TICKERS)

    # ----------------------------------
    # Final Ranking
    # ----------------------------------
    results_sorted = sorted(
        results,
        key=lambda r: (
            r.get("level1_score") is not None,
            r.get("level1_score", -1e9)
        ),
        reverse=True
    )

    # Overwrite JSON with ranked version
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results_sorted, f, indent=2)


    print(f"Done. Wrote ranked results to {OUTPUT_JSON}.")


if __name__ == "__main__":
    main()
