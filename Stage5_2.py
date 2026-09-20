"""
Stage5-2.py — Dynamic Quality-Adjusted Valuation + Peer Context

Purpose:
    Converts Stage5-1 structural outputs into real-time valuation decisions
    using live market data and rebuilt enterprise value calculations.

Inputs:
    - level1_results.json (output of Stage5-1 structural scoring)

Outputs:
    - level_2_results.json (ranked deterministic valuation results)
    - level_2_ranked.csv

What it does:

    1) Loads Stage5-1 results (filtered structural candidates).

    2) For each ticker:
        - Pulls latest quote (live price + market cap)
        - Pulls latest key-metrics (FY snapshot)
        - Pulls latest income statement (FY)
        - Pulls latest cash flow statement (FY)
        - Pulls latest balance sheet (FY)
        - Pulls peer list
        - Pulls peer key-metrics (limit=1) to compute peer medians

    3) Rebuilds Enterprise Value dynamically:
        EV = Live Market Cap + Total Debt − Cash

    4) Computes dynamic valuation metrics:
        - EV/FCF
        - EV/EBITDA
        - EV/Sales
        - FCF Yield
        - Earnings Yield

       (All valuation metrics reflect today’s price, not fiscal snapshot EV.)

    5) Integrates structural quality metrics from Stage5-1:
        - ROIC
        - Revenue CAGR
        - FCF margin
        - Dilution (5Y)
        - SBC / Revenue
        - Net Debt / EBITDA
        - Segment concentration

    6) Computes peer-relative comparisons:
        - Peer median EV/FCF
        - Peer median ROIC

    7) Classifies each company into valuation-quality quadrants:
        - HQ_Cheap
        - HQ_Expensive
        - LQ_Cheap
        - LQ_Expensive
        - Mixed/Unclear

    8) Generates composite Quality-Adjusted Value (QAV) score.

Design Philosophy:

    - Financial quality changes quarterly.
    - Valuation changes daily.
    - Stage5-2 reflects real-time mispricing.
    - Strong Buy means undervalued at today’s price.
    - No reliance on stale snapshot multiples.

Notes:

    - Uses FMP /stable base URL.
    - Rebuilds EV from raw financial statements.
    - Respects 300 calls/min with throttling.
    - Designed for structural compounder discovery, not cyclical peaks.

Requirements:
    pip install requests python-dotenv
"""

import os
import json
import time
import math
import csv
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

import requests
from dotenv import load_dotenv

from fmp_helpers import FmpRequestCounter, normalize_fmp_rows, normalize_symbol, safe_float
from stock_research.fmp_common import median, safe_div, to_float, clamp
from stock_research.peer_data import peer_symbols_from_list
from stock_research.quarterly import compute_quarterly_pulse, fetch_quarterly_bundle

# =========================
# CONFIG
# =========================

load_dotenv()

BASE_URL = os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/stable")
FMP_API_KEY = os.getenv("FMP_API_KEY")
CACHE_DIR = "api_cache"
CACHE_TTL = 900  # seconds (15 minutes)

os.makedirs(CACHE_DIR, exist_ok=True)

if not FMP_API_KEY:
    raise RuntimeError("Missing FMP_API_KEY in environment/.env")

# Inputs / outputs
INPUT_LEVEL1_JSON = os.getenv("INPUT_LEVEL1_JSON", "level1_results.json")
OUT_JSON = os.getenv("OUT_STAGE5_2_JSON", "level2_results.json")
OUT_CSV = os.getenv("OUT_STAGE5_2_CSV", "level2_ranked.csv")

# Universe handling
INCLUDE_ERROR_ROWS = False  # False = skip tickers that had Stage5-1 errors
TOP_N = None  # e.g. 20 to run only top 20 by level1_score, or None to run all

# Rate limiting: 300 calls/min => 5 calls/sec. We'll target ~3.5 calls/sec safely.
SLEEP_BETWEEN_CALLS = 0.30
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# Peers
MAX_PEERS = 8        # cap peers to avoid excessive API usage
FETCH_PEER_METRICS = True  # if False, we only store peer tickers (still safe)

# =========================
# HELPERS
# =========================

def cache_path(symbol: str, endpoint: str, params: Optional[Dict[str, Any]] = None):
    suffix = ""
    params = params or {}
    for key in ("period", "limit"):
        if key in params:
            suffix += f"_{params[key]}"
    return os.path.join(CACHE_DIR, f"{symbol}_{endpoint}{suffix}.json")


def read_cache(symbol: str, endpoint: str, params: Optional[Dict[str, Any]] = None):

    path = cache_path(symbol, endpoint, params)

    if not os.path.exists(path):
        return None

    age = time.time() - os.path.getmtime(path)

    if age > CACHE_TTL:
        return None

    try:
        with open(path, "r") as f:
            payload = json.load(f)
        if isinstance(payload, (list, dict)):
            return payload
        return None
    except Exception:
        try:
            os.remove(path)
        except OSError:
            pass
        return None


def write_cache(
    symbol: str,
    endpoint: str,
    data,
    params: Optional[Dict[str, Any]] = None,
):

    path = cache_path(symbol, endpoint, params)

    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except:
        pass



# =========================
# API CLIENT (FMP stable)
# =========================

@dataclass
class FmpClient:
    base_url: str
    api_key: str
    request_counter: Optional[FmpRequestCounter] = None

    def get(self, path: str, params: Dict[str, Any]) -> Any:
        url = self.base_url.rstrip("/") + path
        params = dict(params or {})
        params["apikey"] = self.api_key

        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if self.request_counter is not None:
                    self.request_counter.record(path)
                r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    payload = r.json()
                    if isinstance(payload, dict) and "Error Message" in payload:
                        raise RuntimeError(payload.get("Error Message", "FMP error"))
                    time.sleep(SLEEP_BETWEEN_CALLS)
                    if isinstance(payload, list):
                        return normalize_fmp_rows(payload)
                    return payload
                last_err = f"HTTP {r.status_code}: {r.text[:2000]}"
            except Exception as e:
                last_err = str(e)

            time.sleep(0.5 * attempt)

        raise RuntimeError(f"API GET failed for {url} params={params}. Last error: {last_err}")


# =========================
# FETCHERS
# =========================
# =========================
# FETCHERS
# =========================

def fetch_live_quote(api: FmpClient, symbol: str) -> Dict[str, Any]:
    data = api.get("/quote", {"symbol": symbol})
    if isinstance(data, list) and data:
        return data[0]
    return {}


def fetch_latest_key_metrics(api: FmpClient, symbol: str):
    params = {"period": "FY", "limit": 1}

    cached = read_cache(symbol, "key_metrics", params)
    if cached:
        return cached

    data = api.get("/key-metrics", {"symbol": symbol, **params})

    if isinstance(data, list) and data:
        result = data[0]
        write_cache(symbol, "key_metrics", result, params)
        return result

    return {}

def fetch_latest_ratios(api: FmpClient, symbol: str):
    params = {"period": "FY", "limit": 1}

    cached = read_cache(symbol, "ratios", params)
    if cached:
        return cached

    data = api.get("/ratios", {"symbol": symbol, **params})

    if isinstance(data, list) and data:
        result = data[0]
        write_cache(symbol, "ratios", result, params)
        return result

    return {}

def peer_symbols_from_list(peers, exclude_symbol: str = None):
    cleaned = []
    for item in peers or []:
        if isinstance(item, dict):
            candidate = item.get("symbol")
        elif isinstance(item, str):
            candidate = item
        else:
            candidate = None
        symbol_value = normalize_symbol(candidate)
        if not symbol_value:
            continue
        if exclude_symbol and symbol_value.upper() == exclude_symbol.upper():
            continue
        cleaned.append(symbol_value)
    return cleaned[:MAX_PEERS]


def fetch_stock_peers(api: FmpClient, symbol: str):

    def clean_peer_list(items):
        cleaned = []
        for item in items or []:
            if isinstance(item, dict):
                symbol_value = normalize_symbol(item.get("symbol"))
                if symbol_value:
                    cleaned.append({"symbol": symbol_value})
            elif isinstance(item, str):
                symbol_value = normalize_symbol(item)
                if symbol_value:
                    cleaned.append({"symbol": symbol_value})
        return cleaned

    cached = read_cache(symbol, "peers")
    if cached:
        cleaned = clean_peer_list(cached)
        if cleaned:
            return cleaned

    data = api.get("/stock-peers", {"symbol": symbol})

    if isinstance(data, list):
        cleaned = clean_peer_list(data)
        write_cache(symbol, "peers", cleaned)
        return cleaned

    return []

def fetch_latest_income_statement(api: FmpClient, symbol: str):
    params = {"period": "FY", "limit": 1}

    cached = read_cache(symbol, "income", params)
    if cached:
        return cached

    data = api.get("/income-statement", {"symbol": symbol, **params})

    if isinstance(data, list) and data:
        result = data[0]
        write_cache(symbol, "income", result, params)
        return result

    return {}

def fetch_latest_cashflow_statement(api: FmpClient, symbol: str):
    params = {"period": "FY", "limit": 1}

    cached = read_cache(symbol, "cashflow", params)
    if cached:
        return cached

    data = api.get("/cash-flow-statement", {"symbol": symbol, **params})

    if isinstance(data, list) and data:
        result = data[0]
        write_cache(symbol, "cashflow", result, params)
        return result

    return {}

def fetch_latest_balance_sheet(api: FmpClient, symbol: str):
    params = {"period": "FY", "limit": 1}

    cached = read_cache(symbol, "balance", params)
    if cached:
        return cached

    data = api.get("/balance-sheet-statement", {"symbol": symbol, **params})

    if isinstance(data, list) and data:
        result = data[0]
        write_cache(symbol, "balance", result, params)
        return result

    return {}


def latest_target_fundamentals(bundle: Optional[Dict[str, Any]]) -> Optional[Dict[str, Dict[str, Any]]]:
    if not isinstance(bundle, dict):
        return None

    result = {}
    for source in ("income", "cashflow", "balance", "ratios", "key_metrics"):
        value = bundle.get(source)
        if isinstance(value, list):
            rows = [row for row in value if isinstance(row, dict)]
            rows.sort(
                key=lambda row: row.get("date") or row.get("fiscalYear") or "",
                reverse=True,
            )
            result[source] = rows[0] if rows else {}
        elif isinstance(value, dict):
            result[source] = value
        else:
            result[source] = {}

    return result



# =========================
# SCORING / CLASSIFICATION
# =========================

def build_value_quality_inputs(level1_row: Dict[str, Any], km: Dict[str, Any], ratios: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes structural metrics.
    Valuation metrics will be calculated dynamically using live price later.
    """

    roic = to_float(km.get("returnOnInvestedCapital"))
    if roic is not None and roic > 0.80:
        roic = None  # guard against denominator distortion

    # Structural metrics only (NO static EV multiples)
    sbc_rev = to_float(km.get("stockBasedCompensationToRevenue"))
    net_debt_ebitda = to_float(km.get("netDebtToEBITDA"))

    # Bring over Stage5-1 computed metrics
    rev_cagr = to_float(level1_row.get("rev_cagr_5y"))
    fcf_margin = to_float(level1_row.get("fcf_margin_latest"))
    dilution = to_float(level1_row.get("dilution_5y"))
    level1_score = to_float(level1_row.get("level1_score"))

    return {
        "roic": roic,
        "ev_to_fcf": None,
        "ev_to_ebitda": None,
        "ev_to_sales": None,
        "fcf_yield": None,
        "earnings_yield": None,
        "sbc_to_revenue": sbc_rev,
        "net_debt_to_ebitda": net_debt_ebitda,
        "rev_cagr_5y": rev_cagr,
        "fcf_margin": fcf_margin,
        "dilution_5y": dilution,
        "level1_score": level1_score,
    }


def quality_bucket(m: Dict[str, Any]) -> str:
    roic = m.get("roic")
    fcfm = m.get("fcf_margin")
    dil = m.get("dilution_5y")

    score = 0

    # ROIC (absolute threshold required)
    if roic is not None:
        if roic >= 0.18:
            score += 2
        elif roic >= 0.12:
            score += 1
        else:
            score -= 1

    # FCF margin
    if fcfm is not None:
        if fcfm >= 0.15:
            score += 2
        elif fcfm >= 0.08:
            score += 1
        else:
            score -= 1

    # Dilution
    if dil is not None:
        if dil <= 0.05:
            score += 1
        elif dil >= 0.25:
            score -= 2

    if score >= 4:
        return "high_quality"
    if score <= 0:
        return "low_quality"
    return "mid_quality"


def valuation_bucket(m: Dict[str, Any]) -> str:
    """
    Institutional valuation bands.
    Uses EV/FCF primarily, falls back to FCF yield.
    Negative or zero denominators are non-applicable for positive valuation scoring.
    """

    ev_fcf = m.get("ev_to_fcf")
    fcf_y = m.get("fcf_yield")
    ev_fcf_applicable = bool(m.get("ev_to_fcf_applicable", ev_fcf is not None and ev_fcf > 0))
    fcf_y_applicable = bool(m.get("fcf_yield_applicable", fcf_y is not None and fcf_y > 0))

    # --- Primary: EV / FCF ---
    if ev_fcf_applicable and ev_fcf is not None and ev_fcf > 0:

        if ev_fcf <= 15:
            return "cheap"

        if 15 < ev_fcf <= 25:
            return "fair"

        if 25 < ev_fcf < 30:
            return "fair"

        if ev_fcf >= 30:
            return "expensive"

    # --- Fallback: FCF Yield ---
    if fcf_y_applicable and fcf_y is not None and fcf_y > 0:

        if fcf_y >= 0.07:   # ~14x implied
            return "cheap"

        if 0.04 <= fcf_y < 0.07:
            return "fair"

        if fcf_y < 0.04:
            return "expensive"

    return "unclear"


def quadrant(quality: str, val: str) -> str:

    if quality == "high_quality":
        if val == "cheap":
            return "HQ_Cheap"
        if val == "fair":
            return "HQ_FairValue"
        if val == "expensive":
            return "HQ_Expensive"

    if quality == "mid_quality":
        if val == "cheap":
            return "MQ_Cheap"
        if val == "fair":
            return "MQ_FairValue"
        if val == "expensive":
            return "MQ_Expensive"

    if quality == "low_quality":
        if val == "cheap":
            return "LQ_Cheap"
        if val == "fair":
            return "LQ_FairValue"
        if val == "expensive":
            return "LQ_Expensive"

    return "Unclassified"

def score_quality_adjusted_value(m: Dict[str, Any], peer_medians: Dict[str, Optional[float]]) -> float:
    """
    Produces a ranking score. Higher = more attractive.

    Philosophy:
      - Reward ROIC, FCF margin, growth
      - Penalize dilution and SBC
      - Reward cheapness only when valuation metrics are economically applicable
      - Reward discount vs peers (if peer data exists)
    """
    s = 0.0

    roic = m.get("roic")
    fcfm = m.get("fcf_margin")
    growth = m.get("rev_cagr_5y")
    dil = m.get("dilution_5y")
    sbc = m.get("sbc_to_revenue")
    ev_fcf = m.get("ev_to_fcf")
    fcf_y = m.get("fcf_yield")
    ev_fcf_applicable = bool(m.get("ev_to_fcf_applicable", ev_fcf is not None and ev_fcf > 0))
    fcf_y_applicable = bool(m.get("fcf_yield_applicable", fcf_y is not None and fcf_y > 0))

    # Quality
    if roic is not None:
        adj_roic = min(roic, 0.50)
        s += clamp((adj_roic - 0.10) * 40, -5, 8)
    if fcfm is not None:
        s += clamp((fcfm - 0.05) * 40, -4, 8)
    if growth is not None:
        s += clamp((growth - 0.04) * 30, -3, 6)

    # Capital discipline
    if dil is not None:
        s += clamp(-(dil) * 10, -8, 2)
    if sbc is not None:
        s += clamp(-(sbc) * 50, -6, 1)

    # Valuation absolute: only when the underlying denominator is economically meaningful.
    if ev_fcf_applicable and ev_fcf is not None:
        s += clamp((25 - ev_fcf) / 5, -6, 6)
    elif fcf_y_applicable and fcf_y is not None:
        s += clamp((fcf_y - 0.04) * 80, -6, 6)

    # Peer relative discount/premium (only if economically meaningful)
    peer_ev_fcf = peer_medians.get("peer_ev_to_fcf")
    if (
        ev_fcf_applicable
        and ev_fcf is not None
        and peer_ev_fcf is not None
        and peer_ev_fcf > 5
    ):
        rel = (peer_ev_fcf - ev_fcf) / peer_ev_fcf
        s += clamp(rel * 6, -3, 3)

    peer_roic = peer_medians.get("peer_roic")
    if (
        roic is not None
        and peer_roic is not None
        and peer_roic > 0.08
    ):
        relq = (roic - peer_roic) / peer_roic
        s += clamp(relq * 3, -2, 2)

    return float(s)


# =========================
# =========================
# IO
# =========================

def read_level1(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError("Expected level1_results.json to be a list of rows")
    return data

def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        return
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in keys})

def investment_view(quality, valuation):

    if quality == "high_quality":

        if valuation == "cheap":
            return "Strong Buy"

        if valuation == "fair":
            return "Accumulate"

        if valuation == "expensive":
            return "Watch for Pullback"

    if quality == "mid_quality":

        if valuation == "cheap":
            return "Speculative Buy"

        if valuation == "fair":
            return "Neutral"

        if valuation == "expensive":
            return "Avoid"

    if quality == "low_quality":

        if valuation == "cheap":
            return "Value Trap Risk"

        return "Avoid"

    return "Unclear"

def analyze_single_stock_stage5_2(
    symbol: str,
    level1_row: Dict[str, Any],
    target_fundamentals: Optional[Dict[str, Any]] = None,
    request_counter: Optional[FmpRequestCounter] = None,
) -> Dict[str, Any]:

    api = FmpClient(BASE_URL, FMP_API_KEY, request_counter)

    target = latest_target_fundamentals(
        target_fundamentals or level1_row.get("target_fundamentals")
    )

    # -------------------------------
    # PARALLEL CORE DATA FETCH
    # -------------------------------

    if target is not None:
        with ThreadPoolExecutor(max_workers=2) as executor:
            peers_future = executor.submit(fetch_stock_peers, api, symbol)
            quote_future = executor.submit(fetch_live_quote, api, symbol)
            peers = peers_future.result()
            quote = quote_future.result()

        km = target["key_metrics"]
        rat = target["ratios"]
        inc = target["income"]
        cf = target["cashflow"]
        bs = target["balance"]
    else:
        with ThreadPoolExecutor(max_workers=7) as executor:
            futures = {
                "km": executor.submit(fetch_latest_key_metrics, api, symbol),
                "rat": executor.submit(fetch_latest_ratios, api, symbol),
                "peers": executor.submit(fetch_stock_peers, api, symbol),
                "quote": executor.submit(fetch_live_quote, api, symbol),
                "inc": executor.submit(fetch_latest_income_statement, api, symbol),
                "cf": executor.submit(fetch_latest_cashflow_statement, api, symbol),
                "bs": executor.submit(fetch_latest_balance_sheet, api, symbol)
            }

            km = futures["km"].result()
            rat = futures["rat"].result()
            peers = futures["peers"].result()
            quote = futures["quote"].result()
            inc = futures["inc"].result()
            cf = futures["cf"].result()
            bs = futures["bs"].result()


    # -------------------------------
    # PEER ANALYSIS
    # -------------------------------

    peer_symbols = peer_symbols_from_list(peers, symbol)

    peer_ev_fcf_vals = []
    peer_roic_vals = []

    with ThreadPoolExecutor(max_workers=6) as executor:

        peer_metrics = list(
            executor.map(lambda ps: fetch_latest_key_metrics(api, ps), peer_symbols)
        )

    for pkm in peer_metrics:

        peer_ev_fcf_vals.append(to_float(pkm.get("evToFreeCashFlow")))
        peer_roic_vals.append(to_float(pkm.get("returnOnInvestedCapital")))

    peer_medians = {
        "peer_ev_to_fcf": median(peer_ev_fcf_vals),
        "peer_roic": median(peer_roic_vals),
    }

    # Build structural metrics
    m = build_value_quality_inputs(level1_row, km, rat)

    live_price = to_float(quote.get("price"))
    live_market_cap = to_float(quote.get("marketCap"))

    total_debt = to_float(bs.get("totalDebt"))
    cash = to_float(bs.get("cashAndCashEquivalents"))

    fcf = to_float(cf.get("freeCashFlow"))
    ebitda = to_float(inc.get("ebitda"))
    revenue = to_float(inc.get("revenue"))
    eps = to_float(inc.get("eps"))

    enterprise_value = None

    if live_market_cap and total_debt and cash is not None:
        enterprise_value = live_market_cap + total_debt - cash

    m["ev_to_fcf_applicable"] = bool(enterprise_value is not None and fcf is not None and fcf > 0 and enterprise_value > 0)
    m["ev_to_ebitda_applicable"] = bool(enterprise_value is not None and ebitda is not None and ebitda > 0 and enterprise_value > 0)
    m["fcf_yield_applicable"] = bool(enterprise_value is not None and fcf is not None and fcf > 0 and enterprise_value > 0)

    if enterprise_value is not None and fcf is not None and fcf != 0:
        m["ev_to_fcf"] = enterprise_value / fcf
        m["fcf_yield"] = fcf / enterprise_value
    else:
        m["ev_to_fcf"] = None if fcf is None or fcf == 0 else enterprise_value / fcf
        m["fcf_yield"] = None if enterprise_value in (None, 0) else fcf / enterprise_value if fcf is not None else None

    if enterprise_value is not None and ebitda is not None and ebitda != 0:
        m["ev_to_ebitda"] = enterprise_value / ebitda
    else:
        m["ev_to_ebitda"] = None if ebitda in (None, 0) else enterprise_value / ebitda

    if enterprise_value and revenue:
        m["ev_to_sales"] = enterprise_value / revenue

    if live_price and eps:
        m["earnings_yield"] = eps / live_price

    q_bucket = quality_bucket(m)
    v_bucket = valuation_bucket(m)
    quad = quadrant(q_bucket, v_bucket)

    qav_score = score_quality_adjusted_value(m, peer_medians)

    investment_signal = investment_view(q_bucket, v_bucket)
    quarterly_bundle = fetch_quarterly_bundle(api, symbol, read_cache, write_cache)

    return {
        "ticker": symbol,
        "quality_bucket": q_bucket,
        "valuation_bucket": v_bucket,
        "quadrant": quad,
        "investment_view": investment_signal,
        "quality_adjusted_value_score": qav_score,
        "metrics": m,
        "peer_medians": peer_medians,
        "peer_symbols": peer_symbols,
        "quarterly_pulse": compute_quarterly_pulse(quarterly_bundle),
    }


def run_single_ticker(symbol: str):

    print(f"\nRunning Stage5-2 for single ticker: {symbol}\n")

    api = FmpClient(BASE_URL, FMP_API_KEY)

    try:
        # Pull core data
        km = fetch_latest_key_metrics(api, symbol)
        rat = fetch_latest_ratios(api, symbol)
        peers = fetch_stock_peers(api, symbol)

        # Build peer medians
        peer_symbols = peer_symbols_from_list(peers, symbol)

        peer_ev_fcf_vals = []
        peer_roic_vals = []

        for ps in peer_symbols:
            try:
                pkm = fetch_latest_key_metrics(api, ps)
                peer_ev_fcf_vals.append(to_float(pkm.get("evToFreeCashFlow")))
                peer_roic_vals.append(to_float(pkm.get("returnOnInvestedCapital")))
            except Exception:
                continue

        peer_medians = {
            "peer_ev_to_fcf": median([v for v in peer_ev_fcf_vals if v is not None]),
            "peer_roic": median([v for v in peer_roic_vals if v is not None]),
        }

        # Load Stage5-1 row for this ticker
        level1_rows = read_level1(INPUT_LEVEL1_JSON)
        level1_row = next((row for row in level1_rows if row.get("ticker") == symbol), {})

        m = build_value_quality_inputs(level1_row, km, rat)


        # =============================
        # DYNAMIC VALUATION (TRUE REBUILD)
        # =============================

        quote = fetch_live_quote(api, symbol)
        inc = fetch_latest_income_statement(api, symbol)
        cf = fetch_latest_cashflow_statement(api, symbol)
        bs = fetch_latest_balance_sheet(api, symbol)

        live_price = to_float(quote.get("price"))
        live_market_cap = to_float(quote.get("marketCap"))

        total_debt = to_float(bs.get("totalDebt"))
        cash = to_float(bs.get("cashAndCashEquivalents"))

        fcf = to_float(cf.get("freeCashFlow"))
        ebitda = to_float(inc.get("ebitda"))
        revenue = to_float(inc.get("revenue"))
        eps = to_float(inc.get("eps"))

        enterprise_value = None

        if live_market_cap is not None and total_debt is not None and cash is not None:
            enterprise_value = live_market_cap + total_debt - cash

        m["ev_to_fcf_applicable"] = bool(enterprise_value is not None and fcf is not None and fcf > 0 and enterprise_value > 0)
        m["ev_to_ebitda_applicable"] = bool(enterprise_value is not None and ebitda is not None and ebitda > 0 and enterprise_value > 0)
        m["fcf_yield_applicable"] = bool(enterprise_value is not None and fcf is not None and fcf > 0 and enterprise_value > 0)

        if enterprise_value is not None and fcf is not None and fcf != 0:
            m["ev_to_fcf"] = enterprise_value / fcf
            m["fcf_yield"] = fcf / enterprise_value
        else:
            m["ev_to_fcf"] = None if fcf is None or fcf == 0 else enterprise_value / fcf
            m["fcf_yield"] = None if enterprise_value in (None, 0) else fcf / enterprise_value if fcf is not None else None

        if enterprise_value is not None and ebitda is not None and ebitda != 0:
            m["ev_to_ebitda"] = enterprise_value / ebitda
        else:
            m["ev_to_ebitda"] = None if ebitda in (None, 0) else enterprise_value / ebitda

        if enterprise_value and revenue and revenue != 0:
            m["ev_to_sales"] = enterprise_value / revenue

        if live_price and eps and eps != 0:
            m["earnings_yield"] = eps / live_price


        # Now classify
        q_bucket = quality_bucket(m)
        v_bucket = valuation_bucket(m)
        quad = quadrant(q_bucket, v_bucket)
        score = score_quality_adjusted_value(m, peer_medians)


        print("===================================")
        print(f"Ticker: {symbol}")
        print(f"ROIC: {m.get('roic')}")
        print(f"FCF Margin: {m.get('fcf_margin')}")
        print(f"EV/FCF: {m.get('ev_to_fcf')}")
        print(f"FCF Yield: {m.get('fcf_yield')}")
        print(f"Net Debt/EBITDA: {m.get('net_debt_to_ebitda')}")
        print(f"Quality Bucket: {q_bucket}")
        print(f"Valuation Bucket: {v_bucket}")
        print(f"Quadrant: {quad}")
        print(f"Quality-Adjusted Score: {score}")
        print("===================================")

    except Exception as e:
        print(f"Error processing {symbol}: {e}")



# =========================
# MAIN
# =========================

def main():
    api = FmpClient(BASE_URL, FMP_API_KEY)
    rows = read_level1(INPUT_LEVEL1_JSON)

    # Filter
    clean = []
    for r in rows:
        if not INCLUDE_ERROR_ROWS and r.get("error"):
            continue
        clean.append(r)

    # Sort by Stage5-1 score if TOP_N set
    clean.sort(
        key=lambda x: (x.get("level1_score") is not None, x.get("level1_score", -1e9)),
        reverse=True
    )
    if TOP_N is not None:
        clean = clean[:TOP_N]

    results: List[Dict[str, Any]] = []

    for idx, r in enumerate(clean, 1):
        symbol = r.get("ticker") or r.get("symbol")
        if not symbol:
            continue

        print(f"[{idx}/{len(clean)}] Stage5-2 processing {symbol}...")

        try:
            target_fundamentals = latest_target_fundamentals(r.get("target_fundamentals"))
            km = target_fundamentals["key_metrics"] if target_fundamentals is not None else fetch_latest_key_metrics(api, symbol)
            rat = target_fundamentals["ratios"] if target_fundamentals is not None else fetch_latest_ratios(api, symbol)
            peers = fetch_stock_peers(api, symbol)
            quote = fetch_live_quote(api, symbol)

            live_price = to_float(quote.get("price"))
            live_market_cap = to_float(quote.get("marketCap"))
            # Build peer medians
            peer_symbols = peer_symbols_from_list(peers, symbol)

            peer_ev_fcf_vals = []
            peer_roic_vals = []

            for ps in peer_symbols:
                try:
                    pkm = fetch_latest_key_metrics(api, ps)
                    peer_ev_fcf_vals.append(to_float(pkm.get("evToFreeCashFlow")))
                    peer_roic_vals.append(to_float(pkm.get("returnOnInvestedCapital")))
                except Exception:
                    continue

            peer_medians = {
                "peer_ev_to_fcf": median([v for v in peer_ev_fcf_vals if v is not None]),
                "peer_roic": median([v for v in peer_roic_vals if v is not None]),
            }


            m = build_value_quality_inputs(r, km, rat)

            # =============================
            # DYNAMIC VALUATION (TRUE REBUILD)
            # =============================

            inc = target_fundamentals["income"] if target_fundamentals is not None else fetch_latest_income_statement(api, symbol)
            cf = target_fundamentals["cashflow"] if target_fundamentals is not None else fetch_latest_cashflow_statement(api, symbol)
            bs = target_fundamentals["balance"] if target_fundamentals is not None else fetch_latest_balance_sheet(api, symbol)

            total_debt = to_float(bs.get("totalDebt"))
            cash = to_float(bs.get("cashAndCashEquivalents"))

            fcf = to_float(cf.get("freeCashFlow"))
            ebitda = to_float(inc.get("ebitda"))
            revenue = to_float(inc.get("revenue"))
            eps = to_float(inc.get("eps"))

            enterprise_value = None

            if live_market_cap is not None and total_debt is not None and cash is not None:
                enterprise_value = live_market_cap + total_debt - cash

            m["ev_to_fcf_applicable"] = bool(enterprise_value is not None and fcf is not None and fcf > 0 and enterprise_value > 0)
            m["ev_to_ebitda_applicable"] = bool(enterprise_value is not None and ebitda is not None and ebitda > 0 and enterprise_value > 0)
            m["fcf_yield_applicable"] = bool(enterprise_value is not None and fcf is not None and fcf > 0 and enterprise_value > 0)

            if enterprise_value is not None and fcf is not None and fcf != 0:
                m["ev_to_fcf"] = enterprise_value / fcf
                m["fcf_yield"] = fcf / enterprise_value
            else:
                m["ev_to_fcf"] = None if fcf is None or fcf == 0 else enterprise_value / fcf
                m["fcf_yield"] = None if enterprise_value in (None, 0) else fcf / enterprise_value if fcf is not None else None

            if enterprise_value is not None and ebitda is not None and ebitda != 0:
                m["ev_to_ebitda"] = enterprise_value / ebitda
            else:
                m["ev_to_ebitda"] = None if ebitda in (None, 0) else enterprise_value / ebitda

            if enterprise_value and revenue and revenue != 0:
                m["ev_to_sales"] = enterprise_value / revenue

            if live_price and eps and eps != 0:
                m["earnings_yield"] = eps / live_price



            q_bucket = quality_bucket(m)
            v_bucket = valuation_bucket(m)
            quad = quadrant(q_bucket, v_bucket)
            qav_score = score_quality_adjusted_value(m, peer_medians)



            result_row = {
                "ticker": symbol,
                "company_name": r.get("company_name"),
                "bucket": r.get("bucket"),
                "level1_score": m.get("level1_score"),
                "kill_flags": r.get("kill_flags", []),

                "roic": m.get("roic"),
                "rev_cagr_5y": m.get("rev_cagr_5y"),
                "fcf_margin": m.get("fcf_margin"),
                "dilution_5y": m.get("dilution_5y"),
                "sbc_to_revenue": m.get("sbc_to_revenue"),
                "net_debt_to_ebitda": m.get("net_debt_to_ebitda"),

                "ev_to_fcf": m.get("ev_to_fcf"),
                "ev_to_ebitda": m.get("ev_to_ebitda"),
                "ev_to_sales": m.get("ev_to_sales"),
                "fcf_yield": m.get("fcf_yield"),
                "earnings_yield": m.get("earnings_yield"),

                "peer_symbols": peer_symbols,
                "peer_ev_to_fcf_median": peer_medians.get("peer_ev_to_fcf"),
                "peer_roic_median": peer_medians.get("peer_roic"),
                "peer_count_used": peer_medians.get("peer_count_used"),

                "quality_bucket": q_bucket,
                "valuation_bucket": v_bucket,
                "quadrant": quad,
                "quality_adjusted_value_score": qav_score,

            }

            results.append(result_row)

        except Exception as e:
            error_row = {
                "ticker": symbol,
                "company_name": r.get("company_name"),
                "bucket": r.get("bucket"),
                "error": str(e),
            }
            results.append(error_row)

        # ✅ WRITE JSON AFTER EVERY TICKER
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        print(f"✔ Saved progress to {OUT_JSON}")

    print("\nStage5-2 processing complete.")


if __name__ == "__main__":

    # If ticker passed via CLI, run single-company mode
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        run_single_ticker(ticker)
    else:
        main()
