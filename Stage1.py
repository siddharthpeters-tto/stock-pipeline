import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from tqdm import tqdm
import sys


# ==========================================
# CONFIG
# ==========================================

BASE_URL = "https://financialmodelingprep.com/stable"

CALLS_PER_MINUTE = 300
CALLS_PER_TICKER = 3
TICKERS_PER_BATCH = 90   # 90 * 3 = 270 < 300
SLEEP_SECONDS = 60

MIN_REQUIRED_QUARTERS = 12


# ==========================================
# ENV
# ==========================================

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

if not API_KEY:
    raise ValueError("FMP_API_KEY not found")

session = requests.Session()


# ==========================================
# HELPERS
# ==========================================

def fetch(endpoint):
    try:
        url = f"{BASE_URL}/{endpoint}&apikey={API_KEY}"
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            return None
        return r.json()
    except:
        return None


def sum_quarters(data, field, start, count=4):
    total = 0
    for i in range(start, start + count):
        if i >= len(data):
            return None
        total += data[i].get(field, 0) or 0
    return total


# ==========================================
# METRIC COMPUTATION (TTM)
# ==========================================

def compute_metrics(income, balance, cashflow):

    if len(income) < MIN_REQUIRED_QUARTERS:
        return None

    try:
        # -----------------------
        # Revenue (TTM)
        # -----------------------
        rev_now = sum_quarters(income, "revenue", 0)
        rev_1y = sum_quarters(income, "revenue", 4)
        rev_2y = sum_quarters(income, "revenue", 8)

        if not rev_now or not rev_2y or rev_now <= 0 or rev_2y <= 0:
            return None

        revenue_1y_growth = (rev_now / rev_1y) - 1 if rev_1y else 0
        revenue_cagr_2y = (rev_now / rev_2y) ** (1/2) - 1

        # -----------------------
        # Operating Margin (TTM)
        # -----------------------
        op_now = sum_quarters(income, "operatingIncome", 0)
        op_2y = sum_quarters(income, "operatingIncome", 8)

        operating_margin = op_now / rev_now
        operating_margin_2y = op_2y / rev_2y

        if operating_margin < 0:
            return None

        # -----------------------
        # Gross Margin (TTM)
        # -----------------------
        gross_now = sum_quarters(income, "grossProfit", 0)
        gross_margin = gross_now / rev_now

        # -----------------------
        # Free Cash Flow (TTM)
        # -----------------------
        cf_now = sum_quarters(cashflow, "operatingCashFlow", 0)
        capex_now = sum_quarters(cashflow, "capitalExpenditure", 0)
        fcf_now = cf_now + capex_now

        cf_2y = sum_quarters(cashflow, "operatingCashFlow", 8)
        capex_2y = sum_quarters(cashflow, "capitalExpenditure", 8)
        fcf_2y = cf_2y + capex_2y

        fcf_margin = fcf_now / rev_now
        fcf_margin_2y = fcf_2y / rev_2y

        # -----------------------
        # Balance Sheet
        # -----------------------
        latest_bs = balance[0]
        total_debt = latest_bs.get("totalDebt", 0)
        cash = latest_bs.get("cashAndCashEquivalents", 0)

        ebitda_now = sum_quarters(income, "ebitda", 0)

        net_debt_to_ebitda = None
        if ebitda_now and ebitda_now > 0:
            net_debt_to_ebitda = (total_debt - cash) / ebitda_now

        if net_debt_to_ebitda and net_debt_to_ebitda > 4:
            return None

        # Structural revenue collapse filter
        if revenue_cagr_2y < -0.10:
            return None

        return {
            "revenue": rev_now,
            "revenue_cagr_2y": revenue_cagr_2y,
            "revenue_1y_growth": revenue_1y_growth,
            "operating_margin": operating_margin,
            "operating_margin_2y": operating_margin_2y,
            "gross_margin": gross_margin,
            "fcf_margin": fcf_margin,
            "fcf_margin_2y": fcf_margin_2y,
            "net_debt_to_ebitda": net_debt_to_ebitda
        }

    except:
        return None


# ==========================================
# RUN SCAN
# ==========================================

def run_scan(tickers):

    try:
        with open("stage1_output.json", "r") as f:
            existing = json.load(f)
            results = existing.get("tickers", {})
    except:
        results = {}

    processed = set(results.keys())
    print(f"Resuming. Already processed: {len(processed)}")

    total_batches = (len(tickers) // TICKERS_PER_BATCH) + 1

    for batch_index in range(total_batches):

        start = batch_index * TICKERS_PER_BATCH
        end = start + TICKERS_PER_BATCH
        batch = tickers[start:end]

        if not batch:
            continue

        print(f"\nBatch {batch_index+1}/{total_batches}")

        for ticker in tqdm(batch):

            if ticker in processed:
                continue

            income = fetch(f"income-statement?symbol={ticker}&period=quarter&limit=20")
            balance = fetch(f"balance-sheet-statement?symbol={ticker}&period=quarter&limit=20")
            cashflow = fetch(f"cash-flow-statement?symbol={ticker}&period=quarter&limit=20")

            if not income or not balance or not cashflow:
                continue

            metrics = compute_metrics(income, balance, cashflow)

            if metrics:
                results[ticker] = metrics

        output = {
            "run_date": datetime.now(timezone.utc).isoformat(),
            "total_scanned": len(tickers),
            "total_valid": len(results),
            "tickers": results
        }

        with open("stage1_output.json", "w") as f:
            json.dump(output, f, indent=2)

        if batch_index < total_batches - 1:
            time.sleep(SLEEP_SECONDS)

    print("\nStage 1 Complete")
    print(f"Valid companies: {len(results)}")


# ==========================================
# ENTRY
# ==========================================

if __name__ == "__main__":

    with open("tickers.txt") as f:
        TICKERS = [line.strip().upper() for line in f if line.strip()]

    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
        TICKERS = TICKERS[:limit]
        print(f"Running test on first {len(TICKERS)} tickers")

    run_scan(TICKERS)