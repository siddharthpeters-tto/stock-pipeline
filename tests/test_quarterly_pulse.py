import importlib.util
from pathlib import Path

import pytest

import fmp_helpers


ROOT = Path(__file__).resolve().parents[1]


def load_stage5_2(module_name):
    spec = importlib.util.spec_from_file_location(module_name, str(ROOT / "Stage5_2.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_quarterly():
    from stock_research import quarterly
    return quarterly


def test_quarterly_pulse_aligns_by_fiscal_period_and_uses_existing_fcf_definition():
    quarterly = load_quarterly()
    income = []
    cashflow = []
    revenues = {
        ("2024", "Q4"): 100.0,
        ("2025", "Q1"): 110.0,
        ("2025", "Q2"): 120.0,
        ("2025", "Q3"): 130.0,
        ("2025", "Q4"): 150.0,
    }
    for (year, period), revenue in reversed(list(revenues.items())):
        income.append({
            "date": f"{year}-12-31",
            "fiscalYear": year,
            "period": period,
            "revenue": revenue,
            "operatingIncome": revenue * 0.10,
            "ebitda": revenue * 0.20,
        })
        cashflow.append({
            "date": f"{year}-12-30",
            "fiscalYear": year,
            "period": period,
            "operatingCashFlow": revenue * 0.12,
            "capitalExpenditure": -revenue * 0.02,
        })

    pulse = quarterly.compute_quarterly_pulse({"income": income, "cashflow": cashflow})

    assert pulse["latest_reported"]["quarter"] == "Q4 FY25"
    assert pulse["latest_reported"]["date"] == "2025-12-31"
    assert len(pulse["quarters"]) == 4
    latest = pulse["quarters"][0]
    assert latest["revenue"] == 150.0
    assert latest["revenue_yoy_growth"] == pytest.approx(0.5)
    assert latest["operating_margin"] == pytest.approx(0.10)
    assert latest["ebitda_margin"] == pytest.approx(0.20)
    assert latest["fcf"] == pytest.approx(15.0)
    assert latest["fcf_margin"] == pytest.approx(0.10)


def test_quarterly_fetch_is_two_cold_calls_and_zero_warm_calls(monkeypatch, tmp_path):
    stage5_2 = load_stage5_2("stage5_2_quarterly_cache")
    stage5_2.CACHE_DIR = str(tmp_path)
    stage5_2.time.sleep = lambda *_args, **_kwargs: None
    counter = fmp_helpers.FmpRequestCounter()
    calls = []

    class Response:
        status_code = 200
        text = ""

        def json(self):
            return [{"date": "2025-12-31", "fiscalYear": "2025", "period": "Q4", "revenue": 100.0}]

    def fake_get(url, params=None, timeout=None):
        calls.append(url)
        return Response()

    monkeypatch.setattr(stage5_2.requests, "get", fake_get)
    api = stage5_2.FmpClient("https://example.com/stable", "key", counter)

    first = stage5_2.fetch_quarterly_bundle(api, "GLOO", stage5_2.read_cache, stage5_2.write_cache)
    assert set(first) == {"income", "cashflow"}
    assert counter.snapshot()["total"] == 2
    assert len(calls) == 2
    assert stage5_2.cache_path("GLOO", "income", {"period": "quarter", "limit": 20}).endswith("GLOO_income_quarter_20.json")

    stage5_2.fetch_quarterly_bundle(api, "GLOO", stage5_2.read_cache, stage5_2.write_cache)
    assert counter.snapshot()["total"] == 2
    assert len(calls) == 2
