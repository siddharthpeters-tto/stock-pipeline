import importlib.util
import json
from pathlib import Path

import pytest

import fmp_helpers

ROOT = Path(__file__).resolve().parents[1]


def load_module(module_name, relative_path):
    spec = importlib.util.spec_from_file_location(module_name, str(ROOT / relative_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = "error" if status_code != 200 else ""

    def json(self):
        return self._payload


def test_fmp_helpers_normalize_and_validate_numeric_values():
    rows = fmp_helpers.normalize_fmp_rows([
        {"date": "2024-12-31", "revenue": 100.0},
        {"date": None, "revenue": None},
        {},
        {"date": "2023-12-31", "revenue": 0},
    ])
    assert len(rows) == 2
    assert rows[0]["revenue"] == 100.0
    assert fmp_helpers.safe_ratio(10, 0) is None
    assert fmp_helpers.safe_ratio(10, 5) == 2.0
    assert fmp_helpers.safe_float("nan") is None


def test_fmp_request_counter_groups_real_requests(monkeypatch):
    stage5_2 = load_module("stage5_2_request_counter", "Stage5_2.py")
    counter = fmp_helpers.FmpRequestCounter()
    calls = []

    class SuccessfulResponse:
        status_code = 200
        text = ""

        def json(self):
            return []

    def fake_get(url, params=None, timeout=None):
        calls.append(url)
        return SuccessfulResponse()

    monkeypatch.setattr(stage5_2.requests, "get", fake_get)
    client = stage5_2.FmpClient("https://example.com/stable", "key", counter)

    client.get("/income-statement", {"symbol": "AAPL"})
    client.get("/income-statement", {"symbol": "MSFT"})
    client.get("/quote", {"symbol": "AAPL"})

    assert len(calls) == 3
    assert counter.snapshot() == {
        "total": 3,
        "by_endpoint": {"income-statement": 2, "quote": 1},
    }


def test_stage5_2_batch_analysis_fetches_one_quote_per_ticker(monkeypatch, tmp_path):
    stage5_2 = load_module("stage5_2_quote_dedup", "Stage5_2.py")
    stage5_2.TOP_N = None
    stage5_2.CACHE_DIR = str(tmp_path / "api_cache")
    stage5_2.INPUT_LEVEL1_JSON = str(tmp_path / "level1.json")
    stage5_2.OUT_JSON = str(tmp_path / "level2.json")
    stage5_2.OUT_CSV = str(tmp_path / "level2.csv")
    stage5_2.time.sleep = lambda *_args, **_kwargs: None
    (tmp_path / "level1.json").write_text(
        json.dumps([{
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "level1_score": 13.0,
            "rev_cagr_5y": 0.18,
            "fcf_margin_latest": 0.17,
            "dilution_5y": 0.04,
        }]),
        encoding="utf-8",
    )

    calls = []

    def fake_get(url, params=None, timeout=None):
        endpoint = url.rstrip("/").split("/")[-1]
        calls.append(endpoint)
        responses = {
            "quote": [{"price": 200.0, "marketCap": 3_000_000_000.0}],
            "key-metrics": [{"returnOnInvestedCapital": 0.22, "evToFreeCashFlow": 18.0}],
            "ratios": [{"grossProfitMargin": 0.46, "operatingProfitMargin": 0.18}],
            "income-statement": [{"ebitda": 80_000_000.0, "revenue": 390_000_000.0, "eps": 6.0}],
            "cash-flow-statement": [{"freeCashFlow": 65_000_000.0}],
            "balance-sheet-statement": [{"totalDebt": 120_000_000.0, "cashAndCashEquivalents": 80_000_000.0}],
            "stock-peers": [{"symbol": "MSFT"}, {"symbol": "NVDA"}],
        }
        return DummyResponse(responses[endpoint])

    monkeypatch.setattr(stage5_2.requests, "get", fake_get)
    stage5_2.main()

    assert calls.count("quote") == 1


def test_stage5_2_reuses_supplied_target_fundamentals(monkeypatch, tmp_path):
    stage5_2 = load_module("stage5_2_shared_fundamentals", "Stage5_2.py")
    stage5_2.CACHE_DIR = str(tmp_path / "api_cache")
    stage5_2.time.sleep = lambda *_args, **_kwargs: None

    target = {
        "income": {"ebitda": 80_000_000.0, "revenue": 390_000_000.0, "eps": 6.0},
        "cashflow": {"freeCashFlow": 65_000_000.0},
        "balance": {"totalDebt": 120_000_000.0, "cashAndCashEquivalents": 80_000_000.0},
        "ratios": {"grossProfitMargin": 0.46, "operatingProfitMargin": 0.18},
        "key_metrics": {"returnOnInvestedCapital": 0.22, "evToFreeCashFlow": 18.0},
    }

    def target_fetch_must_not_run(api, symbol):
        if symbol == "AAPL":
            raise AssertionError("target annual data should come from Stage5_1")
        return {"returnOnInvestedCapital": 0.20, "evToFreeCashFlow": 20.0}

    monkeypatch.setattr(stage5_2, "fetch_latest_key_metrics", target_fetch_must_not_run)
    monkeypatch.setattr(stage5_2, "fetch_latest_ratios", target_fetch_must_not_run)
    monkeypatch.setattr(stage5_2, "fetch_latest_income_statement", target_fetch_must_not_run)
    monkeypatch.setattr(stage5_2, "fetch_latest_cashflow_statement", target_fetch_must_not_run)
    monkeypatch.setattr(stage5_2, "fetch_latest_balance_sheet", target_fetch_must_not_run)
    monkeypatch.setattr(stage5_2, "fetch_stock_peers", lambda api, symbol: [{"symbol": "MSFT"}])
    monkeypatch.setattr(stage5_2, "fetch_live_quote", lambda api, symbol: {"price": 200.0, "marketCap": 3_000_000_000.0})
    monkeypatch.setattr(stage5_2, "fetch_quarterly_bundle", lambda *_args: {"income": [], "cashflow": []})

    result = stage5_2.analyze_single_stock_stage5_2(
        "AAPL",
        {
            "ticker": "AAPL",
            "rev_cagr_5y": 0.18,
            "fcf_margin_latest": 0.17,
            "dilution_5y": 0.04,
            "level1_score": 13.0,
        },
        target_fundamentals=target,
    )

    assert result["metrics"]["ev_to_fcf"] == pytest.approx(46.7692307692)
    assert result["metrics"]["ev_to_ebitda"] == pytest.approx(38.0)


def test_stage5_2_without_shared_fundamentals_keeps_fetch_fallback(monkeypatch):
    stage5_2 = load_module("stage5_2_fallback", "Stage5_2.py")
    fetched = []

    def record_fetch(name, value):
        def fetch(_api, symbol):
            assert symbol == "AAPL"
            fetched.append(name)
            return value
        return fetch

    monkeypatch.setattr(stage5_2, "fetch_latest_key_metrics", record_fetch("key_metrics", {"returnOnInvestedCapital": 0.22}))
    monkeypatch.setattr(stage5_2, "fetch_latest_ratios", record_fetch("ratios", {}))
    monkeypatch.setattr(stage5_2, "fetch_latest_income_statement", record_fetch("income", {"ebitda": 80_000_000.0, "revenue": 390_000_000.0, "eps": 6.0}))
    monkeypatch.setattr(stage5_2, "fetch_latest_cashflow_statement", record_fetch("cashflow", {"freeCashFlow": 65_000_000.0}))
    monkeypatch.setattr(stage5_2, "fetch_latest_balance_sheet", record_fetch("balance", {"totalDebt": 120_000_000.0, "cashAndCashEquivalents": 80_000_000.0}))
    monkeypatch.setattr(stage5_2, "fetch_stock_peers", lambda _api, _symbol: [])
    monkeypatch.setattr(stage5_2, "fetch_live_quote", lambda _api, _symbol: {"price": 200.0, "marketCap": 3_000_000_000.0})
    monkeypatch.setattr(stage5_2, "fetch_quarterly_bundle", lambda *_args: {"income": [], "cashflow": []})

    stage5_2.analyze_single_stock_stage5_2("AAPL", {"ticker": "AAPL"})

    assert set(fetched) == {"key_metrics", "ratios", "income", "cashflow", "balance"}


def test_stage1_compute_metrics_rejects_invalid_quarter_data():
    stage1 = load_module("stage1_hardening", "Stage1.py")
    income = []
    for i in range(12):
        income.append({
            "revenue": 500_000_000 - i * 8_000_000,
            "operatingIncome": 80_000_000 - i * 3_000_000,
            "grossProfit": 300_000_000 - i * 9_000_000,
            "ebitda": 120_000_000 - i * 4_000_000,
            "date": f"202{i % 10}-01-01",
        })
    balance = [{"totalDebt": 1_000_000_000, "cashAndCashEquivalents": 400_000_000}]
    cashflow = []
    for i in range(12):
        cashflow.append({
            "operatingCashFlow": 90_000_000 - i * 3_500_000,
            "capitalExpenditure": -20_000_000 - i * 900_000,
        })
    assert stage1.compute_metrics(income, balance, cashflow) is not None

    bad_income = [dict(row) for row in income]
    bad_income[0]["revenue"] = None
    assert stage1.compute_metrics(bad_income, balance, cashflow) is not None


def test_stage3_fetch_profile_rejects_bad_profile_payloads(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "stage2_output.json").write_text(json.dumps({"results": [{"ticker": "AAPL"}]}) , encoding="utf-8")
    stage3 = load_module("stage3_hardening", "Stage3.py")

    class FakeSession:
        def get(self, url, timeout=10):
            return DummyResponse([{}, {"sector": "Technology"}])

    monkeypatch.setattr(stage3, "session", FakeSession())
    assert stage3.fetch_profile("AAPL") == {"sector": "Technology"}


def test_stage5_2_peer_sets_are_validated(monkeypatch):
    stage5_2 = load_module("stage5_2_hardening", "Stage5_2.py")

    class FakeApi:
        def get(self, path, params):
            if path == "/stock-peers":
                return [
                    {"symbol": "MSFT"},
                    {"symbol": None},
                    {"symbol": "   "},
                    {"symbol": "NVDA"},
                ]
            return []

    peers = stage5_2.fetch_stock_peers(FakeApi(), "AAPL")
    assert [p["symbol"] for p in peers] == ["MSFT", "NVDA"]


def test_build_universe_rejects_invalid_response_shape(monkeypatch):
    build_universe = load_module("build_universe_hardening", "buildUniverse.py")

    class FakeResponse:
        status_code = 200
        text = "ok"

        def json(self):
            return {"error": "bad payload"}

    monkeypatch.setattr(build_universe.requests, "get", lambda *args, **kwargs: FakeResponse())
    with pytest.raises(ValueError):
        build_universe.fetch("NASDAQ")
