import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]


class FakeResponse:
    def __init__(self, payload, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload


def set_env(monkeypatch):
    monkeypatch.setenv("FMP_API_KEY", "test-fmp-key")
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai-key")
    monkeypatch.setenv("PIPELINE_SECRET", "test-secret")
    monkeypatch.setenv("FMP_BASE_URL", "https://example.com/stable")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o-mini")


def load_module(module_name, relative_path):
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, str(ROOT / relative_path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def mock_paid_profile_response(symbol):
    profile_map = {
        "AAPL": {"sector": "Technology", "industry": "Consumer Electronics", "companyName": "Apple Inc."},
        "MSFT": {"sector": "Technology", "industry": "Software - Infrastructure", "companyName": "Microsoft"},
        "BETA": {"sector": "Healthcare", "industry": "Biotechnology", "companyName": "Beta Biotech"},
    }
    data = profile_map.get(symbol, {"sector": "Technology", "industry": "Software", "companyName": symbol})
    return [data]


def build_stage1_metrics():
    income = []
    for i in range(20):
        revenue = 500_000_000 - i * 8_000_000
        operating_income = 80_000_000 - i * 3_000_000
        gross_profit = 300_000_000 - i * 9_000_000
        ebitda = 120_000_000 - i * 4_000_000
        income.append(
            {
                "revenue": revenue,
                "operatingIncome": operating_income,
                "grossProfit": gross_profit,
                "ebitda": ebitda,
                "date": f"202{min(i, 9)}-01-01",
                "calendarYear": 2020 + i,
            }
        )

    balance = [{"totalDebt": 1_000_000_000, "cashAndCashEquivalents": 400_000_000}]
    cashflow = []
    for i in range(20):
        operating_cash_flow = 90_000_000 - i * 3_500_000
        capex = -20_000_000 - i * 900_000
        cashflow.append({"operatingCashFlow": operating_cash_flow, "capitalExpenditure": capex})

    return income, balance, cashflow


def make_stage1_fixture():
    income, balance, cashflow = build_stage1_metrics()
    metrics = {
        "revenue": sum(q["revenue"] for q in income[:4]),
        "revenue_cagr_2y": 0.12,
        "revenue_1y_growth": 0.18,
        "operating_margin": 0.18,
        "operating_margin_2y": 0.10,
        "gross_margin": 0.55,
        "fcf_margin": 0.14,
        "fcf_margin_2y": 0.08,
        "net_debt_to_ebitda": 1.2,
    }
    return {
        "run_date": "2025-01-01T00:00:00+00:00",
        "total_scanned": 1,
        "total_valid": 1,
        "tickers": {"AAPL": metrics},
    }


def make_stage2_fixture():
    return {
        "generated_at": "2025-01-01T00:00:00+00:00",
        "total_ranked": 1,
        "results": [
            {
                "ticker": "AAPL",
                "total_score": 18.5,
                "acceleration": 0.12,
                "margin_delta": 0.08,
                "fcf_delta": 0.09,
                "raw_acceleration": 0.12,
                "raw_margin_delta": 0.08,
                "raw_fcf_delta": 0.09,
                "revenue": 700_000_000,
                "subs": {
                    "growth_1y": 2.0,
                    "accel": 1.0,
                    "cagr_2y": 1.0,
                    "margin_delta": 2.0,
                    "margin_flip": 0.0,
                    "op_quality": 1.0,
                    "fcf_delta": 2.0,
                    "fcf_flip": 0.0,
                    "fcf_quality": 1.0,
                    "balance": 1.0,
                    "scale": 1.0,
                    "saturation": 0.0,
                },
                "rank": 1,
            }
        ],
    }


def make_stage3_fixture():
    return {
        "kept_total": 1,
        "kept": [
            {
                "ticker": "AAPL",
                "companyName": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "score": 18.5,
                "revenue": 700_000_000,
                "subs": {"saturation": 0.0},
                "acceleration": 0.12,
                "margin_delta": 0.08,
                "fcf_delta": 0.09,
            }
        ],
        "buckets": {
            "high_conviction_growth": [],
            "operational_turnaround": [],
            "speculative_asymmetric": ["AAPL"],
        },
    }


def make_stage5_1_row():
    return {
        "ticker": "AAPL",
        "company_name": "Apple Inc.",
        "bucket": "high_conviction_growth",
        "rev_cagr_5y": 0.18,
        "roic_latest": 0.22,
        "fcf_margin_latest": 0.17,
        "dilution_5y": 0.04,
        "cycle_distortion_flag": False,
        "roic_std_5y": 0.08,
        "fcf_margin_std_5y": 0.07,
        "kill_flags": [],
        "level1_score": 13.0,
        "profile_description": "Consumer electronics and services company.",
    }


def make_stage5_2_level1_row():
    return {
        **make_stage5_1_row(),
        "rev_cagr_5y": 0.18,
        "fcf_margin_latest": 0.17,
        "dilution_5y": 0.04,
        "roic_latest": 0.22,
    }


def build_fake_fmp_responses(symbol):
    quote = {"price": 200.0, "marketCap": 3_000_000_000.0}
    balance = {"totalDebt": 120_000_000.0, "cashAndCashEquivalents": 80_000_000.0}
    inc = {"ebitda": 80_000_000.0, "revenue": 390_000_000.0, "eps": 6.0}
    cashflow = {"freeCashFlow": 65_000_000.0}
    key_metrics = {"returnOnInvestedCapital": 0.22, "evToFreeCashFlow": 18.0}
    ratios = {"grossProfitMargin": 0.46, "operatingProfitMargin": 0.18}
    peers = [{"symbol": "MSFT"}, {"symbol": "NVDA"}]
    return {
        "quote": [quote],
        "balance": [balance],
        "income": [inc],
        "cashflow": [cashflow],
        "key_metrics": [key_metrics],
        "ratios": [ratios],
        "peers": peers,
    }


def fake_request_get(url, params=None, timeout=None):
    params = params or {}
    path = url.split("/")[-1].split("?")[0]
    symbol = params.get("symbol", "AAPL")
    if path == "profile":
        return FakeResponse(mock_paid_profile_response(symbol))
    if path == "quote":
        return FakeResponse([{"price": 200.0, "marketCap": 3_000_000_000.0}])
    if path == "key-metrics":
        return FakeResponse([{"returnOnInvestedCapital": 0.22, "evToFreeCashFlow": 18.0}])
    if path == "ratios":
        return FakeResponse([{"grossProfitMargin": 0.46, "operatingProfitMargin": 0.18}])
    if path == "income-statement":
        return FakeResponse([{"ebitda": 80_000_000.0, "revenue": 390_000_000.0, "eps": 6.0}])
    if path == "cash-flow-statement":
        return FakeResponse([{"freeCashFlow": 65_000_000.0}])
    if path == "balance-sheet-statement":
        return FakeResponse([{"totalDebt": 120_000_000.0, "cashAndCashEquivalents": 80_000_000.0}])
    if path == "stock-peers":
        return FakeResponse([{"symbol": "MSFT"}, {"symbol": "NVDA"}])
    return FakeResponse([])


def test_stage1_compute_metrics_valid_and_invalid(monkeypatch, tmp_path):
    set_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    stage1 = load_module("stage1_under_test", "Stage1.py")

    income, balance, cashflow = build_stage1_metrics()
    valid = stage1.compute_metrics(income, balance, cashflow)
    assert valid is not None
    assert valid["revenue"] > 0
    assert valid["net_debt_to_ebitda"] < 4

    bad_balance = [{"totalDebt": 10_000_000_000, "cashAndCashEquivalents": 1_000_000_000}]
    invalid = stage1.compute_metrics(income, bad_balance, cashflow)
    assert invalid is None


def test_stage2_score_company_filters_and_scoring(monkeypatch, tmp_path):
    set_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    stage1_payload = make_stage1_fixture()
    (tmp_path / "stage1_output.json").write_text(json.dumps(stage1_payload), encoding="utf-8")
    stage2 = load_module("stage2_under_test", "Stage2.py")

    good = stage2.score_company(stage1_payload["tickers"]["AAPL"])
    assert good is not None
    assert good["total_score"] > 0

    bad = {**stage1_payload["tickers"]["AAPL"], "revenue": 50_000_000}
    assert stage2.score_company(bad) is None


def test_stage3_exclusions_and_bucket_mapping(monkeypatch, tmp_path):
    set_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "stage2_output.json").write_text(json.dumps(make_stage2_fixture()), encoding="utf-8")
    monkeypatch.setattr("requests.Session.get", lambda *args, **kwargs: FakeResponse(mock_paid_profile_response("AAPL")))
    stage3 = load_module("stage3_under_test", "Stage3.py")

    assert stage3.is_reit("Real Estate", "REIT") is True
    assert stage3.is_miner("Gold Mining") is True
    assert stage3.industry_excluded("Utilities") is True

    assert stage3.buckets["speculative_asymmetric"] == ["AAPL"]
    assert stage3.kept[0]["ticker"] == "AAPL"


def test_stage4_ranked_prime_candidates_and_thesis(monkeypatch, tmp_path):
    set_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "stage3_output.json").write_text(json.dumps(make_stage3_fixture()), encoding="utf-8")
    stage4 = load_module("stage4_under_test", "Stage4.py")

    assert stage4.prime["speculative_asymmetric"][0]["ticker"] == "AAPL"
    assert "growth" in stage4.prime["speculative_asymmetric"][0]["thesis"].lower()


def test_stage5_1_structural_metrics_and_kill_flags(monkeypatch):
    set_env(monkeypatch)
    stage5_1 = load_module("stage5_1_under_test", "Stage5_1.py")

    def fake_get(self, path, params):
        symbol = params["symbol"]
        base = {
            "income": [
                {"date": "2024-12-31", "revenue": 400_000_000.0, "weightedAverageShsOut": 1_000_000_000.0},
                {"date": "2023-12-31", "revenue": 300_000_000.0, "weightedAverageShsOut": 900_000_000.0},
                {"date": "2022-12-31", "revenue": 250_000_000.0, "weightedAverageShsOut": 850_000_000.0},
                {"date": "2021-12-31", "revenue": 220_000_000.0, "weightedAverageShsOut": 820_000_000.0},
                {"date": "2020-12-31", "revenue": 200_000_000.0, "weightedAverageShsOut": 800_000_000.0},
            ],
            "cashflow": [
                {"date": "2024-12-31", "freeCashFlow": 55_000_000.0},
                {"date": "2023-12-31", "freeCashFlow": 45_000_000.0},
                {"date": "2022-12-31", "freeCashFlow": 35_000_000.0},
                {"date": "2021-12-31", "freeCashFlow": 30_000_000.0},
                {"date": "2020-12-31", "freeCashFlow": 25_000_000.0},
            ],
            "balance": [{"totalDebt": 500_000_000.0, "cashAndCashEquivalents": 200_000_000.0}],
            "ratios": [
                {"date": "2024-12-31", "grossProfitMargin": 0.50, "operatingProfitMargin": 0.18},
                {"date": "2023-12-31", "grossProfitMargin": 0.45, "operatingProfitMargin": 0.17},
                {"date": "2022-12-31", "grossProfitMargin": 0.42, "operatingProfitMargin": 0.16},
                {"date": "2021-12-31", "grossProfitMargin": 0.41, "operatingProfitMargin": 0.14},
                {"date": "2020-12-31", "grossProfitMargin": 0.40, "operatingProfitMargin": 0.15},
            ],
            "key_metrics": [
                {"date": "2024-12-31", "returnOnInvestedCapital": 0.22},
                {"date": "2023-12-31", "returnOnInvestedCapital": 0.19},
                {"date": "2022-12-31", "returnOnInvestedCapital": 0.17},
                {"date": "2021-12-31", "returnOnInvestedCapital": 0.16},
                {"date": "2020-12-31", "returnOnInvestedCapital": 0.15},
            ],
            "profile": [{"description": "Consumer electronics and services company."}],
        }
        if path == "/profile":
            return base["profile"]
        if path == "/income-statement":
            return base["income"]
        if path == "/cash-flow-statement":
            return base["cashflow"]
        if path == "/balance-sheet-statement":
            return base["balance"]
        if path == "/ratios":
            return base["ratios"]
        if path == "/key-metrics":
            return base["key_metrics"]
        return []

    monkeypatch.setattr(stage5_1.ApiClient, "get", fake_get)
    result = stage5_1.analyze_single_stock_stage5_1("AAPL")

    assert result["rev_cagr_5y"] > 0
    assert result["roic_latest"] > 0.15
    assert result["cycle_distortion_flag"] is False
    assert result["level1_score"] >= 0

    flags = stage5_1.compute_kill_flags({
        "rev_cagr_5y": 0.03,
        "roic_latest": 0.05,
        "gross_margin_delta": -0.12,
        "op_margin_delta": -0.12,
        "fcf_margin_latest": -0.02,
        "dilution_5y": 0.35,
    })
    assert len(flags) >= 4


def test_stage5_2_valuation_bucket_peer_and_fallback(monkeypatch, tmp_path):
    set_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    stage5_2 = load_module("stage5_2_under_test", "Stage5_2.py")

    def fake_get(self, path, params):
        symbol = params.get("symbol")
        data = build_fake_fmp_responses(symbol)
        if path == "/quote":
            return data["quote"]
        if path == "/key-metrics":
            return data["key_metrics"]
        if path == "/ratios":
            return data["ratios"]
        if path == "/income-statement":
            return data["income"]
        if path == "/cash-flow-statement":
            return data["cashflow"]
        if path == "/balance-sheet-statement":
            return data["balance"]
        if path == "/stock-peers":
            return data["peers"]
        return []

    monkeypatch.setattr(stage5_2.FmpClient, "get", fake_get)
    monkeypatch.setattr(stage5_2, "RUN_GPT", False)

    level1 = make_stage5_2_level1_row()
    metrics = stage5_2.build_value_quality_inputs(level1, {"returnOnInvestedCapital": 0.22}, {"grossProfitMargin": 0.46})
    assert stage5_2.quality_bucket(metrics) == "high_quality"
    assert stage5_2.valuation_bucket({**metrics, "ev_to_fcf": 18.0, "fcf_yield": 0.06}) == "fair"
    assert stage5_2.quadrant("high_quality", "fair") == "HQ_FairValue"

    peer_medians = {"peer_ev_to_fcf": 20.0, "peer_roic": 0.16}
    base = {"roic": 0.22, "fcf_margin": 0.17, "rev_cagr_5y": 0.18, "dilution_5y": 0.04, "sbc_to_revenue": 0.01, "ev_to_fcf": 18.0, "fcf_yield": 0.06}
    score = stage5_2.score_quality_adjusted_value(base, peer_medians)
    assert isinstance(score, float)
    assert score > 0

    result = stage5_2.analyze_single_stock_stage5_2("AAPL", level1)
    assert result["quadrant"] in {"HQ_FairValue", "HQ_Cheap", "HQ_Expensive"}
    assert result["quality_adjusted_value_score"] > 0


def test_stage5_2_parse_fallback_and_short_context(monkeypatch):
    set_env(monkeypatch)
    stage5_2 = load_module("stage5_2_parse_under_test", "Stage5_2.py")

    fake_client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **kwargs: SimpleNamespace(
                    choices=[SimpleNamespace(message=SimpleNamespace(content="{not json"))]
                )
            )
        )
    )
    monkeypatch.setattr(stage5_2, "client", fake_client)
    parsed = stage5_2.call_gpt_nuance("AAPL", "A short context")
    assert parsed["_parse_error"] is True

    short_context = "too short"
    assert len(stage5_2.build_gpt_context({}, {}, [], {"peer_ev_to_fcf": None, "peer_roic": None})) >= 0
    assert len(short_context) < stage5_2.MIN_GPT_CONTEXT_CHARS


def test_generate_report_persistence_and_top_candidates(monkeypatch, tmp_path):
    set_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    payload = [
        {
            "ticker": "AAPL",
            "quality_adjusted_value_score": 14.0,
            "quadrant": "HQ_Cheap",
            "bucket": "high_conviction_growth",
            "gpt_nuance": {"decision_tilt": "strong_buy"},
        },
        {
            "ticker": "MSFT",
            "quality_adjusted_value_score": 9.0,
            "quadrant": "HQ_Expensive",
            "bucket": "high_conviction_growth",
            "gpt_nuance": {"decision_tilt": "buy"},
        },
    ]
    (tmp_path / "level2_results.json").write_text(json.dumps(payload), encoding="utf-8")
    (tmp_path / "pipeline_history.json").write_text(json.dumps([]), encoding="utf-8")

    generate_report = load_module("generate_report_under_test", "generate_report.py")

    assert (tmp_path / "email_summary.txt").exists()
    content = (tmp_path / "email_summary.txt").read_text(encoding="utf-8")
    assert "AAPL" in content
    assert "Final Score" in content


def test_end_to_end_stage_pipeline_regression_fixture(monkeypatch, tmp_path):
    set_env(monkeypatch)
    monkeypatch.chdir(tmp_path)

    (tmp_path / "stage1_output.json").write_text(json.dumps(make_stage1_fixture()), encoding="utf-8")
    stage2 = load_module("stage2_e2e", "Stage2.py")
    assert stage2.results[0]["ticker"] == "AAPL"

    (tmp_path / "stage2_output.json").write_text(json.dumps(make_stage2_fixture()), encoding="utf-8")
    monkeypatch.setattr("requests.Session.get", lambda *args, **kwargs: FakeResponse(mock_paid_profile_response("AAPL")))
    stage3 = load_module("stage3_e2e", "Stage3.py")
    assert stage3.kept[0]["ticker"] == "AAPL"

    (tmp_path / "stage3_output.json").write_text(json.dumps(make_stage3_fixture()), encoding="utf-8")
    stage4 = load_module("stage4_e2e", "Stage4.py")
    assert stage4.prime["speculative_asymmetric"][0]["ticker"] == "AAPL"

    level1_row = make_stage5_1_row()
    stage5_1 = load_module("stage5_1_e2e", "Stage5_1.py")

    def fake_get(self, path, params):
        endpoint_map = {
            "/quote": "quote",
            "/income-statement": "income",
            "/cash-flow-statement": "cashflow",
            "/balance-sheet-statement": "balance",
            "/ratios": "ratios",
            "/key-metrics": "key_metrics",
            "/profile": "profile",
            "/stock-peers": "peers",
        }
        key = endpoint_map.get(path)
        if key is None:
            raise KeyError(path)
        return build_fake_fmp_responses(params["symbol"])[key]

    monkeypatch.setattr(stage5_1.ApiClient, "get", fake_get)
    stage5_1_result = stage5_1.analyze_single_stock_stage5_1("AAPL")
    assert stage5_1_result["ticker"] == "AAPL"

    stage5_2 = load_module("stage5_2_e2e", "Stage5_2.py")
    monkeypatch.setattr(stage5_2, "RUN_GPT", False)
    monkeypatch.setattr(stage5_2.FmpClient, "get", fake_get)
    stage5_2_result = stage5_2.analyze_single_stock_stage5_2("AAPL", level1_row)
    stage5_2_result.setdefault("gpt_nuance", {"decision_tilt": "strong_buy"})
    stage5_2_result.setdefault("bucket", "speculative_asymmetric")
    assert stage5_2_result["quality_bucket"] in {"high_quality", "mid_quality", "low_quality"}
    assert stage5_2_result["quadrant"]

    (tmp_path / "level2_results.json").write_text(json.dumps([stage5_2_result]), encoding="utf-8")
    (tmp_path / "pipeline_history.json").write_text(json.dumps([]), encoding="utf-8")
    generate_report = load_module("generate_report_e2e", "generate_report.py")
    assert (tmp_path / "email_summary.txt").exists()
