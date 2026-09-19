import importlib.util
from pathlib import Path

import pytest

import fmp_helpers


ROOT = Path(__file__).resolve().parents[1]


def load_trajectory(module_name="trajectory_under_test"):
    spec = importlib.util.spec_from_file_location(
        module_name,
        str(ROOT / "stock_research" / "trajectory.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_stage5_1(module_name="stage5_1_trajectory_integration"):
    spec = importlib.util.spec_from_file_location(module_name, str(ROOT / "Stage5_1.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_bundle(values, *, newest_first=True):
    rows = []
    for year, revenue, operating_margin, fcf, roic in values:
        rows.append({
            "date": f"{year}-12-31",
            "fiscalYear": str(year),
            "period": "FY",
            "revenue": revenue,
            "grossProfit": revenue * 0.5 if revenue is not None else None,
            "operatingIncome": revenue * operating_margin if revenue is not None else None,
            "ebitda": revenue * 0.2 if revenue is not None else None,
            "weightedAverageShsOut": 100.0 + (year - 2021),
        })
    if newest_first:
        rows.reverse()

    cashflow = []
    ratios = []
    key_metrics = []
    for row, (_, _, _, fcf, roic) in zip(rows, reversed(values) if newest_first else values):
        cashflow.append({
            "date": row["date"],
            "fiscalYear": row["fiscalYear"],
            "period": "FY",
            "freeCashFlow": fcf,
            "operatingCashFlow": fcf + 10 if fcf is not None else None,
            "capitalExpenditure": -10,
        })
        ratios.append({
            "date": row["date"],
            "fiscalYear": row["fiscalYear"],
            "period": "FY",
            "grossProfitMargin": 0.5 if row["revenue"] else None,
            "operatingProfitMargin": row["operatingIncome"] / row["revenue"] if row["revenue"] else None,
        })
        key_metrics.append({
            "date": row["date"],
            "fiscalYear": row["fiscalYear"],
            "period": "FY",
            "returnOnInvestedCapital": roic,
            "stockBasedCompensationToRevenue": 0.02,
        })

    return {
        "income": rows,
        "cashflow": cashflow,
        "balance": [],
        "ratios": ratios,
        "key_metrics": key_metrics,
    }


def test_annual_history_aligns_sources_and_orders_oldest_to_newest():
    trajectory = load_trajectory()
    bundle = make_bundle([
        (2021, 100.0, 0.10, 10.0, 0.08),
        (2022, 120.0, 0.12, 12.0, 0.10),
        (2023, 156.0, 0.15, 15.0, 0.12),
    ])
    bundle["ratios"][0]["date"] = "2023-12-30"

    history = trajectory.align_annual_history(bundle)

    assert [row["fiscal_year"] for row in history] == ["2021", "2022", "2023"]
    assert history[-1]["operating_margin"] == pytest.approx(0.15)
    assert history[-1]["roic"] == pytest.approx(0.12)


@pytest.mark.parametrize(
    ("revenues", "expected_latest", "expected_prior", "expected_delta"),
    [
        ([100.0, 120.0, 156.0], 0.30, 0.20, 0.10),
        ([100.0, 150.0, 180.0], 0.20, 0.50, -0.30),
    ],
)
def test_revenue_growth_and_acceleration_are_separate(revenues, expected_latest, expected_prior, expected_delta):
    trajectory = load_trajectory()
    values = [(2021 + index, revenue, 0.1, 10.0, 0.1) for index, revenue in enumerate(revenues)]

    metrics = trajectory.compute_trajectory_metrics(make_bundle(values))

    assert metrics["revenue_growth_latest"] == pytest.approx(expected_latest)
    assert metrics["revenue_growth_prior"] == pytest.approx(expected_prior)
    assert metrics["revenue_growth_delta"] == pytest.approx(expected_delta)


def test_crossing_zero_preserves_level_and_direction():
    trajectory = load_trajectory()
    bundle = make_bundle([
        (2024, 100.0, -0.40, -30.0, 0.05),
        (2025, 100.0, -0.20, -10.0, 0.08),
    ])

    metrics = trajectory.compute_trajectory_metrics(bundle)

    assert metrics["operating_margin_delta_1y"] == pytest.approx(0.20)
    assert metrics["operating_margin_latest"] == pytest.approx(-0.20)
    assert metrics["fcf_margin_delta_1y"] == pytest.approx(0.20)
    assert metrics["fcf_margin_latest"] == pytest.approx(-0.10)


def test_missing_invalid_and_nonconsecutive_data_is_unavailable():
    trajectory = load_trajectory()
    bundle = make_bundle([
        (2021, 100.0, 0.10, 10.0, 0.08),
        (2023, 0.0, 0.12, float("inf"), None),
    ])
    bundle["income"][0]["ebitda"] = None
    bundle["income"][1]["revenue"] = None
    bundle["key_metrics"][0]["returnOnInvestedCapital"] = None

    metrics = trajectory.compute_trajectory_metrics(bundle)

    assert metrics["periods"]["history"] == ["FY2021", "FY2023"]
    assert metrics["revenue_growth_latest"] is None
    assert metrics["revenue_growth_prior"] is None
    assert metrics["revenue_growth_delta"] is None
    assert metrics["ebitda_margin_latest"] is None
    assert metrics["roic_latest"] is None
    assert metrics["fcf_latest"] is None
    assert metrics["operating_margin_delta_1y"] is None


def test_one_valid_period_has_no_fabricated_trend():
    trajectory = load_trajectory()
    metrics = trajectory.compute_trajectory_metrics(
        make_bundle([(2025, 100.0, 0.10, 10.0, 0.08)])
    )

    assert metrics["periods"]["latest"] == "FY2025"
    assert metrics["periods"]["prior"] is None
    assert metrics["operating_margin_latest"] == pytest.approx(0.10)
    assert metrics["operating_margin_delta_1y"] is None


def test_trajectory_is_pure_and_adds_zero_fmp_requests():
    trajectory = load_trajectory()
    counter = fmp_helpers.FmpRequestCounter()
    before = counter.snapshot()

    trajectory.compute_trajectory_metrics(
        make_bundle([
            (2024, 100.0, 0.10, 10.0, 0.08),
            (2025, 120.0, 0.12, 12.0, 0.10),
        ])
    )

    assert counter.snapshot() == before == {"total": 0, "by_endpoint": {}}


def test_trajectory_output_has_fy_provenance():
    trajectory = load_trajectory()
    metrics = trajectory.compute_trajectory_metrics(
        make_bundle([
            (2024, 100.0, 0.10, 10.0, 0.08),
            (2025, 120.0, 0.12, 12.0, 0.10),
        ])
    )

    assert metrics["basis"] == "FY"
    assert metrics["periods"] == {
        "history": ["FY2024", "FY2025"],
        "latest": "FY2025",
        "prior": "FY2024",
    }


def test_stage5_1_computes_trajectory_from_the_single_annual_bundle(monkeypatch):
    stage5_1 = load_stage5_1()
    bundle = make_bundle([
        (2024, 100.0, 0.10, 10.0, 0.08),
        (2025, 120.0, 0.12, 12.0, 0.10),
    ])
    calls = []

    def fake_fetch(_api, symbol):
        calls.append(symbol)
        return bundle

    monkeypatch.setattr(stage5_1, "API_KEY", "test-key")
    monkeypatch.setattr(stage5_1, "fetch_quant_bundle", fake_fetch)

    result = stage5_1.analyze_single_stock_stage5_1("AAPL")

    assert calls == ["AAPL"]
    assert result["trajectory_metrics"]["basis"] == "FY"
    assert result["trajectory_metrics"]["operating_margin_delta_1y"] == pytest.approx(0.02)