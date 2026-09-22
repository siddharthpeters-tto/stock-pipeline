import pytest

from fmp_helpers import FmpRequestCounter
from stock_research.suggested_valuation import (
    HORIZON_YEARS,
    REQUIRED_RETURN,
    calculate_suggested_valuation,
    extract_valuation_inputs,
    terminal_multiple,
)


def inputs(revenues, margins, shares=100.0):
    start_year = 2021
    return {
        "annual_observations": [
            {
                "fiscal_year": str(start_year + index),
                "date": f"{start_year + index}-12-31",
                "revenue": revenue,
                "fcf_margin": margin,
            }
            for index, (revenue, margin) in enumerate(zip(revenues, margins))
        ],
        "current_share_count": shares,
    }


def test_stable_profitable_company_and_exact_compounding():
    data = inputs([1000, 1100, 1210, 1331, 1464.1], [0.20] * 5)
    result = calculate_suggested_valuation(data, current_price=100, roic=0.15, net_debt_to_ebitda=1.0)

    assert result["applicable"] is True
    assert result["base"]["revenue_cagr"] == pytest.approx(0.10)
    assert result["base"]["future_revenue"] == pytest.approx(1464.1 * 1.10 ** 10)
    assert result["base"]["future_fcf"] == pytest.approx(result["base"]["future_revenue"] * 0.20)
    assert result["base"]["future_share_price"] == pytest.approx(result["base"]["future_fcf"] * 20 / 100)
    assert result["base"]["buy_below_price"] == pytest.approx(result["base"]["future_share_price"] / 1.10 ** 10)
    assert result["base"]["implied_return_at_current_price"] == pytest.approx((result["base"]["future_share_price"] / 100) ** 0.1 - 1)
    assert HORIZON_YEARS == 10
    assert REQUIRED_RETURN == pytest.approx(0.10)


def test_extreme_recent_growth_is_normalized_and_base_capped():
    result = calculate_suggested_valuation(inputs([100, 105, 110, 115, 400], [0.20] * 5), 50)
    assert result["base"]["revenue_cagr"] == pytest.approx(0.20)
    assert result["methodology"]["growth_debug"]["raw_pre_clamp"]["base"] > 0.20
    assert result["optimistic"]["revenue_cagr"] <= 0.25


def test_volatile_growth_sets_half_standard_deviation_spread():
    result = calculate_suggested_valuation(inputs([100, 150, 120, 180, 162], [0.20] * 5), 50)
    debug = result["methodology"]["growth_debug"]
    assert debug["scenario_spread"] == pytest.approx(0.5 * debug["historical_growth_std_dev"])
    assert debug["scenario_spread"] > 0.02


def test_stable_and_volatile_fcf_margin_percentiles_and_ordering():
    stable = calculate_suggested_valuation(inputs([100, 110, 120, 130, 140], [0.20] * 5), 50)
    assert stable["conservative"]["fcf_margin"] == pytest.approx(0.20)
    assert stable["base"]["fcf_margin"] == pytest.approx(0.20)
    assert stable["optimistic"]["fcf_margin"] == pytest.approx(0.20)

    volatile = calculate_suggested_valuation(inputs([100, 110, 120, 130, 140], [0.02, 0.30, 0.10, 0.40, 0.20]), 50)
    assert volatile["conservative"]["fcf_margin"] == pytest.approx(0.10)
    assert volatile["base"]["fcf_margin"] == pytest.approx(0.20)
    assert volatile["optimistic"]["fcf_margin"] == pytest.approx(0.30)
    assert volatile["conservative"]["fcf_margin"] <= volatile["base"]["fcf_margin"] <= volatile["optimistic"]["fcf_margin"]


@pytest.mark.parametrize(
    "data, reason",
    [
        (inputs([100, 110, 120], [-0.10, -0.05, -0.02]), "positive normalized FCF basis not established"),
        (inputs([100, 110], [0.10, 0.12]), "insufficient annual revenue history"),
        (inputs([100, 110, 120], [None, None, 0.12]), "insufficient annual FCF-margin history"),
        (inputs([100, 110, 0], [0.10, 0.11, 0.12]), "positive latest revenue not available"),
        (inputs([100, 110, 120], [0.10, 0.11, 0.12], shares=0), "positive current share count not available"),
    ],
)
def test_unavailable_cases(data, reason):
    assert calculate_suggested_valuation(data, 50) == {"applicable": False, "reason": reason}


def test_terminal_multiple_floor_ceiling_and_quality_adjustments():
    floor = terminal_multiple(0.0, roic=0.05, fcf_margin=0.10, net_debt_to_ebitda=1.0)
    assert floor == {"growth_derived_multiple": 12.0, "quality_adjustment": -2.0, "terminal_p_fcf": 12.0}

    ceiling = terminal_multiple(0.25, roic=0.25, fcf_margin=0.20, net_debt_to_ebitda=1.0)
    assert ceiling["growth_derived_multiple"] == pytest.approx(32.0)
    assert ceiling["quality_adjustment"] == pytest.approx(2.0)
    assert ceiling["terminal_p_fcf"] == pytest.approx(30.0)

    negative = terminal_multiple(0.10, roic=0.25, fcf_margin=0.20, net_debt_to_ebitda=3.5)
    assert negative["quality_adjustment"] == pytest.approx(-2.0)


def test_scenario_assumptions_and_values_are_ordered():
    result = calculate_suggested_valuation(inputs([100, 108, 119, 130, 145], [0.10, 0.12, 0.14, 0.16, 0.18]), 50, roic=0.22)
    for field in ("revenue_cagr", "fcf_margin", "terminal_p_fcf", "buy_below_price"):
        assert result["conservative"][field] <= result["base"][field] <= result["optimistic"][field]


def test_input_extraction_uses_existing_bundle_and_adds_no_requests(monkeypatch):
    counter = FmpRequestCounter()
    monkeypatch.setattr("requests.get", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("valuation must not use the network")))
    bundle = {
        "income": [
            {"date": f"{year}-12-31", "fiscalYear": str(year), "period": "FY", "revenue": revenue, "weightedAverageShsOut": 100}
            for year, revenue in ((2025, 140), (2024, 130), (2023, 120))
        ],
        "cashflow": [
            {"date": f"{year}-12-31", "fiscalYear": str(year), "period": "FY", "freeCashFlow": fcf}
            for year, fcf in ((2025, 28), (2024, 26), (2023, 24))
        ],
    }
    before = counter.snapshot()
    extracted = extract_valuation_inputs(bundle)
    after = counter.snapshot()

    assert after == before == {"total": 0, "by_endpoint": {}}
    assert extracted["current_share_count"] == pytest.approx(100)
    assert len(extracted["annual_observations"]) == 3
    assert extracted["annual_observations"][-1]["fcf_margin"] == pytest.approx(0.20)
