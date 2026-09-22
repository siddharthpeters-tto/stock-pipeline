import math
import statistics
from typing import Any, Dict, List, Optional

from .trajectory import align_annual_history


HORIZON_YEARS = 10
REQUIRED_RETURN = 0.10


def _number(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def extract_valuation_inputs(annual_bundle: Dict[str, Any]) -> Dict[str, Any]:
    """Derive the compact valuation history while the Stage5_1 bundle is in memory."""
    history = align_annual_history(annual_bundle)[-5:]
    observations = []
    for row in history:
        observations.append({
            "fiscal_year": row.get("fiscal_year"),
            "date": row.get("date"),
            "revenue": _number(row.get("revenue")),
            "fcf_margin": _number(row.get("fcf_margin")),
        })

    latest = history[-1] if history else {}
    share_count = _number(latest.get("shares"))
    income_rows = annual_bundle.get("income", [])
    if isinstance(income_rows, dict):
        income_rows = [income_rows]
    if isinstance(income_rows, list):
        income_rows = sorted(
            (row for row in income_rows if isinstance(row, dict)),
            key=lambda row: row.get("date") or row.get("fiscalYear") or row.get("calendarYear") or "",
        )
        if income_rows:
            latest_income = income_rows[-1]
            diluted_shares = _number(latest_income.get("weightedAverageShsOutDil"))
            if diluted_shares is not None:
                share_count = diluted_shares
            statement_currency = latest_income.get("reportedCurrency") or latest_income.get("currency")
        else:
            statement_currency = None
    else:
        statement_currency = None
    return {
        "annual_observations": observations,
        "current_share_count": share_count,
        "financial_statement_currency": str(statement_currency).upper() if statement_currency else None,
    }


def _year(record: Dict[str, Any]) -> Optional[int]:
    value = record.get("fiscal_year")
    try:
        return int(float(value))
    except (TypeError, ValueError):
        date = str(record.get("date") or "")
        return int(date[:4]) if len(date) >= 4 and date[:4].isdigit() else None


def _growth_assumptions(revenue_rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    valid = [
        {**row, "revenue": _number(row.get("revenue"))}
        for row in revenue_rows
        if _number(row.get("revenue")) is not None and _number(row.get("revenue")) > 0
    ]
    if len(valid) < 3:
        return None

    yoy_growth = []
    for prior, current in zip(valid, valid[1:]):
        prior_year, current_year = _year(prior), _year(current)
        if prior_year is not None and current_year is not None and current_year - prior_year != 1:
            continue
        yoy_growth.append(current["revenue"] / prior["revenue"] - 1)
    if not yoy_growth:
        return None

    oldest, latest = valid[0], valid[-1]
    oldest_year, latest_year = _year(oldest), _year(latest)
    span_years = latest_year - oldest_year if oldest_year is not None and latest_year is not None else len(valid) - 1
    if span_years <= 0:
        return None

    multi_year_cagr = (latest["revenue"] / oldest["revenue"]) ** (1 / span_years) - 1
    latest_yoy = yoy_growth[-1]
    raw_base = 0.70 * multi_year_cagr + 0.30 * latest_yoy
    base = _clamp(raw_base, 0.0, 0.20)
    growth_std_dev = statistics.pstdev(yoy_growth)
    spread = _clamp(0.5 * growth_std_dev, 0.02, 0.05)
    raw = {
        "conservative": base - spread,
        "base": raw_base,
        "optimistic": base + spread,
    }
    return {
        "conservative": _clamp(raw["conservative"], 0.0, 0.25),
        "base": base,
        "optimistic": _clamp(raw["optimistic"], 0.0, 0.25),
        "latest_yoy_growth": latest_yoy,
        "multi_year_cagr": multi_year_cagr,
        "historical_growth_std_dev": growth_std_dev,
        "scenario_spread": spread,
        "raw_pre_clamp": raw,
        "valid_yoy_growth": yoy_growth,
    }


def _margin_assumptions(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    valid = [_number(row.get("fcf_margin")) for row in rows]
    valid = [value for value in valid if value is not None]
    if len(valid) < 2:
        return None

    recent = valid[-3:]
    raw_base = statistics.median(recent)
    raw_conservative = min(recent)
    raw_optimistic = max(recent)
    base = _clamp(raw_base, 0.0, 0.40)
    conservative = min(_clamp(raw_conservative, 0.0, 0.40), base)
    optimistic = max(_clamp(raw_optimistic, 0.0, 0.40), base)
    return {
        "conservative": conservative,
        "base": base,
        "optimistic": optimistic,
        "raw_pre_clamp": {
            "conservative": raw_conservative,
            "base": raw_base,
            "optimistic": raw_optimistic,
        },
        "valid_fcf_margins": valid,
        "recent_fcf_margins": recent,
    }


def terminal_multiple(revenue_growth: float) -> Dict[str, float]:
    growth_derived = 12.0 + 0.8 * (revenue_growth * 100.0)
    return {
        "growth_derived_multiple": growth_derived,
        "quality_adjustment": 0.0,
        "terminal_p_fcf": _clamp(growth_derived, 12.0, 30.0),
    }


def quote_currency_from_payload(quote: Dict[str, Any]) -> Optional[str]:
    if not isinstance(quote, dict):
        return None
    currency = quote.get("currency") or quote.get("reportedCurrency")
    if currency:
        return str(currency).upper()
    exchange = str(quote.get("exchange") or quote.get("exchangeFullName") or "").upper()
    if any(name in exchange for name in ("NASDAQ", "NYSE", "AMEX", "NEW YORK STOCK EXCHANGE", "OTC")):
        return "USD"
    return None


def _unavailable(reason: str) -> Dict[str, Any]:
    return {"applicable": False, "reason": reason}


def calculate_suggested_valuation(
    valuation_inputs: Dict[str, Any],
    current_price: Any,
    quote_currency: Optional[str] = None,
) -> Dict[str, Any]:
    rows = valuation_inputs.get("annual_observations", []) if isinstance(valuation_inputs, dict) else []
    rows = [row for row in rows if isinstance(row, dict)][-5:]
    latest_revenue = _number(rows[-1].get("revenue")) if rows else None
    if latest_revenue is None or latest_revenue <= 0:
        return _unavailable("positive latest revenue not available")
    revenue_rows = [row for row in rows if _number(row.get("revenue")) is not None and _number(row.get("revenue")) > 0]
    current_revenue = latest_revenue

    share_count = _number(valuation_inputs.get("current_share_count"))
    if share_count is None or share_count <= 0:
        return _unavailable("positive current share count not available")
    statement_currency = valuation_inputs.get("financial_statement_currency")
    if statement_currency and quote_currency and str(statement_currency).upper() != str(quote_currency).upper():
        return _unavailable("financial statement and quote currencies are not directly comparable")
    if len(revenue_rows) < 3:
        return _unavailable("insufficient annual revenue history")

    growth = _growth_assumptions(revenue_rows)
    if growth is None:
        return _unavailable("insufficient annual revenue history")
    margins = _margin_assumptions(rows)
    if margins is None:
        return _unavailable("insufficient annual FCF-margin history")
    if margins["base"] <= 0:
        return _unavailable("positive normalized FCF basis not established")

    price = _number(current_price)
    if price is None or price <= 0:
        return _unavailable("positive current price not available")
    result: Dict[str, Any] = {
        "applicable": True,
        "horizon_years": HORIZON_YEARS,
        "required_return": REQUIRED_RETURN,
        "current_price": price,
        "current_revenue": current_revenue,
        "current_share_count": share_count,
    }
    for name in ("conservative", "base", "optimistic"):
        assumed_growth = growth[name]
        assumed_margin = margins[name]
        multiple = terminal_multiple(assumed_growth)
        future_revenue = current_revenue * (1 + assumed_growth) ** HORIZON_YEARS
        future_fcf = future_revenue * assumed_margin
        future_equity_value = future_fcf * multiple["terminal_p_fcf"]
        future_share_price = future_equity_value / share_count
        buy_below_price = future_share_price / (1 + REQUIRED_RETURN) ** HORIZON_YEARS
        implied_return = (future_share_price / price) ** (1 / HORIZON_YEARS) - 1
        result[name] = {
            "revenue_cagr": assumed_growth,
            "fcf_margin": assumed_margin,
            **multiple,
            "future_revenue": future_revenue,
            "future_fcf": future_fcf,
            "future_equity_value": future_equity_value,
            "future_share_price": future_share_price,
            "buy_below_price": buy_below_price,
            "implied_return_at_current_price": implied_return,
        }

    result["methodology"] = {
        "share_count_assumption": "constant",
        "revenue_growth_method": "70% longest-span annual revenue CAGR + 30% latest YoY growth; base clamped to 0%-20%; scenario spread is 0.5x population standard deviation of valid YoY growth clamped to 2pp-5pp; scenarios clamped to 0%-25%",
        "fcf_margin_method": "conservative/base/optimistic are the minimum/median/maximum of the latest 3 valid annual FCF margins; scenarios clamped to 0%-40% and ordered",
        "terminal_multiple_method": "12 + 0.8x assumed revenue growth percentage; final multiple clamped to 12x-30x; no quality adjustment",
        "fcf_definition": "freeCashFlow from the existing annual Stage5_1 cash-flow history",
        "growth_debug": growth,
        "fcf_margin_debug": margins,
    }
    return result
