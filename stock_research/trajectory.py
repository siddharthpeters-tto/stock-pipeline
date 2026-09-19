import math
from typing import Any, Dict, List, Optional, Tuple


ANNUAL_SOURCES = ("income", "cashflow", "balance", "ratios", "key_metrics")


def _number(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _period_metadata(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], str]:
    date = row.get("date") or row.get("fiscalDateEnding")
    date = str(date) if date else None
    fiscal_year = row.get("fiscalYear") or row.get("calendarYear")
    if fiscal_year is None and date and len(date) >= 4:
        fiscal_year = date[:4]
    fiscal_year = str(fiscal_year) if fiscal_year is not None else None
    period = str(row.get("period") or "FY")
    return date, fiscal_year, period


def _same_period(record: Dict[str, Any], date: Optional[str], fiscal_year: Optional[str], period: str) -> bool:
    if date and record.get("date") == date:
        record_year = record.get("fiscal_year")
        return not fiscal_year or not record_year or record_year == fiscal_year
    return bool(
        fiscal_year
        and record.get("fiscal_year") == fiscal_year
        and (not record.get("period") or record.get("period") == period)
    )


def _period_sort_key(record: Dict[str, Any]) -> Tuple[int, str, str]:
    fiscal_year = record.get("fiscal_year")
    try:
        year = int(float(fiscal_year))
    except (TypeError, ValueError):
        year = -1
    return year, record.get("date") or "", record.get("period") or ""


def _set_numeric(record: Dict[str, Any], field: str, value: Any) -> None:
    number = _number(value)
    if number is not None:
        record[field] = number


def align_annual_history(bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    source_fields = {
        "income": {
            "revenue": "revenue",
            "gross_profit": "grossProfit",
            "operating_income": "operatingIncome",
            "ebitda": "ebitda",
            "shares": "weightedAverageShsOut",
        },
        "cashflow": {
            "fcf": "freeCashFlow",
            "ocf": "operatingCashFlow",
            "capex": "capitalExpenditure",
        },
        "ratios": {
            "gross_margin": "grossProfitMargin",
            "operating_margin": "operatingProfitMargin",
        },
        "key_metrics": {
            "roic": "returnOnInvestedCapital",
            "sbc_to_revenue": "stockBasedCompensationToRevenue",
        },
    }

    for source in ANNUAL_SOURCES:
        rows = bundle.get(source, [])
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue
            date, fiscal_year, period = _period_metadata(row)
            if not date and not fiscal_year:
                continue

            record = next(
                (
                    candidate
                    for candidate in records
                    if _same_period(candidate, date, fiscal_year, period)
                ),
                None,
            )
            if record is None:
                record = {
                    "fiscal_year": fiscal_year,
                    "date": date,
                    "period": period,
                }
                records.append(record)
            elif not record.get("date") and date:
                record["date"] = date
            elif not record.get("fiscal_year") and fiscal_year:
                record["fiscal_year"] = fiscal_year

            for field, source_field in source_fields.get(source, {}).items():
                _set_numeric(record, field, row.get(source_field))

    for record in records:
        revenue = record.get("revenue")
        if record.get("gross_margin") is None and revenue not in (None, 0):
            if record.get("gross_profit") is not None:
                record["gross_margin"] = record["gross_profit"] / revenue
        if record.get("operating_margin") is None and revenue not in (None, 0):
            if record.get("operating_income") is not None:
                record["operating_margin"] = record["operating_income"] / revenue
        if revenue not in (None, 0):
            if record.get("ebitda") is not None:
                record["ebitda_margin"] = record["ebitda"] / revenue
            if record.get("fcf") is not None:
                record["fcf_margin"] = record["fcf"] / revenue

    records.sort(key=_period_sort_key)
    return records


def _is_adjacent(latest: Dict[str, Any], prior: Dict[str, Any]) -> bool:
    try:
        return int(float(latest.get("fiscal_year"))) - int(float(prior.get("fiscal_year"))) == 1
    except (TypeError, ValueError):
        return True


def _latest_prior(history: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not history:
        return None, None
    latest = history[-1]
    prior = history[-2] if len(history) >= 2 and _is_adjacent(latest, history[-2]) else None
    return latest, prior


def _value(record: Optional[Dict[str, Any]], field: str) -> Optional[float]:
    if not record or field not in record:
        return None
    return _number(record[field])


def _delta(latest: Optional[float], prior: Optional[float]) -> Optional[float]:
    if latest is None or prior is None:
        return None
    return latest - prior


def _period_label(record: Optional[Dict[str, Any]]) -> Optional[str]:
    if not record:
        return None
    fiscal_year = record.get("fiscal_year")
    return f"FY{fiscal_year}" if fiscal_year else record.get("date")


def compute_trajectory_metrics(bundle: Dict[str, Any]) -> Dict[str, Any]:
    history = align_annual_history(bundle)
    latest, prior = _latest_prior(history)
    prior_prior = None
    if prior is not None and len(history) >= 3 and _is_adjacent(prior, history[-3]):
        prior_prior = history[-3]

    latest_revenue = _value(latest, "revenue")
    prior_revenue = _value(prior, "revenue")
    prior_prior_revenue = _value(prior_prior, "revenue")

    revenue_growth_latest = None
    revenue_growth_prior = None
    if latest_revenue is not None and prior_revenue is not None and prior_revenue > 0:
        revenue_growth_latest = latest_revenue / prior_revenue - 1
    if prior_revenue is not None and prior_prior_revenue is not None and prior_prior_revenue > 0:
        revenue_growth_prior = prior_revenue / prior_prior_revenue - 1

    gross_margin_latest = _value(latest, "gross_margin")
    gross_margin_prior = _value(prior, "gross_margin")
    operating_margin_latest = _value(latest, "operating_margin")
    operating_margin_prior = _value(prior, "operating_margin")
    ebitda_margin_latest = _value(latest, "ebitda_margin")
    ebitda_margin_prior = _value(prior, "ebitda_margin")
    fcf_latest = _value(latest, "fcf")
    fcf_margin_latest = _value(latest, "fcf_margin")
    fcf_margin_prior = _value(prior, "fcf_margin")
    roic_latest = _value(latest, "roic")
    roic_prior = _value(prior, "roic")
    ocf_latest = _value(latest, "ocf")
    ocf_prior = _value(prior, "ocf")
    shares_latest = _value(latest, "shares")
    shares_prior = _value(prior, "shares")
    sbc_latest = _value(latest, "sbc_to_revenue")
    sbc_prior = _value(prior, "sbc_to_revenue")

    gross_margin_values = [record.get("gross_margin") for record in history if record.get("gross_margin") is not None]
    gross_margin_delta_multi = None
    if len(gross_margin_values) >= 3:
        gross_margin_delta_multi = gross_margin_values[-1] - gross_margin_values[0]

    return {
        "basis": "FY",
        "periods": {
            "history": [_period_label(record) for record in history],
            "latest": _period_label(latest),
            "prior": _period_label(prior),
        },
        "revenue_growth_latest": revenue_growth_latest,
        "revenue_growth_prior": revenue_growth_prior,
        "revenue_growth_delta": _delta(revenue_growth_latest, revenue_growth_prior),
        "gross_margin_latest": gross_margin_latest,
        "gross_margin_prior": gross_margin_prior,
        "gross_margin_delta_1y": _delta(gross_margin_latest, gross_margin_prior),
        "gross_margin_delta_multi_period": gross_margin_delta_multi,
        "operating_margin_latest": operating_margin_latest,
        "operating_margin_prior": operating_margin_prior,
        "operating_margin_delta_1y": _delta(operating_margin_latest, operating_margin_prior),
        "ebitda_margin_latest": ebitda_margin_latest,
        "ebitda_margin_prior": ebitda_margin_prior,
        "ebitda_margin_delta_1y": _delta(ebitda_margin_latest, ebitda_margin_prior),
        "fcf_latest": fcf_latest,
        "fcf_margin_latest": fcf_margin_latest,
        "fcf_margin_prior": fcf_margin_prior,
        "fcf_margin_delta_1y": _delta(fcf_margin_latest, fcf_margin_prior),
        "roic_latest": roic_latest,
        "roic_prior": roic_prior,
        "roic_delta_1y": _delta(roic_latest, roic_prior),
        "ocf_latest": ocf_latest,
        "ocf_prior": ocf_prior,
        "ocf_change": _delta(ocf_latest, ocf_prior),
        "shares_latest": shares_latest,
        "shares_prior": shares_prior,
        "share_count_change_1y": _delta(shares_latest, shares_prior),
        "sbc_to_revenue_latest": sbc_latest,
        "sbc_to_revenue_prior": sbc_prior,
        "sbc_to_revenue_delta_1y": _delta(sbc_latest, sbc_prior),
    }