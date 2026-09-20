from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .fmp_common import to_float


def _period_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    date = str(row.get("date") or row.get("fiscalDateEnding") or "")
    fiscal_year = str(row.get("fiscalYear") or row.get("calendarYear") or (date[:4] if date else ""))
    period = str(row.get("period") or "")
    return date, fiscal_year, period


def _align_rows(
    income: Any,
    cashflow: Any,
) -> List[Dict[str, Any]]:
    records: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for source, rows in (("income", income), ("cashflow", cashflow)):
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = _period_key(row)
            if not any(key):
                continue
            matching_key = next(
                (
                    existing_key for existing_key in records
                    if key[1] and key[2]
                    and existing_key[1] == key[1]
                    and existing_key[2] == key[2]
                ),
                key,
            )
            record = records.setdefault(matching_key, {
                "date": key[0] or None,
                "fiscal_year": key[1] or None,
                "period": key[2] or None,
            })
            fields = {
                "revenue": "revenue",
                "operating_income": "operatingIncome",
                "ebitda": "ebitda",
            } if source == "income" else {
                "operating_cash_flow": "operatingCashFlow",
                "capex": "capitalExpenditure",
            }
            for target, field in fields.items():
                value = to_float(row.get(field))
                if value is not None:
                    record[target] = value

    def sort_key(row):
        try:
            year = int(row.get("fiscal_year"))
        except (TypeError, ValueError):
            year = -1
        try:
            quarter = int(str(row.get("period") or "").lstrip("Q"))
        except (TypeError, ValueError):
            quarter = 0
        return year, quarter, row.get("date") or ""

    return sorted(records.values(), key=sort_key)


def _margin(numerator: Optional[float], revenue: Optional[float]) -> Optional[float]:
    if numerator is None or revenue in (None, 0):
        return None
    return numerator / revenue


def _quarter_label(row: Dict[str, Any]) -> Optional[str]:
    period = row.get("period")
    fiscal_year = row.get("fiscal_year")
    if not period and not fiscal_year:
        return None
    year = str(fiscal_year or "")
    year_label = year[-2:] if len(year) >= 2 else year
    return f"{period} FY{year_label}" if period else f"FY{year_label}"


def _display_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").strftime("%d %b %Y").lstrip("0")
    except ValueError:
        return value


def fetch_quarterly_bundle(api, symbol: str, read_cache, write_cache) -> Dict[str, Any]:
    requests = {
        "income": ("income", "/income-statement"),
        "cashflow": ("cashflow", "/cash-flow-statement"),
    }
    params = {"period": "quarter", "limit": 20}
    bundle = {}
    for name, (cache_endpoint, api_endpoint) in requests.items():
        cached = read_cache(symbol, cache_endpoint, params)
        if cached is not None:
            bundle[name] = cached
            continue
        data = api.get(api_endpoint, {"symbol": symbol, **params})
        bundle[name] = data if isinstance(data, list) else []
        write_cache(symbol, cache_endpoint, bundle[name], params)
    return bundle


def compute_quarterly_pulse(bundle: Dict[str, Any]) -> Dict[str, Any]:
    history = _align_rows(bundle.get("income"), bundle.get("cashflow"))
    latest_four = history[-4:]
    by_key = {(row.get("fiscal_year"), row.get("period")): row for row in history}
    output_rows = []

    for row in latest_four:
        revenue = row.get("revenue")
        fcf = None
        if row.get("operating_cash_flow") is not None and row.get("capex") is not None:
            fcf = row["operating_cash_flow"] + row["capex"]
        fiscal_year = str(row.get("fiscal_year") or "")
        prior_year = by_key.get((str(int(fiscal_year) - 1), row.get("period"))) if fiscal_year.isdigit() else None
        prior_revenue = prior_year.get("revenue") if prior_year else None

        output_rows.append({
            "quarter": _quarter_label(row),
            "date": row.get("date"),
            "revenue": revenue,
            "revenue_yoy_growth": revenue / prior_revenue - 1 if revenue is not None and prior_revenue not in (None, 0) else None,
            "operating_margin": _margin(row.get("operating_income"), revenue),
            "ebitda_margin": _margin(row.get("ebitda"), revenue),
            "fcf": fcf,
            "fcf_margin": _margin(fcf, revenue),
        })

    latest = output_rows[-1] if output_rows else {}
    return {
        "basis": "quarterly",
        "latest_reported": {
            "quarter": latest.get("quarter"),
            "date": latest.get("date"),
            "display": f"{latest['quarter']} · {_display_date(latest.get('date'))}" if latest.get("quarter") and latest.get("date") else None,
        },
        "quarters": list(reversed(output_rows)),
    }