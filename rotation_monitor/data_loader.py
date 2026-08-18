"""
rotation_monitor.data_loader — keeps local ETF price history up to date.

For each enabled ticker in the universe:
    - if nothing is cached yet, pull ~2 years of daily bars
    - if data already exists, pull from a few days before the last cached
      date (covers late corrections/holiday backfills) through today, and
      upsert — so a normal run only requests a handful of new rows, not a
      full re-download (§4's caching requirement).

Errors for one symbol (API failure, empty response) are logged and do not
abort the run for the rest of the universe (§35 graceful failure).
"""

import argparse
from datetime import date, timedelta
from typing import Dict, List

from rotation_monitor import config, storage
from rotation_monitor.fmp_client import FmpApiError, RotationFmpClient

# When we already have data, re-fetch this many days of overlap to catch
# late-arriving corrections without re-downloading full history.
BACKFILL_OVERLAP_DAYS = 7


def _normalize_bar(raw: dict) -> dict:
    """Map the dividend-adjusted endpoint's field names to our schema."""
    return {
        "date": raw.get("date"),
        "open": raw.get("adjOpen"),
        "high": raw.get("adjHigh"),
        "low": raw.get("adjLow"),
        "close": raw.get("adjClose"),
        "adj_close": raw.get("adjClose"),
        "volume": raw.get("volume"),
    }


def refresh_symbol(client: RotationFmpClient, symbol: str) -> Dict:
    """Fetch and upsert new bars for one symbol. Returns a status dict."""
    today = date.today()
    latest = storage.get_latest_date(symbol)

    if latest is None:
        date_from = (today - timedelta(days=config.HISTORY_DAYS_TARGET)).isoformat()
    else:
        last_dt = date.fromisoformat(latest)
        date_from = (last_dt - timedelta(days=BACKFILL_OVERLAP_DAYS)).isoformat()

    date_to = today.isoformat()

    try:
        raw_bars = client.get_daily_bars(symbol, date_from, date_to)
    except FmpApiError as e:
        return {"symbol": symbol, "status": "ERROR", "error": str(e), "rows_written": 0}

    if not raw_bars:
        return {"symbol": symbol, "status": "NO_DATA", "rows_written": 0}

    bars = [_normalize_bar(b) for b in raw_bars]
    written = storage.upsert_bars(symbol, bars)

    return {
        "symbol": symbol,
        "status": "OK",
        "rows_written": written,
        "date_from_requested": date_from,
        "date_to_requested": date_to,
        "total_rows_cached": storage.get_row_count(symbol),
    }


def refresh_universe(tickers: List[str] = None) -> List[Dict]:
    storage.init_db()
    client = RotationFmpClient()

    universe = config.load_universe()
    tickers = tickers or list(universe.keys())

    results = []
    for symbol in tickers:
        results.append(refresh_symbol(client, symbol))

    return results


def _print_summary(results: List[Dict]) -> None:
    ok = [r for r in results if r["status"] == "OK"]
    errors = [r for r in results if r["status"] != "OK"]

    print(f"\nRefreshed {len(ok)}/{len(results)} symbols.")
    for r in ok:
        print(
            f"  {r['symbol']:<6} +{r['rows_written']:>4} rows written  "
            f"(cached total: {r['total_rows_cached']})"
        )

    if errors:
        print(f"\n{len(errors)} symbol(s) had problems:")
        for r in errors:
            print(f"  {r['symbol']:<6} {r['status']}  {r.get('error', '')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh cached ETF daily price history.")
    parser.add_argument(
        "--symbols", nargs="*", default=None,
        help="Specific tickers to refresh (default: full enabled universe).",
    )
    args = parser.parse_args()

    results = refresh_universe(args.symbols)
    _print_summary(results)
