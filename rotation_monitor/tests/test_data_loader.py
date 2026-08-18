"""
Tests for rotation_monitor.data_loader — field-mapping, and graceful
handling of missing/erroring data, without hitting the real FMP API or the
real rotation.db.
"""

import pytest

from rotation_monitor import data_loader, storage
from rotation_monitor.fmp_client import FmpApiError


@pytest.fixture(autouse=True)
def temp_db(tmp_path, monkeypatch):
    db_dir = tmp_path / "rotation_db"
    monkeypatch.setattr(storage, "DB_DIR", str(db_dir))
    monkeypatch.setattr(storage, "DB_PATH", str(db_dir / "rotation_test.db"))
    storage.init_db()
    yield


class StubClient:
    def __init__(self, bars=None, raises=None):
        self._bars = bars or []
        self._raises = raises
        self.calls = []

    def get_daily_bars(self, symbol, date_from, date_to):
        self.calls.append((symbol, date_from, date_to))
        if self._raises:
            raise self._raises
        return self._bars


def test_normalize_bar_maps_dividend_adjusted_fields():
    raw = {"symbol": "SPY", "date": "2025-01-02", "adjOpen": 1, "adjHigh": 2,
           "adjLow": 0.5, "adjClose": 1.5, "volume": 1000}
    bar = data_loader._normalize_bar(raw)
    assert bar == {
        "date": "2025-01-02", "open": 1, "high": 2, "low": 0.5,
        "close": 1.5, "adj_close": 1.5, "volume": 1000,
    }


def test_refresh_symbol_first_pull_writes_rows():
    bars = [
        {"date": "2025-01-02", "adjOpen": 1, "adjHigh": 2, "adjLow": 0.5, "adjClose": 1.5, "volume": 1000},
        {"date": "2025-01-03", "adjOpen": 1.5, "adjHigh": 2.5, "adjLow": 1, "adjClose": 2.0, "volume": 1200},
    ]
    client = StubClient(bars=bars)

    result = data_loader.refresh_symbol(client, "TEST")

    assert result["status"] == "OK"
    assert result["rows_written"] == 2
    assert storage.get_row_count("TEST") == 2


def test_refresh_symbol_no_data_status():
    client = StubClient(bars=[])
    result = data_loader.refresh_symbol(client, "TEST")
    assert result["status"] == "NO_DATA"
    assert result["rows_written"] == 0


def test_refresh_symbol_api_error_is_handled_gracefully():
    client = StubClient(raises=FmpApiError("boom"))
    result = data_loader.refresh_symbol(client, "TEST")
    assert result["status"] == "ERROR"
    assert "boom" in result["error"]


def test_refresh_symbol_incremental_uses_backfill_overlap():
    storage.upsert_bars("TEST", [{"date": "2025-01-10", "close": 1.0}])

    client = StubClient(bars=[{"date": "2025-01-10", "adjClose": 1.0, "volume": 1}])
    data_loader.refresh_symbol(client, "TEST")

    date_from_used = client.calls[0][1]
    # should start a week before the last cached date, not from scratch
    assert date_from_used < "2025-01-10"
    assert date_from_used > "2024-06-01"


def test_refresh_symbol_skips_rows_missing_close():
    bars = [
        {"date": "2025-01-02", "adjClose": None, "volume": 1000},
        {"date": "2025-01-03", "adjClose": 2.0, "volume": 1200},
    ]
    client = StubClient(bars=bars)
    result = data_loader.refresh_symbol(client, "TEST")

    assert result["status"] == "OK"
    assert storage.get_row_count("TEST") == 1
