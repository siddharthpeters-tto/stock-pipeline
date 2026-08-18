"""
Tests for rotation_monitor.storage against a temp SQLite file (never the
real rotation.db) — dedupe/upsert behavior and ordering.
"""

import os

import pytest

from rotation_monitor import storage


@pytest.fixture(autouse=True)
def temp_db(tmp_path, monkeypatch):
    db_dir = tmp_path / "rotation_db"
    monkeypatch.setattr(storage, "DB_DIR", str(db_dir))
    monkeypatch.setattr(storage, "DB_PATH", str(db_dir / "rotation_test.db"))
    storage.init_db()
    yield


def test_upsert_and_read_back():
    bars = [
        {"date": "2025-01-02", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 1000},
        {"date": "2025-01-03", "open": 10.5, "high": 12, "low": 10, "close": 11.5, "volume": 1200},
    ]
    written = storage.upsert_bars("TEST", bars)
    assert written == 2

    rows = storage.get_price_history("TEST")
    assert len(rows) == 2
    assert rows[0][0] == "2025-01-02"  # ascending order
    assert rows[1][0] == "2025-01-03"


def test_upsert_is_idempotent_on_same_date():
    bars = [{"date": "2025-01-02", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 1000}]
    storage.upsert_bars("TEST", bars)
    storage.upsert_bars("TEST", bars)  # re-insert same date

    rows = storage.get_price_history("TEST")
    assert len(rows) == 1


def test_upsert_updates_existing_row_on_conflict():
    storage.upsert_bars("TEST", [{"date": "2025-01-02", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 1000}])
    storage.upsert_bars("TEST", [{"date": "2025-01-02", "open": 10, "high": 11, "low": 9, "close": 99.9, "volume": 5000}])

    rows = storage.get_price_history("TEST")
    assert len(rows) == 1
    assert rows[0][4] == 99.9  # close column updated, not duplicated


def test_upsert_skips_rows_missing_date_or_close():
    bars = [
        {"date": None, "close": 10.0},
        {"date": "2025-01-02", "close": None},
        {"date": "2025-01-03", "close": 5.0},
    ]
    written = storage.upsert_bars("TEST", bars)
    assert written == 1
    rows = storage.get_price_history("TEST")
    assert len(rows) == 1
    assert rows[0][0] == "2025-01-03"


def test_get_latest_date_none_when_empty():
    assert storage.get_latest_date("NOPE") is None


def test_get_latest_date_returns_max():
    storage.upsert_bars("TEST", [
        {"date": "2025-01-02", "close": 1},
        {"date": "2025-01-05", "close": 2},
        {"date": "2025-01-03", "close": 3},
    ])
    assert storage.get_latest_date("TEST") == "2025-01-05"
