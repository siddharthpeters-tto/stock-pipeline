"""
rotation_monitor.storage — local SQLite store for ETF daily price history.

Chosen over parquet to avoid adding a pyarrow dependency, and because the
access pattern here (point lookups by symbol+date, per-symbol ordered
series, incremental upserts) is a good fit for a simple relational table.
The stdlib `sqlite3` module needs no new dependency.

This module only stores raw price history. Daily rotation-state snapshots
(regime, persistence, rotation flags, etc.) are added in Phase 2+ once that
logic exists.
"""

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterable, List, Optional, Tuple

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_DIR = os.path.join(_REPO_ROOT, "data", "rotation")
DB_PATH = os.path.join(DB_DIR, "rotation.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS etf_prices (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,       -- YYYY-MM-DD
    open REAL,
    high REAL,
    low REAL,
    close REAL,                -- dividend-adjusted close (canonical price)
    adj_close REAL,             -- same as close today; kept as its own
                                 -- column so a future raw/unadjusted series
                                 -- can be added without a schema change
    volume INTEGER,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_etf_prices_symbol_date
    ON etf_prices (symbol, date);
"""


@contextmanager
def _connect():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with _connect() as conn:
        conn.executescript(_SCHEMA)


def upsert_bars(symbol: str, bars: Iterable[dict]) -> int:
    """
    bars: iterable of dicts with keys date, open, high, low, close, volume
    (adj_close defaults to close). Returns number of rows written.
    """
    rows = []
    for b in bars:
        date = b.get("date")
        close = b.get("close")
        if not date or close is None:
            continue
        rows.append((
            symbol,
            date,
            b.get("open"),
            b.get("high"),
            b.get("low"),
            close,
            b.get("adj_close", close),
            b.get("volume"),
        ))

    if not rows:
        return 0

    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO etf_prices (symbol, date, open, high, low, close, adj_close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                adj_close=excluded.adj_close,
                volume=excluded.volume
            """,
            rows,
        )

    return len(rows)


def get_latest_date(symbol: str) -> Optional[str]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT MAX(date) FROM etf_prices WHERE symbol = ?", (symbol,)
        ).fetchone()
    return row[0] if row else None


def get_price_history(symbol: str, start_date: Optional[str] = None) -> List[Tuple]:
    """
    Returns rows (date, open, high, low, close, adj_close, volume) ascending
    by date. `close`/`adj_close` are dividend-adjusted (see schema note).
    """
    query = "SELECT date, open, high, low, close, adj_close, volume FROM etf_prices WHERE symbol = ?"
    params: list = [symbol]
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    query += " ORDER BY date ASC"

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()

    return rows


def get_row_count(symbol: str) -> int:
    with _connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM etf_prices WHERE symbol = ?", (symbol,)
        ).fetchone()
    return row[0] if row else 0


def get_all_symbols() -> List[str]:
    with _connect() as conn:
        rows = conn.execute("SELECT DISTINCT symbol FROM etf_prices ORDER BY symbol").fetchall()
    return [r[0] for r in rows]
