import sqlite3
from pathlib import Path

_OBJECT = "id TEXT PRIMARY KEY, raw TEXT, archived INTEGER NOT NULL DEFAULT 0"

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS deals (
    {_OBJECT},
    dealname TEXT, dealstage TEXT, pipeline TEXT,
    amount_in_home_currency REAL, closedate INTEGER, hubspot_owner_id TEXT,
    hs_last_activity_date INTEGER, hs_lastmodifieddate INTEGER, createdate INTEGER
);
CREATE TABLE IF NOT EXISTS companies (
    {_OBJECT},
    name TEXT, domain TEXT, hubspot_owner_id TEXT,
    hs_last_activity_date INTEGER, hs_lastmodifieddate INTEGER
);
CREATE TABLE IF NOT EXISTS contacts (
    {_OBJECT},
    firstname TEXT, lastname TEXT, email TEXT, hubspot_owner_id TEXT,
    hs_last_activity_date INTEGER, lastmodifieddate INTEGER
);
CREATE TABLE IF NOT EXISTS engagements (
    {_OBJECT},
    type TEXT, timestamp INTEGER, hs_lastmodifieddate INTEGER
);
CREATE TABLE IF NOT EXISTS owners (
    {_OBJECT},
    email TEXT, first_name TEXT, last_name TEXT
);
CREATE TABLE IF NOT EXISTS pipelines (
    {_OBJECT},
    label TEXT
);
CREATE TABLE IF NOT EXISTS stages (
    {_OBJECT},
    pipeline_id TEXT, label TEXT, display_order INTEGER,
    is_closed INTEGER, probability REAL
);
CREATE TABLE IF NOT EXISTS associations (
    from_type TEXT NOT NULL, from_id TEXT NOT NULL,
    to_type TEXT NOT NULL, to_id TEXT NOT NULL,
    PRIMARY KEY (from_type, from_id, to_type, to_id)
);
CREATE TABLE IF NOT EXISTS sync_state (
    object_type TEXT PRIMARY KEY, last_modified_ms INTEGER, cursor TEXT,
    phase TEXT, last_run_at INTEGER, status TEXT
);
CREATE TABLE IF NOT EXISTS signal_results (
    signal_id TEXT, record_id TEXT, record_type TEXT, owner_id TEXT,
    value REAL, computed_at INTEGER
);
CREATE TABLE IF NOT EXISTS snoozes (
    signal_id TEXT NOT NULL, record_id TEXT NOT NULL,
    until INTEGER, created_at INTEGER, fingerprint TEXT,
    PRIMARY KEY (signal_id, record_id)
);

CREATE INDEX IF NOT EXISTS idx_deals_owner ON deals(hubspot_owner_id);
CREATE INDEX IF NOT EXISTS idx_deals_activity ON deals(hs_last_activity_date);
CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(dealstage);
CREATE INDEX IF NOT EXISTS idx_deals_closedate ON deals(closedate);
CREATE INDEX IF NOT EXISTS idx_deals_archived ON deals(archived);
CREATE INDEX IF NOT EXISTS idx_deals_amount ON deals(amount_in_home_currency);
CREATE INDEX IF NOT EXISTS idx_companies_owner ON companies(hubspot_owner_id);
CREATE INDEX IF NOT EXISTS idx_companies_activity ON companies(hs_last_activity_date);
CREATE INDEX IF NOT EXISTS idx_companies_archived ON companies(archived);
CREATE INDEX IF NOT EXISTS idx_contacts_owner ON contacts(hubspot_owner_id);
CREATE INDEX IF NOT EXISTS idx_contacts_activity ON contacts(hs_last_activity_date);
CREATE INDEX IF NOT EXISTS idx_contacts_archived ON contacts(archived);
CREATE INDEX IF NOT EXISTS idx_assoc_to ON associations(to_type, to_id);
CREATE INDEX IF NOT EXISTS idx_signal_results_owner ON signal_results(owner_id);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    with conn:
        for stmt in SCHEMA.split(";"):
            if stmt.strip():
                conn.execute(stmt)


def upsert_sql(conn: sqlite3.Connection, table: str, cols: list[str]) -> str:
    """The `INSERT ... ON CONFLICT(id)` statement `upsert` executes.

    Exposed so callers that need the write to share a transaction with other
    statements (the sync layer checkpoints its cursor alongside each page) can
    execute it inside a transaction of their own.
    """
    known = {r["name"] for r in conn.execute(f'PRAGMA table_info("{table}")')}
    if not known or "id" not in known or not set(cols) <= known:
        raise ValueError(f"cannot upsert into {table!r} with columns {cols}")
    names = ", ".join(f'"{c}"' for c in cols)
    marks = ", ".join(f":{c}" for c in cols)
    sets = ", ".join(f'"{c}"=excluded."{c}"' for c in cols if c != "id")
    action = f"DO UPDATE SET {sets}" if sets else "DO NOTHING"
    return (f'INSERT INTO "{table}" ({names}) VALUES ({marks}) '
            f"ON CONFLICT(id) {action}")


def upsert(conn: sqlite3.Connection, table: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = upsert_sql(conn, table, list(rows[0]))
    with conn:
        conn.executemany(sql, rows)
    return len(rows)
