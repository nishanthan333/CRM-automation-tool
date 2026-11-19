import sqlite3

import pytest

from crm.db import connect, connect_readonly, migrate, upsert

TABLES = ["contacts", "companies", "deals", "engagements", "owners",
          "associations", "sync_state", "pipelines", "stages",
          "signal_results", "snoozes"]


def test_migrate_creates_all_tables(tmp_path):
    conn = connect(tmp_path / "t.db")
    migrate(conn)
    names = {r[0] for r in conn.execute(
        "select name from sqlite_master where type='table'")}
    assert set(TABLES) <= names


def test_migrate_is_idempotent(tmp_path):
    conn = connect(tmp_path / "t.db")
    migrate(conn)
    migrate(conn)
    for t in TABLES:
        n = conn.execute(
            "select count(*) from sqlite_master where type='table' and name=?",
            (t,)).fetchone()[0]
        assert n == 1
    assert conn.execute("select count(*) from deals").fetchone()[0] == 0


def test_required_indexes_exist(tmp_path):
    conn = connect(tmp_path / "t.db")
    migrate(conn)
    sql = " ".join(r[0] or "" for r in conn.execute(
        "select sql from sqlite_master where type='index'"))
    for col in ("hubspot_owner_id", "hs_last_activity_date", "dealstage",
                "closedate", "archived", "amount_in_home_currency"):
        assert col in sql


def test_upsert_replaces_on_id_and_preserves_raw(tmp_path):
    conn = connect(tmp_path / "t.db")
    migrate(conn)
    upsert(conn, "deals", [{"id": "1", "dealname": "A", "raw": '{"x":1}'}])
    upsert(conn, "deals", [{"id": "1", "dealname": "B", "raw": '{"x":2}'}])
    row = conn.execute("select dealname, raw from deals").fetchone()
    assert tuple(row) == ("B", '{"x":2}')
    assert conn.execute("select count(*) from deals").fetchone()[0] == 1


def test_readonly_connection_rejects_writes(tmp_path):
    p = tmp_path / "t.db"
    migrate(connect(p))
    with pytest.raises(sqlite3.OperationalError):
        connect_readonly(p).execute("delete from deals")
