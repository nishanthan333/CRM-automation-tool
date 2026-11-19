import json
import sqlite3
from pathlib import Path

import httpx
import pytest

from crm.ask import (AskResult, ModelUnavailable, QueryTimeout, UnsafeSQL, ask,
                     generate_sql, guard_sql, run_query, schema_prompt,
                     strip_model_output)
from crm.config import Config
from crm.db import upsert
from tests.conftest import add_deal

SLOW_CROSS_JOIN = (
    "with recursive c(x) as (select 1 union all select x + 1 from c "
    "where x < 1000000000000) select count(*) from c a, c b")


@pytest.fixture
def db_path(conn):
    for i in range(30):
        add_deal(conn, f"d{i}")
    return Path(conn.execute("PRAGMA database_list").fetchone()["file"])


@pytest.fixture
def cfg(db_path):
    return Config(hubspot_token="t", db_path=db_path, timezone="local",
                  week_starts_on="monday", ollama_model="test-model",
                  ollama_url="http://ollama.test", sync_interval_minutes=15)


def stub(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def replying(text):
    return stub(lambda req: httpx.Response(200, json={"response": text}))


def refusing():
    def handler(req):
        raise httpx.ConnectError("refused", request=req)
    return stub(handler)


# --- schema prompt ---------------------------------------------------------

def test_schema_prompt_contains_table_and_column_names(conn):
    p = schema_prompt(conn)
    assert "deals" in p and "amount_in_home_currency" in p
    assert "sqlite_" not in p


def test_schema_prompt_contains_no_row_data(conn):
    sentinel = "Acme Secret Project"
    upsert(conn, "deals", [{"id": "d1", "dealname": sentinel, "raw": sentinel}])
    upsert(conn, "companies", [{"id": "c1", "name": sentinel}])
    # The tables are non-empty, so a leak would be visible here.
    assert conn.execute("SELECT count(*) FROM deals WHERE dealname = ?",
                        (sentinel,)).fetchone()[0] == 1
    p = schema_prompt(conn)
    assert sentinel not in p
    assert "'d1'" not in p and "c1" not in p.replace("c1_", "")


def test_the_archived_rule_names_the_only_tables_that_have_the_column(conn):
    """`archived` exists on the five object tables only, and schema_prompt
    ships every table, so an unscoped rule invites `no such column: archived`
    on any question about a snooze or a flag."""
    from crm.ask import _INSTRUCTIONS
    rule = next(line for line in _INSTRUCTIONS.splitlines()
                if "archived" in line and line.startswith("-"))
    schema = schema_prompt(conn)
    without = [name for name in (
        "associations", "sync_state", "signal_results", "snoozes", "meta")
        if f"{name}(" in schema]
    assert without, "schema_prompt no longer ships the column-less tables"
    for name in ("contacts", "companies", "deals", "engagements", "owners"):
        assert name in rule, name
    assert "never reference it elsewhere" in rule


# --- guard -----------------------------------------------------------------

@pytest.mark.parametrize("bad", [
    "delete from deals",
    "update deals set amount_in_home_currency = 0",
    "select 1; drop table deals",
    "select 1;;",
    "select 1; select 2",
    "attach database '/etc/passwd' as p",
    "pragma writable_schema = 1",
    "with x as (select 1) insert into deals select * from x",
    "with x as (select 1) delete from deals",
    "with x as (select 1 as a) replace into deals(id) select a from x",
    "with x as (select 1) update deals set archived = 1",
    "select 1 where replace into",
    "SELECT 1 -- ; delete from deals",
    "-- hi\nselect 1",
    "/* hi */ select 1",
    "select 1 /* x */",
    "select * from (select * from (pragma_table_info('deals')))"
    " where (select 1 from (pragma writable_schema))",
    "SeLeCt 1; DrOp TABLE deals",
    "insert into deals(id) values ('x')",
    "replace into deals(id) values ('x')",
    "vacuum",
    "(select 1)",
    "begin; select 1",
    "",
    "   ",
    "select 'unterminated",
    "select 1 from [deals",
])
def test_guard_rejects_non_select(bad):
    with pytest.raises(UnsafeSQL):
        guard_sql(bad)


@pytest.mark.parametrize("ok", [
    "select count(*) from deals",
    "  \n\tSELECT count(*) FROM deals;  ",
    "﻿select 1",
    "with x as (select 1 as a) select a from x",
    "select 'a;b', 'delete -- x', \"update\" from deals",
    "select 'it''s; pragma' from deals",
    "select replace(dealname, 'a', 'b'), replace (id, 'x', 'y') from deals",
    "select case when 1 then replace(dealname, 'a', 'b') else 0 end from deals",
])
def test_guard_accepts_single_select(ok):
    assert guard_sql(ok).lower().startswith(("select", "with"))


def test_guard_normalises_whitespace_and_trailing_semicolon():
    assert guard_sql("  select  1 \n from   deals ;") == "select 1 from deals"


def test_guard_leaves_string_literals_alone():
    assert guard_sql("select 'a  b'") == "select 'a  b'"


@pytest.mark.parametrize("raw", [
    "Here is the SQL you asked for:\n```sql\nselect 1 from deals\n```",
    "```\nselect 1 from deals\n```",
    "```sql\nSELECT 1 FROM deals;\n```\nThis counts deals.",
    "Sure, here it is:\nselect 1 from deals",
    "Sure, with pleasure:\nselect 1 from deals",
])
def test_fenced_or_prefaced_output_is_unwrapped(raw):
    assert guard_sql(strip_model_output(raw)).lower() == "select 1 from deals"


@pytest.mark.parametrize("prose", [
    "I'm not sure what you mean.",
    "I can't help with that.",
    "Deals with no owner cannot be counted.",
    "insert into deals(id) select 'x' from (select 1)",
])
def test_prose_or_dml_with_no_sql_line_raises_unsafe(prose):
    with pytest.raises(UnsafeSQL):
        strip_model_output(prose)


def test_comment_error_is_actionable():
    with pytest.raises(UnsafeSQL, match="remove the SQL comment"):
        guard_sql("select 1 -- x")


# --- run_query -------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "delete from deals",
    "with x as (select 1 as a) replace into deals(id) select a from x",
    "with x as (select 1 as a) insert into deals(id) select a from x",
    "update deals set archived = 1",
    "create table z(a)",
    "drop table deals",
])
def test_run_query_refuses_writes_even_unguarded(db_path, sql):
    with pytest.raises(sqlite3.DatabaseError):
        run_query(db_path, sql, 1.0, 100)
    n = sqlite3.connect(db_path).execute("select count(*) from deals").fetchone()[0]
    assert n == 30


def test_readonly_connection_alone_refuses_writes(db_path):
    # Layer two, independent of the authorizer.
    from crm.db import connect_readonly
    ro = connect_readonly(db_path)
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("delete from deals")
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("with x as (select 1 as a) replace into deals(id) "
                   "select a from x")
    ro.close()


def test_authorizer_alone_refuses_writes_on_writable_connection(db_path):
    from crm.ask import _authorizer
    w = sqlite3.connect(db_path)
    w.set_authorizer(_authorizer)
    for sql in ("with x as (select 1 as a) replace into deals(id) select a from x",
                "delete from deals", "attach database ':memory:' as p",
                "pragma query_only = 0"):
        with pytest.raises(sqlite3.DatabaseError):
            w.execute(sql)
    assert w.execute("select count(*) from deals").fetchone()[0] == 30
    w.close()


def test_authorizer_denies_detach_action():
    import sqlite3 as s
    from crm.ask import _authorizer
    assert _authorizer(s.SQLITE_DETACH, None, None, None, None) == s.SQLITE_DENY
    assert _authorizer(s.SQLITE_SELECT, None, None, None, None) == s.SQLITE_OK


def test_run_query_refuses_attach_and_pragma_even_unguarded(db_path):
    for sql in ("attach database ':memory:' as p", "pragma query_only = 0"):
        with pytest.raises(sqlite3.DatabaseError):
            run_query(db_path, sql, 1.0, 100)


def test_run_query_caps_rows(db_path):
    r = run_query(db_path, "select * from deals", 5.0, 10)
    assert len(r.rows) == 10 and r.truncated
    assert "id" in r.columns
    r = run_query(db_path, "select * from deals", 5.0, 100)
    assert len(r.rows) == 30 and not r.truncated


def test_run_query_times_out_on_long_query(db_path):
    with pytest.raises(QueryTimeout):
        run_query(db_path, SLOW_CROSS_JOIN, 0.2, 100)


# --- generate_sql ----------------------------------------------------------

def test_generate_sql_posts_schema_and_question_to_configured_model(cfg):
    seen = {}

    def handler(req):
        seen["url"] = str(req.url)
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"response": "select 1"})

    out = generate_sql("how many deals?", "deals(id)", cfg, http=stub(handler))
    assert out == "select 1"
    assert seen["url"] == "http://ollama.test/api/generate"
    assert seen["body"]["model"] == "test-model"
    assert seen["body"]["stream"] is False
    assert "deals(id)" in seen["body"]["prompt"]
    assert "how many deals?" in seen["body"]["prompt"]


def test_generate_sql_connect_error_is_model_unavailable(cfg):
    with pytest.raises(ModelUnavailable):
        generate_sql("q", "s", cfg, http=refusing())


def test_generate_sql_http_error_is_model_unavailable(cfg):
    with pytest.raises(ModelUnavailable):
        generate_sql("q", "s", cfg, http=stub(lambda r: httpx.Response(404)))


# --- ask -------------------------------------------------------------------

def test_ask_happy_path(conn, cfg):
    r = ask("how many deals?", conn, cfg,
            http=replying("```sql\nselect count(*) as n from deals\n```"))
    assert isinstance(r, AskResult)
    assert r.error is None and r.sql == "select count(*) as n from deals"
    assert r.columns == ["n"] and r.rows == [(30,)]


def test_ask_reports_model_unavailable_when_ollama_refuses(conn, cfg):
    r = ask("anything", conn, cfg, http=refusing())
    assert r.error and "Ollama" in r.error and r.rows == []


def test_ask_returns_sql_even_when_query_fails(conn, cfg):
    r = ask("bad question", conn, cfg, http=replying("select nope from deals"))
    assert r.sql == "select nope from deals"
    assert r.error and r.rows == []


def test_ask_returns_sql_when_guard_rejects(conn, cfg):
    r = ask("x", conn, cfg, http=replying("delete from deals"))
    assert r.sql == "delete from deals"
    assert r.error and r.rows == []
    assert conn.execute("select count(*) from deals").fetchone()[0] == 30


@pytest.mark.parametrize("text", [
    "I'm not sure what you mean.", "I can't help with that.",
    "insert into deals(id) select 'x' from (select 1)"])
def test_ask_returns_model_text_when_it_is_not_sql(conn, cfg, text):
    r = ask("x", conn, cfg, http=replying(text))
    assert r.sql == text and r.error and r.rows == []
    assert conn.execute("select count(*) from deals").fetchone()[0] == 30


@pytest.mark.parametrize("body", [[], "str", 5, None])
def test_ask_survives_malformed_ollama_json(conn, cfg, body):
    r = ask("x", conn, cfg, http=stub(lambda q: httpx.Response(200, json=body)))
    assert r.error and "Ollama" in r.error


def test_ask_rejects_in_memory_connection(cfg):
    mem = sqlite3.connect(":memory:")
    with pytest.raises(ValueError, match="file-backed"):
        ask("x", mem, cfg, http=refusing())


def test_ask_returns_sql_on_timeout(conn, cfg, monkeypatch):
    monkeypatch.setattr("crm.ask.QUERY_TIMEOUT_S", 0.2)
    r = ask("x", conn, cfg, http=replying(SLOW_CROSS_JOIN))
    assert r.sql and r.error and "time" in r.error.lower()


def test_ask_empty_question_does_not_call_model(conn, cfg):
    r = ask("   ", conn, cfg, http=refusing())
    assert r.error and "Ollama" not in r.error
