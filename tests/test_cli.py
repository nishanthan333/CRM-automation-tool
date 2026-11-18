import dataclasses
import logging
import os
from pathlib import Path

import httpx
import pytest

from crm import cli
from crm.cli import main
from crm.config import load_config
from crm.db import connect, migrate, upsert
from crm.hubspot import RateLimited
from crm.sync import _state_sql, api_types, backfill_state
from crm.timer import SyncTimer
from tests.conftest import add_deal, add_stage
from tests.fakes import FakeClient

DEFAULTS = Path(__file__).parent.parent / "crm" / "defaults"
ALL_TYPES = ["owners", "companies", "deals", "contacts", "engagements"]


@pytest.fixture
def home(tmp_path, monkeypatch):
    """An initialised home: token-shaped config, default thresholds."""
    (tmp_path / "config.toml").write_text(
        (DEFAULTS / "config.toml.example").read_text().replace(
            "pat-na1-REPLACE-ME", "pat-na1-test"))
    (tmp_path / "signals.toml").write_text((DEFAULTS / "signals.toml").read_text())
    monkeypatch.setenv("WATCHDOG_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def client(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(cli, "make_client", lambda cfg: fake)
    return fake


def db(home):
    (home / "data").mkdir(exist_ok=True)
    conn = connect(home / "data" / "crm.db")
    migrate(conn)
    return conn


def cfg_for(home):
    return dataclasses.replace(load_config(home / "config.toml"),
                               db_path=home / "data" / "crm.db")


def set_phase(conn, phase, *api_type_names, status=None):
    for t in api_type_names:
        with conn:
            conn.execute(*_state_sql(t, {"phase": phase, "status": status}))


def complete_all(conn):
    for o in ALL_TYPES:
        set_phase(conn, "complete", *api_types(o))


# --- init ---------------------------------------------------------------------

def test_init_writes_config_from_example_without_overwriting(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("WATCHDOG_HOME", str(tmp_path))
    assert main(["init"]) == 0
    assert "[hubspot]" in (tmp_path / "config.toml").read_text()
    assert (tmp_path / "signals.toml").exists()
    (tmp_path / "config.toml").write_text("mine")
    assert main(["init"]) == 0
    assert (tmp_path / "config.toml").read_text() == "mine"
    assert "already exists" in capsys.readouterr().out


def test_init_does_not_create_a_database(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHDOG_HOME", str(tmp_path))
    main(["init"])
    assert not (tmp_path / "data").exists()


# --- usage and config errors ---------------------------------------------------

def test_backfill_is_not_reachable_without_an_explicit_subcommand(home, client, capsys):
    assert main([]) != 0
    assert "usage" in capsys.readouterr().err.lower()
    assert client.list_calls == [] and client.count_calls == []
    assert not (home / "data").exists()


@pytest.mark.parametrize("command", ["estimate", "backfill", "sync", "reconcile", "serve"])
def test_missing_config_exits_with_actionable_message(tmp_path, monkeypatch, capsys, command):
    monkeypatch.setenv("WATCHDOG_HOME", str(tmp_path))
    assert main([command]) == 2
    assert "watchdog init" in capsys.readouterr().err
    assert not (tmp_path / "data").exists()


def test_missing_signals_toml_is_actionable(home, client, capsys):
    (home / "signals.toml").unlink()
    assert main(["sync"]) == 2
    assert "signals.toml" in capsys.readouterr().err


def test_placeholder_token_is_refused(home, client, capsys):
    (home / "config.toml").write_text((DEFAULTS / "config.toml.example").read_text())
    assert main(["estimate"]) == 2
    err = capsys.readouterr().err
    assert "token" in err and "config.toml" in err


def test_bad_timezone_is_a_message_not_a_traceback(home, client, capsys):
    text = (home / "config.toml").read_text().replace(
        'timezone = "local"', 'timezone = "Mars/Base"')
    (home / "config.toml").write_text(text)
    assert main(["sync"]) == 2
    assert "Mars/Base" in capsys.readouterr().err


def test_cli_sets_watchdog_home_explicitly(home, client, monkeypatch):
    monkeypatch.delenv("WATCHDOG_HOME")
    monkeypatch.chdir(home)
    assert main(["estimate"]) == 0
    assert os.environ["WATCHDOG_HOME"] == str(home.resolve())
    monkeypatch.delenv("WATCHDOG_HOME")


def test_home_flag_overrides_cwd(home, client, tmp_path_factory, monkeypatch):
    elsewhere = tmp_path_factory.mktemp("elsewhere")
    monkeypatch.chdir(elsewhere)
    assert main(["--home", str(home), "estimate"]) == 0
    assert os.environ["WATCHDOG_HOME"] == str(home.resolve())


def test_rejected_token_is_a_message_not_a_traceback(home, client, capsys):
    def boom(_type):
        req = httpx.Request("POST", "https://api.hubapi.com/x")
        raise httpx.HTTPStatusError("401", request=req,
                                    response=httpx.Response(401, request=req))
    client.count = boom
    assert main(["estimate"]) == 1
    err = capsys.readouterr().err
    assert "401" in err and "token" in err.lower()
    assert "Traceback" not in err


# --- estimate ------------------------------------------------------------------

def test_estimate_prints_per_type_counts_and_makes_no_writes(home, client, capsys):
    client.counts = {"companies": 1234, "deals": 10_000, "contacts": 5,
                     "calls": 100, "notes": 100}
    assert main(["estimate"]) == 0
    out = capsys.readouterr().out
    assert "~1,234" in out
    assert "10,000+" in out and "at least" in out.lower()
    assert "owners" in out
    assert "approximate" in out.lower()
    assert not (home / "data").exists()          # nothing created at all


def test_estimate_leaves_an_existing_database_byte_identical(home, client):
    db(home).close()
    folder = home / "data"
    before = {p.name: p.read_bytes() for p in folder.iterdir()}
    assert main(["estimate"]) == 0
    assert {p.name: p.read_bytes() for p in folder.iterdir()} == before


# --- backfill ------------------------------------------------------------------

def test_backfill_completes_and_says_so(home, client, capsys):
    assert main(["backfill"]) == 0
    assert "backfill complete" in capsys.readouterr().out.lower()
    states = backfill_state(db(home))
    assert all(phase == "complete" for phase, _ in states.values())


def test_backfill_refuses_to_call_a_blocked_type_complete(home, client, capsys):
    real = client.list_page

    def list_page(object_type, after, archived=False):
        if object_type == "deals":
            req = httpx.Request("GET", "https://api.hubapi.com/crm/v3/objects/deals")
            raise httpx.HTTPStatusError("400 bad property", request=req,
                                        response=httpx.Response(400, request=req))
        return real(object_type, after, archived)

    client.list_page = list_page
    assert main(["backfill"]) == 1
    cap = capsys.readouterr()
    text = cap.out + cap.err
    assert "BLOCKED deals" in text and "400 bad property" in text
    assert "backfill complete" not in text.lower()
    # and the block is durable: a re-run is still refused, not "complete"
    assert main(["backfill"]) == 1


def test_backfill_rate_limit_keeps_checkpoint_and_says_rerun(home, client, capsys):
    client.fail_on_page = 1
    client.set_pages("companies", 3)
    assert main(["backfill"]) == 1
    assert "re-run" in capsys.readouterr().err.lower()


def test_backfill_interrupt_is_safe_and_resumable(home, client, capsys, monkeypatch):
    def interrupt(*a, **k):
        raise KeyboardInterrupt
    monkeypatch.setattr(cli, "backfill", interrupt)
    assert main(["backfill"]) == 130
    assert "resume" in capsys.readouterr().err.lower()


# --- sync ----------------------------------------------------------------------

def test_sync_runs_incremental_then_evaluates_signals(home, client):
    conn = db(home)
    complete_all(conn)
    add_stage(conn)
    add_deal(conn, "d1", activity=1_000)         # quiet since 1970
    conn.close()
    assert main(["sync"]) == 0
    assert db(home).execute("SELECT COUNT(*) FROM signal_results").fetchone()[0] > 0


def test_sync_refuses_when_nothing_has_been_backfilled(home, client, capsys):
    assert main(["sync"]) == 2
    assert "watchdog backfill" in capsys.readouterr().err
    assert client.search_calls == []


def test_sync_surfaces_a_blocked_type(home, client, capsys):
    conn = db(home)
    complete_all(conn)
    set_phase(conn, "blocked", "contacts", status="blocked: 400 nope")
    conn.close()
    assert main(["sync"]) == 1
    cap = capsys.readouterr()
    assert "contacts" in cap.out + cap.err and "400 nope" in cap.out + cap.err


# --- reconcile -----------------------------------------------------------------

def test_reconcile_archives_only_for_complete_types_and_reports_skips(home, client, capsys):
    conn = db(home)
    set_phase(conn, "complete", "companies")
    upsert(conn, "companies", [{"id": "c1", "archived": 0}, {"id": "c2", "archived": 0}])
    conn.close()
    client.remote_ids["companies"] = ["c1"]
    client.counts["companies"] = 1
    assert main(["reconcile"]) == 1              # other types skipped: attention
    out = capsys.readouterr().out
    assert "companies: 1 newly archived" in out
    assert "SKIPPED" in out and "deals" in out
    assert {c.object_type for c in client.list_calls} == {"companies"}
    assert all(c.archived for c in client.list_calls)
    archived = db(home).execute("SELECT archived FROM companies WHERE id='c2'").fetchone()[0]
    assert archived == 1


def test_reconcile_with_no_complete_type_does_not_run_the_archived_pass(home, client, capsys):
    assert main(["reconcile"]) == 1
    assert client.list_calls == []               # [] must not mean "all types"
    assert "SKIPPED" in capsys.readouterr().out


def test_reconcile_zero_is_not_a_skip(home, client, capsys):
    conn = db(home)
    complete_all(conn)
    conn.close()
    assert main(["reconcile"]) == 0
    out = capsys.readouterr().out
    assert "companies: 0 newly archived" in out and "SKIPPED" not in out


# --- serve and the timer ---------------------------------------------------------

class _Quiet:
    def start(self): pass
    def stop(self): pass


def test_serve_starts_timer_at_configured_interval_and_binds_localhost(home, client, monkeypatch):
    started, run = [], {}

    class Recorder:
        def __init__(self, interval_s, job, **kw):
            started.append(interval_s)
        def start(self): run["started"] = True
        def stop(self): run["stopped"] = True

    monkeypatch.setattr(cli, "SyncTimer", Recorder)
    monkeypatch.setattr(cli.uvicorn, "run", lambda app, **kw: run.update(kw))
    assert main(["serve"]) == 0
    assert started == [15 * 60]
    assert run["host"] == "127.0.0.1"
    assert run["started"] and run["stopped"]


def test_serve_warns_when_data_is_not_backfilled(home, client, monkeypatch, capsys):
    monkeypatch.setattr(cli.uvicorn, "run", lambda app, **kw: None)
    monkeypatch.setattr(cli, "SyncTimer", lambda *a, **k: _Quiet())
    assert main(["serve"]) == 0
    assert "backfill" in capsys.readouterr().err


class FakeTimer:
    armed: list = []

    def __init__(self, interval, fn):
        FakeTimer.armed.append(interval)
        self.daemon = False

    def start(self): pass
    def cancel(self): pass


def test_timer_job_survives_failure_and_rearms(caplog):
    FakeTimer.armed = []
    calls = []

    def job():
        calls.append(1)
        raise RuntimeError("hubspot down")

    timer = SyncTimer(900, job, timer_factory=FakeTimer)
    timer.start()
    with caplog.at_level(logging.ERROR, logger="watchdog"):
        timer._fire()
        timer._fire()
    assert len(calls) == 2
    assert FakeTimer.armed == [900, 900, 900]    # start, then a re-arm per run
    assert "hubspot down" in caplog.text


def test_timer_does_not_rearm_after_stop():
    FakeTimer.armed = []
    timer = SyncTimer(60, lambda: None, timer_factory=FakeTimer)
    timer.start()
    timer.stop()
    timer._fire()
    assert FakeTimer.armed == [60]


def test_scheduled_job_skips_when_never_backfilled(home, client, caplog):
    with caplog.at_level(logging.WARNING, logger="watchdog"):
        cli.scheduled_sync(cfg_for(home))
    assert client.search_calls == []
    assert "backfill" in caplog.text


def test_scheduled_job_logs_report_errors(home, client, caplog):
    conn = db(home)
    complete_all(conn)
    set_phase(conn, "blocked", "contacts", status="blocked: 400 nope")
    conn.close()
    with caplog.at_level(logging.WARNING, logger="watchdog"):
        cli.scheduled_sync(cfg_for(home))
    assert "contacts" in caplog.text


def test_scheduled_job_lets_a_rate_limit_reach_the_timer(home, client):
    conn = db(home)
    complete_all(conn)
    conn.close()
    client.fail_on_page = 1
    client.set_pages("companies", 3)
    with pytest.raises(RateLimited):
        cli.scheduled_sync(cfg_for(home))
