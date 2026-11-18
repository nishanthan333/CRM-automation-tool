import tomllib
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from crm.db import connect, migrate
from tests.fakes import FakeClient


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "watchdog.db")
    migrate(connection)
    yield connection
    connection.close()


@pytest.fixture
def fake_client():
    return FakeClient()


# --- signal helpers (shared with Task 7's briefing/snooze tests) ---------------
# Every helper takes `now_ms`; nothing here, or in crm.signals, reads the clock.

DAY = 86_400_000
UTC = timezone.utc
# Wednesday 2026-11-18 12:00 UTC.
NOW = int(datetime(2026, 11, 18, 12, 0, tzinfo=UTC).timestamp() * 1000)
DEFAULT_THRESHOLDS = tomllib.loads(
    (Path(__file__).parent.parent / "crm" / "defaults" / "signals.toml").read_text())

_UNSET = object()


def days_ago(n: float, now_ms: int = NOW) -> int:
    return int(now_ms - n * DAY)


def add_stage(conn, stage_id="s_open", is_closed=0, label=None, pipeline="p1"):
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO stages (id, pipeline_id, label, display_order,"
            " is_closed, probability) VALUES (?, ?, ?, 0, ?, 0.5)",
            (stage_id, pipeline, label or stage_id, is_closed))


def add_owner(conn, owner_id, first="Rhea", last="Mehta", email=None):
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO owners (id, email, first_name, last_name)"
            " VALUES (?, ?, ?, ?)", (owner_id, email, first, last))


def add_deal(conn, deal_id, stage="s_open", amount=_UNSET, owner="o1",
             activity=_UNSET, closedate=_UNSET, createdate=_UNSET, archived=0):
    """Seed a deal. Defaults: open, 50k, owned, active 3 days ago, closing in 30d.

    Pass None explicitly to store SQL NULL.
    """
    row = {
        "id": deal_id, "dealstage": stage, "pipeline": "p1", "archived": archived,
        "amount_in_home_currency": 50000.0 if amount is _UNSET else amount,
        "hubspot_owner_id": owner,
        "hs_last_activity_date": days_ago(3) if activity is _UNSET else activity,
        "closedate": NOW + 30 * DAY if closedate is _UNSET else closedate,
        "createdate": days_ago(90) if createdate is _UNSET else createdate,
    }
    cols = ", ".join(row)
    marks = ", ".join(f":{c}" for c in row)
    with conn:
        conn.execute(f"INSERT OR REPLACE INTO deals ({cols}) VALUES ({marks})", row)


def add_company(conn, company_id, owner="o1", activity=_UNSET, archived=0,
                open_deals=()):
    """Seed a company, linked (deals -> companies, the stored direction)."""
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO companies (id, hubspot_owner_id,"
            " hs_last_activity_date, archived) VALUES (?, ?, ?, ?)",
            (company_id, owner, days_ago(3) if activity is _UNSET else activity,
             archived))
        for deal_id in open_deals:
            conn.execute(
                "INSERT OR REPLACE INTO associations VALUES"
                " ('deals', ?, 'companies', ?)", (deal_id, company_id))


def _evaluated(conn, thresholds, now_ms):
    from crm.signals import evaluate_all
    evaluate_all(conn, thresholds or DEFAULT_THRESHOLDS, now_ms, tz=UTC)


def flagged(conn, signal_id, thresholds=None, now_ms=NOW) -> set[str]:
    """Evaluate, then return the record ids a signal flagged."""
    _evaluated(conn, thresholds, now_ms)
    return {r["record_id"] for r in conn.execute(
        "SELECT record_id FROM signal_results WHERE signal_id = ?", (signal_id,))}


def flagged_in_briefing(conn, now_ms=NOW, signal=None, thresholds=None) -> set[str]:
    """Record ids the briefing would show, i.e. after snoozes are applied.

    Re-runs evaluate_all first (idempotent), so it is not a pure read.
    """
    from crm.signals import flagged_records
    _evaluated(conn, thresholds, now_ms)
    return {r["record_id"] for r in flagged_records(conn, now_ms, signal)}


def summary(conn, signal_id, now_ms=NOW, thresholds=None):
    """The briefing's SignalSummary for one signal, plus `.record_ids`.

    Re-runs evaluate_all first, like flagged_in_briefing.
    SignalSummary has no record_ids field; this view adds it, read from
    signal_results through the same snooze filter the briefing uses.
    """
    from crm.signals import SIGNALS, briefing
    ids = frozenset(flagged_in_briefing(conn, now_ms, signal_id, thresholds))
    for s in briefing(conn, now_ms):
        if s.signal_id == signal_id:
            return SimpleNamespace(**vars(s), record_ids=ids)
    sig = next(s for s in SIGNALS if s.id == signal_id)
    return SimpleNamespace(signal_id=sig.id, title=sig.title,
                           severity=sig.severity, count=0, total_value=0.0,
                           by_owner=[], record_ids=ids)


@pytest.fixture(autouse=True)
def _watchdog_home(monkeypatch):
    """Sync evaluates signals from $WATCHDOG_HOME/signals.toml; the shipped
    defaults live in crm/defaults. Tests that care set their own."""
    monkeypatch.setenv(
        "WATCHDOG_HOME",
        str(Path(__file__).parent.parent / "crm" / "defaults"))
