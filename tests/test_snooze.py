from crm.snooze import (SNOOZE_FIELDS, active_snoozes, drop_stale_snoozes,
                        fingerprint, snooze)
import crm.snooze as snooze_module
import pytest
from pathlib import Path
from crm.db import upsert
from crm.signals import flagged_records
from crm.sync import backfill, normalise_record, sync_incremental
from tests.conftest import (DAY, NOW, add_company, add_deal, add_stage,
                            flagged_in_briefing)
from tests.fakes import FakePage
from tests.test_sync_incremental import deal

WEEK_MS = 7 * DAY


def stalled(conn, deal="d1"):
    add_stage(conn)
    add_deal(conn, deal, activity=NOW - 20 * DAY)


def update_deal(conn, **cols):
    sets = ", ".join(f"{c} = ?" for c in cols)
    with conn:
        conn.execute(f"UPDATE deals SET {sets} WHERE id = 'd1'", list(cols.values()))


def test_snoozed_record_hidden_from_briefing(conn):
    stalled(conn)
    assert "d1" in flagged_in_briefing(conn, NOW)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    assert "d1" not in flagged_in_briefing(conn, NOW)


def test_expired_snooze_reappears(conn):
    stalled(conn)
    snooze(conn, "stalled_deal", "deals", "d1", NOW - 1, NOW)
    assert "d1" in flagged_in_briefing(conn, NOW)


def test_snooze_is_per_signal_not_global(conn):
    add_stage(conn)
    add_deal(conn, "d1", activity=NOW - 20 * DAY, amount=900000.0)
    before = flagged_in_briefing(conn, NOW, signal="big_and_quiet")
    assert "d1" in before
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    assert "d1" not in flagged_in_briefing(conn, NOW, signal="stalled_deal")
    assert flagged_in_briefing(conn, NOW, signal="big_and_quiet") == before


def test_snooze_dropped_when_value_stage_or_owner_changes(conn):
    for col, new in [("amount_in_home_currency", 99999.0),
                     ("dealstage", "s_other"), ("hubspot_owner_id", "o2")]:
        stalled(conn)
        snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
        update_deal(conn, **{col: new})
        assert drop_stale_snoozes(conn) == 1, col
        assert active_snoozes(conn, NOW) == set()


def test_snooze_survives_an_unrelated_field_change(conn):
    stalled(conn)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    update_deal(conn, dealname="renamed", hs_last_activity_date=NOW)
    assert drop_stale_snoozes(conn) == 0
    assert active_snoozes(conn, NOW) == {("stalled_deal", "deals", "d1")}


def test_only_the_changed_record_loses_its_snooze(conn):
    stalled(conn)
    add_deal(conn, "d2", activity=NOW - 20 * DAY)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    snooze(conn, "stalled_deal", "deals", "d2", NOW + WEEK_MS, NOW)
    update_deal(conn, amount_in_home_currency=1.0)
    assert drop_stale_snoozes(conn) == 1
    assert active_snoozes(conn, NOW) == {("stalled_deal", "deals", "d2")}


def test_snoozing_a_company_leaves_the_deal_with_the_same_id_visible(conn):
    add_stage(conn)
    add_deal(conn, "777", amount=4000, owner=None)
    add_company(conn, "777", owner=None, open_deals=["777"])
    snooze(conn, "unowned_record", "companies", "777", NOW + WEEK_MS, NOW)
    flagged_in_briefing(conn, NOW)  # evaluates
    shown = {(r["record_type"], r["record_id"])
             for r in flagged_records(conn, NOW, "unowned_record")}
    assert ("deals", "777") in shown
    assert ("companies", "777") not in shown


def test_null_fingerprint_snooze_is_kept(conn):
    stalled(conn)
    with conn:
        conn.execute("INSERT INTO snoozes (signal_id, record_type, record_id,"
                     " until, created_at) VALUES ('stalled_deal', 'deals', 'd1',"
                     " ?, ?)", (NOW + WEEK_MS, NOW))
    update_deal(conn, dealstage="s_other")
    assert drop_stale_snoozes(conn) == 0


def test_resnooze_replaces_and_refreshes_fingerprint(conn):
    stalled(conn)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + DAY, NOW)
    update_deal(conn, dealstage="s_other")
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    assert drop_stale_snoozes(conn) == 0
    assert tuple(conn.execute("SELECT count(*), max(until) FROM snoozes")
                 .fetchone()) == (1, NOW + WEEK_MS)


def test_material_fields_declared_for_deals():
    assert SNOOZE_FIELDS["deals"] == (
        "amount_in_home_currency", "dealstage", "hubspot_owner_id")


@pytest.mark.parametrize("signal_id, record_type", [
    ("stalled_deal", "deal"), ("no_such_signal", "deals")])
def test_snooze_rejects_unknown_signal_or_record_type(conn, signal_id,
                                                      record_type):
    with pytest.raises(ValueError):
        snooze(conn, signal_id, record_type, "d1", NOW + WEEK_MS, NOW)
    assert conn.execute("SELECT count(*) FROM snoozes").fetchone()[0] == 0


# --- the no-op resync claim: real normalise + upsert, twice ---------------------

@pytest.mark.parametrize("props", [
    {},
    {"amount_in_home_currency": None, "hubspot_owner_id": None,
     "dealstage": None},
    {"amount_in_home_currency": "", "hubspot_owner_id": None},
    {"amount_in_home_currency": "900000"},
])
def test_resyncing_identical_payload_keeps_the_snooze(conn, props):
    record = deal("4001", "2026-11-10T14:02:55.310Z", **props)
    upsert(conn, "deals", [normalise_record("deals", record)])
    snooze(conn, "stalled_deal", "deals", "4001", NOW + WEEK_MS, NOW)
    upsert(conn, "deals", [normalise_record("deals", record)])
    assert drop_stale_snoozes(conn) == 0
    assert active_snoozes(conn, NOW) == {("stalled_deal", "deals", "4001")}


def test_numeric_representation_drift_does_not_drop_the_snooze(conn):
    first = deal("4001", "2026-11-10T14:02:55.310Z",
                 amount_in_home_currency="900000")
    again = deal("4001", "2026-11-10T14:02:55.310Z",
                 amount_in_home_currency=900000.0)
    upsert(conn, "deals", [normalise_record("deals", first)])
    snooze(conn, "stalled_deal", "deals", "4001", NOW + WEEK_MS, NOW)
    upsert(conn, "deals", [normalise_record("deals", again)])
    assert drop_stale_snoozes(conn) == 0


# --- wiring: every write path reaches the check via _evaluate_signals ----------

def test_sync_incremental_noop_keeps_and_change_drops(conn, fake_client):
    def pages(**props):
        fake_client.set_pages("deals", [FakePage(records=[
            deal("4001", "2026-11-10T14:02:55.310Z", **props)])])
    pages()
    sync_incremental(conn, fake_client, ["deals"])
    snooze(conn, "stalled_deal", "deals", "4001", NOW + WEEK_MS, NOW)
    pages()
    sync_incremental(conn, fake_client, ["deals"])
    assert active_snoozes(conn, NOW) == {("stalled_deal", "deals", "4001")}
    pages(amount_in_home_currency="99999")
    sync_incremental(conn, fake_client, ["deals"])
    assert active_snoozes(conn, NOW) == set()


def test_backfill_drops_snooze_on_material_change(conn, fake_client):
    upsert(conn, "deals", [normalise_record(
        "deals", deal("4001", "2026-11-10T14:02:55.310Z"))])
    snooze(conn, "stalled_deal", "deals", "4001", NOW + WEEK_MS, NOW)
    fake_client.set_pages("deals", [FakePage(records=[deal(
        "4001", "2026-11-10T14:02:55.310Z", hubspot_owner_id="999")])])
    backfill(conn, fake_client)
    assert active_snoozes(conn, NOW) == set()


def test_snooze_module_has_no_hubspot_dependency():
    source = Path(snooze_module.__file__).read_text()
    assert "httpx" not in source and "crm.hubspot" not in source
