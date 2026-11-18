from crm.snooze import (SNOOZE_FIELDS, active_snoozes, drop_stale_snoozes,
                        fingerprint, snooze)
from crm.sync import sync_incremental
from tests.conftest import DAY, NOW, add_company, add_deal, add_stage, flagged_in_briefing

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
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    assert "d1" not in flagged_in_briefing(conn, NOW, signal="stalled_deal")
    assert flagged_in_briefing(conn, NOW, signal="big_and_quiet") == before


def test_snooze_dropped_when_value_stage_or_owner_changes(conn):
    for col, new in [("amount_in_home_currency", 99999.0),
                     ("dealstage", "s_other"), ("hubspot_owner_id", "o2")]:
        stalled(conn)
        snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
        update_deal(conn, **{col: new})
        assert drop_stale_snoozes(conn, "deals", ["d1"]) == 1, col
        assert active_snoozes(conn, NOW) == set()


def test_snooze_survives_an_unrelated_field_change(conn):
    stalled(conn)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    update_deal(conn, dealname="renamed", hs_last_activity_date=NOW)
    assert drop_stale_snoozes(conn, "deals", ["d1"]) == 0
    assert active_snoozes(conn, NOW) == {("stalled_deal", "deals", "d1")}


def test_noop_resync_keeps_snooze_and_null_fields_are_stable(conn):
    add_stage(conn)
    add_deal(conn, "d1", amount=None, owner=None, activity=NOW - 20 * DAY)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    assert fingerprint(conn, "deals", "d1") == fingerprint(conn, "deals", "d1")
    assert drop_stale_snoozes(conn, "deals", ["d1"]) == 0


def test_drop_only_affects_listed_ids_and_matching_record_type(conn):
    stalled(conn)
    add_deal(conn, "d2", activity=NOW - 20 * DAY)
    add_company(conn, "d1")
    for sid, rt, rid in [("stalled_deal", "deals", "d1"),
                         ("stalled_deal", "deals", "d2"),
                         ("unowned_record", "companies", "d1")]:
        snooze(conn, sid, rt, rid, NOW + WEEK_MS, NOW)
    with conn:
        conn.execute("UPDATE deals SET amount_in_home_currency = 1.0")
        conn.execute("UPDATE companies SET hubspot_owner_id = 'o9'")
    assert drop_stale_snoozes(conn, "deals", ["d1"]) == 1
    assert active_snoozes(conn, NOW) == {("stalled_deal", "deals", "d2"),
                                         ("unowned_record", "companies", "d1")}


def test_resnooze_replaces_and_refreshes_fingerprint(conn):
    stalled(conn)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + DAY, NOW)
    update_deal(conn, dealstage="s_other")
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    assert drop_stale_snoozes(conn, "deals", ["d1"]) == 0
    assert tuple(conn.execute("SELECT count(*), max(until) FROM snoozes")
                 .fetchone()) == (1, NOW + WEEK_MS)


def test_material_fields_declared_for_deals():
    assert SNOOZE_FIELDS["deals"] == (
        "amount_in_home_currency", "dealstage", "hubspot_owner_id")


def test_sync_incremental_drops_snooze_on_material_change(conn, fake_client,
                                                          monkeypatch):
    import crm.sync as sync
    stalled(conn)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    monkeypatch.setattr(sync, "_evaluate_signals", lambda *a, **k: None)
    monkeypatch.setattr(sync, "_refresh_reference_data", lambda *a, **k: None)
    monkeypatch.setattr(sync, "refresh_associations", lambda *a, **k: None)

    def fake_one(conn_, client, api_type):
        update_deal(conn_, amount_in_home_currency=12345.0)
        return 1, ["d1"], 0
    monkeypatch.setattr(sync, "_sync_one_type", fake_one)
    sync_incremental(conn, fake_client, ["deals"])
    assert active_snoozes(conn, NOW) == set()


def test_snoozing_makes_no_hubspot_calls(conn, fake_client):
    stalled(conn)
    snooze(conn, "stalled_deal", "deals", "d1", NOW + WEEK_MS, NOW)
    c = fake_client  # the fake has no write methods; no read call either
    assert not (c.search_calls or c.list_calls or c.ids_only_calls
                or c.assoc_calls or c.count_calls or c.pipeline_calls)
