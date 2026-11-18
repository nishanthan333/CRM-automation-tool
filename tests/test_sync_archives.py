import pytest

from crm.db import upsert
from crm.hubspot import RateLimited
from crm.sync import _state_sql, reconcile, sync_archived, sync_incremental


def set_phase(conn, phase, *api_types):
    for t in api_types:
        with conn:
            conn.execute(*_state_sql(t, {"phase": phase}))


def complete(conn, *api_types):
    set_phase(conn, "complete", *api_types)


def flags(conn, table="deals"):
    return {r[0]: r[1] for r in conn.execute(f"select id, archived from {table}")}


def test_sync_archived_sets_flag_on_existing_records(conn, fake_client):
    upsert(conn, "deals", [{"id": "4001", "archived": 0}])
    fake_client.archived["deals"] = [{"id": "4001"}]
    sync_archived(conn, fake_client, ["deals"])
    assert conn.execute("select archived from deals").fetchone()[0] == 1


def test_sync_archived_requests_archived_true(conn, fake_client):
    sync_archived(conn, fake_client, ["deals"])
    assert fake_client.list_calls[0].archived is True


def test_sync_archived_walks_every_page(conn, fake_client):
    fake_client.archived["deals"] = [[{"id": "1"}], [{"id": "2"}]]
    report = sync_archived(conn, fake_client, ["deals"])
    assert flags(conn) == {"1": 1, "2": 1} and report.per_type["deals"] == 2


def test_sync_archived_fans_out_engagements(conn, fake_client):
    fake_client.archived["calls"] = [{"id": "c1"}]
    fake_client.archived["notes"] = [{"id": "n1"}]
    sync_archived(conn, fake_client, ["engagements"])
    assert {c.object_type for c in fake_client.list_calls} == {
        "calls", "meetings", "emails", "notes", "tasks"}
    assert flags(conn, "engagements") == {"c1": 1, "n1": 1}


def test_reconcile_archives_locals_absent_from_remote_id_set(conn, fake_client):
    upsert(conn, "deals", [{"id": "1"}, {"id": "2"}])
    complete(conn, "deals")
    # "unsynced" is live in HubSpot but not local yet, so the walk is not short
    # against the local live count; "2" is genuinely gone.
    fake_client.remote_ids["deals"] = ["1", "unsynced"]
    assert reconcile(conn, fake_client)["deals"] == 1
    assert flags(conn) == {"1": 0, "2": 1}


def test_reconcile_unarchives_a_record_hubspot_lists_as_live_again(conn, fake_client):
    upsert(conn, "deals", [{"id": "1", "archived": 1}, {"id": "2", "archived": 1}])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = ["1"]     # restored in HubSpot
    reconcile(conn, fake_client)
    # "2" is in neither the live listing nor the local live set: left alone.
    assert flags(conn) == {"1": 0, "2": 1}


def test_reconcile_unarchive_is_scoped_per_engagement_type(conn, fake_client):
    upsert(conn, "engagements", [
        {"id": "1", "type": "calls", "archived": 1},
        {"id": "2", "type": "notes", "archived": 1}])
    complete(conn, "calls", "meetings", "emails", "notes", "tasks")
    fake_client.remote_ids["calls"] = ["1"]
    fake_client.remote_ids["notes"] = ["9"]     # a note id, not engagement "2"
    reconcile(conn, fake_client)
    assert flags(conn, "engagements") == {"1": 0, "2": 1}


def test_reconcile_is_idempotent_in_both_directions(conn, fake_client):
    upsert(conn, "deals", [{"id": "1"}, {"id": "2"}])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = ["1", "unsynced"]
    assert reconcile(conn, fake_client)["deals"] == 1       # "2" archived
    fake_client.remote_ids["deals"] = ["1", "2", "unsynced"]
    result = reconcile(conn, fake_client)                   # "2" is back
    assert result["deals"] == 0 and result.unarchived["deals"] == 1
    assert flags(conn) == {"1": 0, "2": 0}


def test_reconcile_baseline_falls_back_to_the_local_live_count(conn, fake_client):
    # HubSpot's search total is capped, so it can report far fewer records than
    # exist. A walk short against the *local* live rows is still truncated.
    upsert(conn, "deals", [{"id": str(i)} for i in range(1, 101)])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = ["1", "2"]
    fake_client.counts["deals"] = 2            # all the capped total can say
    result = reconcile(conn, fake_client)
    assert "deals" not in result and "truncated" in result.skipped["deals"]
    assert set(flags(conn).values()) == {0}


def test_reconcile_never_deletes_rows(conn, fake_client):
    upsert(conn, "deals", [{"id": "1"}, {"id": "2"}])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = ["1", "unsynced"]
    reconcile(conn, fake_client)
    assert conn.execute("select count(*) from deals").fetchone()[0] == 2


def test_reconcile_uses_id_only_endpoint(conn, fake_client):
    complete(conn, "deals")
    reconcile(conn, fake_client)
    assert fake_client.ids_only_calls and fake_client.list_calls == []


def test_reconcile_unions_all_id_pages(conn, fake_client):
    upsert(conn, "deals", [{"id": str(i)} for i in range(1, 6)])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = ["1", "2", "3", "4", "5"]
    fake_client.ids_page_size = 2
    assert reconcile(conn, fake_client)["deals"] == 0
    assert set(flags(conn).values()) == {0}


def test_reconcile_failed_id_walk_archives_nothing(conn, fake_client):
    upsert(conn, "deals", [{"id": str(i)} for i in range(1, 6)])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = ["1", "2", "3", "4", "5"]
    fake_client.ids_page_size = 1
    fake_client.fail_on_page = 3   # pages 1-2 arrive, so 3-5 look "absent"
    with pytest.raises(RateLimited):
        reconcile(conn, fake_client)
    assert set(flags(conn).values()) == {0}


def test_reconcile_skips_types_without_a_complete_backfill(conn, fake_client):
    upsert(conn, "deals", [{"id": "1"}])
    upsert(conn, "companies", [{"id": "9"}])
    complete(conn, "companies")
    set_phase(conn, "backfill", "deals")
    result = reconcile(conn, fake_client)
    assert "deals" not in result and "deals" in result.skipped
    assert flags(conn) == {"1": 0}
    assert all(c.object_type != "deals" for c in fake_client.ids_only_calls)


def test_reconcile_empty_remote_set_is_not_trusted(conn, fake_client):
    upsert(conn, "deals", [{"id": "1"}])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = []
    assert "deals" not in reconcile(conn, fake_client)
    assert flags(conn) == {"1": 0}


def test_reconcile_scopes_engagement_ids_per_api_type(conn, fake_client):
    upsert(conn, "engagements", [
        {"id": "1", "type": "calls"}, {"id": "2", "type": "notes"},
        {"id": "3", "type": "notes"}])
    complete(conn, "calls", "meetings", "emails", "notes", "tasks")
    fake_client.remote_ids["calls"] = ["1"]
    fake_client.remote_ids["notes"] = ["2", "unsynced"]
    assert reconcile(conn, fake_client)["engagements"] == 1
    assert flags(conn, "engagements") == {"1": 0, "2": 0, "3": 1}


def test_incremental_run_does_not_run_the_archived_pass(conn, fake_client):
    sync_incremental(conn, fake_client, ["deals"])
    assert not any(c.archived for c in fake_client.list_calls)


def test_sync_archived_rate_limit_is_confined_to_its_type(conn, fake_client):
    fake_client.archived["deals"] = [{"id": "1"}]
    real = fake_client.list_page

    def flaky(object_type, after, archived=False):
        if object_type == "companies":
            raise RateLimited("fake 429")
        return real(object_type, after, archived)

    fake_client.list_page = flaky
    report = sync_archived(conn, fake_client, ["companies", "deals"])
    assert any(e.startswith("companies archived") for e in report.errors)
    assert flags(conn) == {"1": 1}


def test_reconcile_ignores_rows_synced_during_the_walk(conn, fake_client):
    upsert(conn, "deals", [{"id": "1"}, {"id": "2"}])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = ["1", "2"]
    fake_client.ids_page_size = 1
    real = fake_client.list_ids_page

    def create_mid_walk(object_type, after):
        if after is not None:   # second page: incremental syncs a new deal
            upsert(conn, "deals", [{"id": "NEW"}])
        return real(object_type, after)

    fake_client.list_ids_page = create_mid_walk
    assert reconcile(conn, fake_client)["deals"] == 0
    assert flags(conn)["NEW"] == 0


def test_reconcile_refuses_a_truncated_walk(conn, fake_client):
    upsert(conn, "deals", [{"id": str(i)} for i in range(1, 6)])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = ["1", "2"]   # short, but non-empty
    fake_client.counts["deals"] = 5
    result = reconcile(conn, fake_client)
    assert "deals" not in result and "truncated" in result.skipped["deals"]
    assert set(flags(conn).values()) == {0}


def test_reconcile_reports_skips_distinguishably(conn, fake_client):
    upsert(conn, "deals", [{"id": "1"}])
    complete(conn, "deals")
    fake_client.remote_ids["deals"] = []
    result = reconcile(conn, fake_client)
    assert "deals" not in result
    assert "deals" in result.skipped and "companies" in result.skipped
    assert "backfill not complete" in result.skipped["companies"]


def test_reconcile_engagements_all_or_nothing(conn, fake_client):
    upsert(conn, "engagements", [
        {"id": "1", "type": "calls"}, {"id": "2", "type": "notes"}])
    complete(conn, "calls", "meetings", "emails", "notes", "tasks")
    fake_client.remote_ids["calls"] = []          # calls guarded
    fake_client.remote_ids["notes"] = ["9"]       # notes would archive "2"
    result = reconcile(conn, fake_client)
    assert "engagements" in result.skipped
    assert flags(conn, "engagements") == {"1": 0, "2": 0}


def test_weekly_pass_runs_archived_only_for_complete_types(conn, fake_client):
    from crm.sync import weekly_pass
    complete(conn, "companies")
    upsert(conn, "companies", [{"id": "c1", "archived": 0}, {"id": "c2", "archived": 0}])
    fake_client.archived["companies"] = [{"id": "c9"}]
    fake_client.remote_ids["companies"] = ["c1", "unsynced"]
    fake_client.counts["companies"] = 1
    result = weekly_pass(conn, fake_client)
    assert {c.object_type for c in fake_client.list_calls} == {"companies"}
    assert result.archived_found == {"companies": 1}
    assert result.reconciled["companies"] == 1
    assert "deals" in result.reconciled.skipped


def test_weekly_pass_with_nothing_complete_skips_the_archived_pass(conn, fake_client):
    from crm.sync import weekly_pass
    result = weekly_pass(conn, fake_client)
    assert fake_client.list_calls == []
    assert result.archived_found == {} and len(result.reconciled.skipped) == 4


# --- the weekly pass leaves a mark -------------------------------------------

def weekly_marker(conn):
    from crm.sync import weekly_checked_at
    return weekly_checked_at(conn)


def test_weekly_pass_stamps_when_archives_were_last_checked(conn, fake_client):
    from crm.sync import WEEKLY_STATE, weekly_pass
    assert weekly_marker(conn) is None
    complete(conn, "companies")
    weekly_pass(conn, fake_client)
    assert weekly_marker(conn) is not None
    row = conn.execute("SELECT status FROM sync_state WHERE object_type = ?",
                       (WEEKLY_STATE,)).fetchone()
    assert row["status"]        # says what the pass did, skips included


def test_weekly_pass_is_due_when_never_run_or_older_than_a_week(conn, fake_client):
    from crm.sync import WEEKLY_INTERVAL_MS, weekly_due, weekly_pass
    assert weekly_due(conn, 1_800_000_000_000) is True       # never run
    weekly_pass(conn, fake_client)
    stamped = weekly_marker(conn)
    assert weekly_due(conn, stamped) is False
    assert weekly_due(conn, stamped + WEEKLY_INTERVAL_MS + 1) is True


def test_the_weekly_marker_is_not_mistaken_for_an_object_type(conn):
    from crm.sync import INCREMENTAL_TYPES, WEEKLY_STATE
    assert WEEKLY_STATE not in INCREMENTAL_TYPES
