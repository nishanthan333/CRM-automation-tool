import sqlite3

import pytest

from crm.hubspot import ENGAGEMENT_TYPES, RateLimited
from crm.sync import (
    WATERMARK_REWIND_MS,
    SyncReport,
    cursor,
    normalise_record,
    refresh_pipelines,
    set_watermark,
    sync_incremental,
    to_epoch_ms,
    watermark,
)
from tests.fakes import FakePage

START = 1_700_000_000_000
MOD_1 = 1_794_319_375_310  # 2026-11-10T14:02:55.310Z
MOD_2 = 1_794_476_710_101  # 2026-11-12T09:45:10.101Z


def deal(deal_id: str, modified: str, **props) -> dict:
    properties = {
        "dealname": f"Deal {deal_id}",
        "dealstage": "contractsent",
        "pipeline": "default",
        "amount_in_home_currency": "18500.00",
        "closedate": "2026-12-15T00:00:00.000Z",
        "createdate": "2026-09-02T10:14:31.822Z",
        "hubspot_owner_id": "81234567",
        "hs_last_activity_date": "2026-11-09T16:30:00Z",
        "hs_lastmodifieddate": modified,
    }
    properties.update(props)
    return {"id": deal_id, "properties": properties, "archived": False}


def two_deal_pages(fake_client) -> None:
    fake_client.set_pages("deals", [
        FakePage(records=[deal("4001", "2026-11-10T14:02:55.310Z")],
                 after="page-2-cursor"),
        FakePage(records=[deal("4002", "2026-11-12T09:45:10.101Z")]),
    ])


def assoc_rows(conn, to_type: str) -> list[tuple]:
    return [tuple(r) for r in conn.execute(
        "select from_type, from_id, to_type, to_id from associations "
        "where to_type = ? order by from_id, to_id", (to_type,))]


# --- timestamp conversion (HubSpot sends ISO-8601; our columns are epoch ms) ---

def test_to_epoch_ms_parses_iso_8601_strings():
    assert to_epoch_ms("2026-11-10T14:02:55.310Z") == MOD_1
    assert to_epoch_ms("2026-11-12T09:45:10.101Z") == MOD_2


def test_to_epoch_ms_passes_through_epoch_values():
    assert to_epoch_ms(MOD_1) == MOD_1
    assert to_epoch_ms(str(MOD_1)) == MOD_1


def test_to_epoch_ms_maps_missing_values_to_none_not_zero():
    assert to_epoch_ms(None) is None
    assert to_epoch_ms("") is None


def test_absent_date_property_is_stored_as_sql_null(conn, fake_client):
    fake_client.set_pages("deals", [FakePage(records=[
        deal("4001", "2026-11-10T14:02:55.310Z", hs_last_activity_date=None)])])
    sync_incremental(conn, fake_client, ["deals"])
    row = conn.execute(
        "select hs_last_activity_date from deals where id = '4001'").fetchone()
    assert row["hs_last_activity_date"] is None


def test_records_are_stored_with_converted_dates_and_amount(conn, fake_client):
    two_deal_pages(fake_client)
    sync_incremental(conn, fake_client, ["deals"])
    row = conn.execute("select * from deals where id = '4001'").fetchone()
    assert row["hs_lastmodifieddate"] == MOD_1
    assert row["closedate"] == 1_797_292_800_000
    assert row["amount_in_home_currency"] == 18500.0
    assert row["archived"] == 0
    assert '"4001"' in row["raw"]


def test_owner_payload_names_are_mapped_to_columns():
    row = normalise_record("owners", {
        "id": "81234567", "email": "sam@example.com",
        "firstName": "Sam", "lastName": "Rivera", "archived": False})
    assert row["first_name"] == "Sam"
    assert row["last_name"] == "Rivera"
    assert row["email"] == "sam@example.com"


def test_engagement_rows_carry_their_endpoint_as_type():
    row = normalise_record("calls", {
        "id": "12001",
        "properties": {"hs_timestamp": "2026-11-03T15:00:00.000Z",
                       "hs_lastmodifieddate": "2026-11-03T15:20:41.100Z"}})
    assert row["type"] == "calls"
    assert row["timestamp"] == 1_793_718_000_000


# --- watermark semantics ------------------------------------------------------

def test_search_uses_stored_watermark_rewound_by_60s(conn, fake_client):
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)
    sync_incremental(conn, fake_client, ["deals"])
    assert WATERMARK_REWIND_MS == 60_000
    assert fake_client.search_calls[0].since_ms == START - 60_000


def test_rewind_never_produces_a_negative_since(conn, fake_client):
    two_deal_pages(fake_client)
    sync_incremental(conn, fake_client, ["deals"])
    assert fake_client.search_calls[0].since_ms == 0


def test_rewind_is_applied_to_reads_not_to_what_is_stored(conn, fake_client):
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)
    sync_incremental(conn, fake_client, ["deals"])
    assert watermark(conn, "deals") == MOD_2


def test_watermark_advances_to_max_modified_only_after_type_completes(
        conn, fake_client):
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)
    during = []
    serve = fake_client.search_modified_since

    def spy(object_type, since_ms, after):
        during.append(watermark(conn, object_type))
        return serve(object_type, since_ms, after)

    fake_client.search_modified_since = spy
    sync_incremental(conn, fake_client, ["deals"])

    assert during == [START, START]  # unmoved while paging
    assert watermark(conn, "deals") == MOD_2


def test_watermark_stays_put_when_a_type_returns_nothing(conn, fake_client):
    set_watermark(conn, "deals", START)
    fake_client.set_pages("deals", [FakePage(records=[])])
    sync_incremental(conn, fake_client, ["deals"])
    assert watermark(conn, "deals") == START


# --- crash and resume ---------------------------------------------------------

def test_crash_midway_leaves_watermark_unmoved_and_cursor_checkpointed(
        conn, fake_client):
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)
    fake_client.fail_on_page = 2
    with pytest.raises(RateLimited):
        sync_incremental(conn, fake_client, ["deals"])
    assert watermark(conn, "deals") == START
    assert cursor(conn, "deals") == "page-2-cursor"
    assert conn.execute("select count(*) from deals").fetchone()[0] == 1


def test_page_rows_and_cursor_are_written_in_one_transaction(
        conn, fake_client, monkeypatch):
    """If the checkpoint write fails, the page's rows must roll back with it."""
    two_deal_pages(fake_client)

    def broken_state_sql(object_type, fields):
        return "INSERT INTO no_such_table VALUES (1)", []

    monkeypatch.setattr("crm.sync._state_sql", broken_state_sql)
    with pytest.raises(sqlite3.OperationalError):
        sync_incremental(conn, fake_client, ["deals"])
    assert conn.execute("select count(*) from deals").fetchone()[0] == 0


def test_cursor_never_advances_without_the_rows_it_stands_for(
        conn, fake_client, monkeypatch):
    """The other direction: a failed row write must not leave a cursor behind."""
    two_deal_pages(fake_client)

    def broken_upsert_sql(connection, table, cols):
        return "INSERT INTO no_such_table VALUES (1)"

    monkeypatch.setattr("crm.sync.upsert_sql", broken_upsert_sql)
    with pytest.raises(sqlite3.OperationalError):
        sync_incremental(conn, fake_client, ["deals"])
    assert cursor(conn, "deals") is None


def test_rerun_after_crash_resumes_from_checkpointed_cursor(conn, fake_client):
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)
    fake_client.fail_on_page = 2
    with pytest.raises(RateLimited):
        sync_incremental(conn, fake_client, ["deals"])

    fake_client.fail_on_page = None
    fake_client.reset_calls()
    sync_incremental(conn, fake_client, ["deals"])

    assert fake_client.search_calls[-1].after == "page-2-cursor"
    # the resumed run must start at the checkpoint, not re-walk the type
    assert [c.after for c in fake_client.search_calls] == ["page-2-cursor"]
    assert fake_client.search_calls[0].since_ms == START - WATERMARK_REWIND_MS


def test_rerun_after_crash_loses_no_record_and_duplicates_none(
        conn, fake_client):
    two_deal_pages(fake_client)
    fake_client.fail_on_page = 2
    with pytest.raises(RateLimited):
        sync_incremental(conn, fake_client, ["deals"])
    fake_client.fail_on_page = None
    sync_incremental(conn, fake_client, ["deals"])

    ids = [r["id"] for r in conn.execute("select id from deals order by id")]
    assert ids == ["4001", "4002"]
    assert cursor(conn, "deals") is None


# --- pipelines and owners -----------------------------------------------------

def test_pipelines_refreshed_on_every_run(conn, fake_client):
    fake_client.pipelines = [{
        "id": "default", "label": "Sales Pipeline", "archived": False,
        "stages": [{"id": "closedwon", "label": "Closed Won",
                    "displayOrder": 5, "archived": False,
                    "metadata": {"isClosed": "true", "probability": "1.0"}}]}]
    two_deal_pages(fake_client)
    sync_incremental(conn, fake_client, ["deals"])
    assert fake_client.pipeline_calls == 1
    assert conn.execute("select label from pipelines").fetchone()[0] == "Sales Pipeline"
    stage = conn.execute("select * from stages where id = 'closedwon'").fetchone()
    assert stage["pipeline_id"] == "default"
    assert stage["is_closed"] == 1
    assert stage["probability"] == 1.0
    assert stage["display_order"] == 5


def test_refresh_pipelines_is_callable_on_its_own(conn, fake_client):
    fake_client.pipelines = [{"id": "default", "label": "Sales Pipeline",
                              "archived": False, "stages": []}]
    refresh_pipelines(conn, fake_client)
    assert fake_client.pipeline_calls == 1
    assert conn.execute("select count(*) from pipelines").fetchone()[0] == 1


def test_owners_are_refreshed_in_full_never_through_search(conn, fake_client):
    fake_client.set_pages("owners", [FakePage(records=[
        {"id": "81234567", "email": "sam@example.com", "firstName": "Sam",
         "lastName": "Rivera", "archived": False}])])
    two_deal_pages(fake_client)
    sync_incremental(conn, fake_client, ["deals"])
    assert [c.object_type for c in fake_client.list_calls] == ["owners"]
    assert all(c.object_type != "owners" for c in fake_client.search_calls)
    assert conn.execute("select email from owners").fetchone()[0] == "sam@example.com"


# --- associations -------------------------------------------------------------

def test_associations_refetched_only_for_changed_ids(conn, fake_client):
    two_deal_pages(fake_client)
    sync_incremental(conn, fake_client, ["deals"])
    assert set(fake_client.assoc_calls[0].ids) == {"4001", "4002"}
    assert fake_client.assoc_calls[0].from_type == "deals"


def test_associations_rewritten_not_appended(conn, fake_client):
    with conn:
        conn.execute("insert into associations values ('deals','4001','companies','9')")
    fake_client.set_pages("deals", [FakePage(records=[
        deal("4001", "2026-11-10T14:02:55.310Z")])])
    fake_client.associations[("deals", "companies")] = {"4001": ["10"]}
    sync_incremental(conn, fake_client, ["deals"])
    assert assoc_rows(conn, "companies") == [("deals", "4001", "companies", "10")]


def test_association_removal_deletes_the_stale_row(conn, fake_client):
    with conn:
        conn.execute("insert into associations values ('deals','4001','companies','9')")
    fake_client.set_pages("deals", [FakePage(records=[
        deal("4001", "2026-11-10T14:02:55.310Z")])])
    fake_client.associations[("deals", "companies")] = {"4001": []}
    sync_incremental(conn, fake_client, ["deals"])
    assert assoc_rows(conn, "companies") == []


def test_associations_untouched_for_records_that_did_not_change(
        conn, fake_client):
    with conn:
        conn.execute("insert into associations values ('deals','4009','companies','9')")
    fake_client.set_pages("deals", [FakePage(records=[
        deal("4001", "2026-11-10T14:02:55.310Z")])])
    fake_client.associations[("deals", "companies")] = {"4001": ["10"]}
    sync_incremental(conn, fake_client, ["deals"])
    assert assoc_rows(conn, "companies") == [
        ("deals", "4001", "companies", "10"),
        ("deals", "4009", "companies", "9"),
    ]


def test_only_the_three_signal_pairs_are_refetched(conn, fake_client):
    two_deal_pages(fake_client)
    fake_client.set_pages("companies", [FakePage(records=[
        {"id": "7001", "properties": {"name": "Acme",
                                      "hs_lastmodifieddate": "2026-11-10T14:02:55.310Z"}}])])
    fake_client.set_pages("contacts", [FakePage(records=[
        {"id": "5001", "properties": {"firstname": "Ada",
                                      "lastmodifieddate": "2026-11-10T14:02:55.310Z"}}])])
    sync_incremental(conn, fake_client, ["deals", "companies", "contacts"])
    pairs = {(c.from_type, c.to_type) for c in fake_client.assoc_calls}
    assert pairs == {("deals", "companies"), ("deals", "contacts"),
                     ("companies", "contacts"), ("contacts", "deals"),
                     ("contacts", "companies"), ("companies", "deals")}


def test_engagement_associations_not_bulk_synced(conn, fake_client):
    for engagement_type in ENGAGEMENT_TYPES:
        fake_client.set_pages(engagement_type, [FakePage(records=[
            {"id": f"1-{engagement_type}",
             "properties": {"hs_timestamp": "2026-11-03T15:00:00.000Z",
                            "hs_lastmodifieddate": "2026-11-03T15:20:41.100Z"}}])])
    sync_incremental(conn, fake_client, ["engagements"])
    engagement_names = set(ENGAGEMENT_TYPES) | {"engagements"}
    assert fake_client.assoc_calls == []
    assert all(c.from_type not in engagement_names
               for c in fake_client.assoc_calls)
    assert conn.execute(
        "select count(*) from engagements").fetchone()[0] == len(ENGAGEMENT_TYPES)


# --- report -------------------------------------------------------------------

def test_types_without_a_search_endpoint_are_rejected_loudly(conn, fake_client):
    with pytest.raises(ValueError, match="owners"):
        sync_incremental(conn, fake_client, ["owners"])


def test_report_counts_records_per_requested_type(conn, fake_client):
    two_deal_pages(fake_client)
    report = sync_incremental(conn, fake_client, ["deals"])
    assert isinstance(report, SyncReport)
    assert report.per_type == {"deals": 2}
    assert report.errors == []


def test_engagement_counts_are_reported_under_one_type(conn, fake_client):
    for engagement_type in ENGAGEMENT_TYPES:
        fake_client.set_pages(engagement_type, [FakePage(records=[
            {"id": f"1-{engagement_type}",
             "properties": {"hs_timestamp": "2026-11-03T15:00:00.000Z",
                            "hs_lastmodifieddate": "2026-11-03T15:20:41.100Z"}}])])
    report = sync_incremental(conn, fake_client, ["engagements"])
    assert report.per_type == {"engagements": len(ENGAGEMENT_TYPES)}
