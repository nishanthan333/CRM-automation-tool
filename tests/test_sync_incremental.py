import sqlite3

import httpx
import pytest

from crm import sync
from crm.hubspot import ENGAGEMENT_TYPES, Page, RateLimited
from crm.sync import (
    WATERMARK_REWIND_MS,
    SyncReport,
    _rewrite_pair,
    cursor,
    normalise_record,
    refresh_pipelines,
    set_cursor,
    set_watermark,
    sync_incremental,
    to_epoch_ms,
    watermark,
)
from tests.conftest import mark_backfilled
from tests.fakes import FakePage

START = 1_700_000_000_000
MOD_1_ISO = "2026-11-10T14:02:55.310Z"
MOD_2_ISO = "2026-11-12T09:45:10.101Z"
MOD_1 = 1_794_319_375_310  # 2026-11-10T14:02:55.310Z
MOD_2 = 1_794_476_710_101  # 2026-11-12T09:45:10.101Z


def http_error(status: int = 400) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://api.hubapi.com/crm/v3/x")
    return httpx.HTTPStatusError(
        f"HTTP {status}", request=request,
        response=httpx.Response(status, request=request))


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


# --- the watermark waits for the associations ---------------------------------

def test_watermark_unmoved_when_association_refresh_raises(conn, fake_client):
    """A record past the watermark is never searched again, so its association
    rows would stay stale for good if the watermark advanced first."""
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)

    def boom(from_type, to_type, ids):
        raise RateLimited("fake 429 on associations")

    fake_client.batch_associations = boom
    with pytest.raises(RateLimited):
        sync_incremental(conn, fake_client, ["deals"])
    assert watermark(conn, "deals") == START


def test_watermark_unmoved_when_association_refresh_is_a_recorded_error(
        conn, fake_client):
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)

    def boom(from_type, to_type, ids):
        raise http_error(400)

    fake_client.batch_associations = boom
    report = sync_incremental(conn, fake_client, ["deals"])
    assert watermark(conn, "deals") == START
    assert report.errors and report.errors[0].startswith("deals:")


def test_watermark_advances_once_associations_land(conn, fake_client):
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)
    sync_incremental(conn, fake_client, ["deals"])
    assert watermark(conn, "deals") == MOD_2


# --- the ~10,000 result search cap --------------------------------------------

def test_search_window_restarts_before_the_result_cap(
        conn, fake_client, monkeypatch):
    monkeypatch.setattr("crm.sync.SEARCH_WINDOW_LIMIT", 1)
    set_watermark(conn, "deals", START)
    served, checkpoints = [], []

    def search(object_type, since_ms, after):
        served.append((since_ms, after))
        checkpoints.append(cursor(conn, object_type))
        if len(served) == 1:
            return Page(records=[deal("4001", MOD_1_ISO)], after="page-2-cursor")
        return Page(records=[deal("4002", MOD_2_ISO)], after=None)

    fake_client.search_modified_since = search
    sync_incremental(conn, fake_client, ["deals"])

    # the capped window is abandoned for a fresh search, not paged into the cap
    assert served == [(START - WATERMARK_REWIND_MS, None), (MOD_1 - 1, None)]
    assert checkpoints[1] is None  # the stale cursor was cleared durably
    assert watermark(conn, "deals") == MOD_2
    assert [r["id"] for r in conn.execute("select id from deals order by id")] \
        == ["4001", "4002"]


def test_a_resumed_cursor_re_windows_instead_of_paging_toward_the_cap(
        conn, fake_client):
    """The `after` token is opaque, so we cannot know how deep into the cap it
    already sits. A resumed cursor must re-window at the first page boundary,
    not page a further SEARCH_WINDOW_LIMIT records on top of an unknown offset.
    Note: no monkeypatched limit here — the real 9,000 must not be reached."""
    set_watermark(conn, "deals", START)
    set_cursor(conn, "deals", "deep-cursor")
    served = []

    def search(object_type, since_ms, after):
        served.append((since_ms, after))
        if len(served) == 1:
            return Page(records=[deal("4001", MOD_1_ISO)], after="next-page")
        return Page(records=[deal("4002", MOD_2_ISO)], after=None)

    fake_client.search_modified_since = search
    sync_incremental(conn, fake_client, ["deals"])

    assert served == [(START - WATERMARK_REWIND_MS, "deep-cursor"),
                      (MOD_1 - 1, None)]
    assert watermark(conn, "deals") == MOD_2
    assert cursor(conn, "deals") is None


def test_a_failed_type_drops_its_cursor_so_the_next_run_re_walks(
        conn, fake_client):
    """A cursor a type failed on may be poisoned; the watermark holds the line."""
    set_watermark(conn, "deals", START)
    served = []

    def search(object_type, since_ms, after):
        served.append(after)
        if len(served) == 1:
            return Page(records=[deal("4001", MOD_1_ISO)], after="page-2-cursor")
        raise http_error(400)

    fake_client.search_modified_since = search
    report = sync_incremental(conn, fake_client, ["deals"])

    assert any(e.startswith("deals:") for e in report.errors)
    assert cursor(conn, "deals") is None
    assert watermark(conn, "deals") == START


def test_a_rate_limited_type_keeps_its_cursor(conn, fake_client):
    """The opposite case: resuming from the checkpoint is the point of backoff."""
    set_watermark(conn, "deals", START)
    two_deal_pages(fake_client)
    fake_client.fail_on_page = 2
    with pytest.raises(RateLimited):
        sync_incremental(conn, fake_client, ["deals"])
    assert cursor(conn, "deals") == "page-2-cursor"


def test_a_window_that_cannot_advance_fails_loudly(
        conn, fake_client, monkeypatch):
    monkeypatch.setattr("crm.sync.SEARCH_WINDOW_LIMIT", 1)
    calls = []

    def search(object_type, since_ms, after):
        calls.append(after)
        # bounded, so a sync that pages on forever fails instead of hanging
        assert len(calls) <= 10, "paged past the cap instead of re-windowing"
        return Page(records=[deal("4001", MOD_1_ISO)], after="next")

    fake_client.search_modified_since = search
    # Loudly, but against the type: SearchWindowStuck is in _TYPE_ERRORS.
    report = sync_incremental(conn, fake_client, ["deals"])
    assert any("search result cap" in e for e in report.errors)
    with pytest.raises(RuntimeError, match="search result cap"):
        sync._sync_one_type(conn, fake_client, "deals")


# --- one type's failure is not the run's --------------------------------------

def test_reference_data_failure_does_not_gate_the_object_sync(
        conn, fake_client):
    two_deal_pages(fake_client)

    real = fake_client.list_page

    def boom(object_type, after, archived=False):
        if object_type != "owners":
            return real(object_type, after, archived)
        raise RateLimited("fake 429 on owners")

    fake_client.list_page = boom
    report = sync_incremental(conn, fake_client, ["deals"])
    assert report.per_type == {"deals": 2}
    assert any(w.startswith("owners:") for w in report.warnings)
    assert conn.execute("select count(*) from deals").fetchone()[0] == 2


def _signals_phase(conn):
    row = conn.execute("SELECT phase, status FROM sync_state"
                       " WHERE object_type = 'signals'").fetchone()
    return row["phase"]


@pytest.mark.parametrize("attribute, name", [("account_details", "account"),
                                             ("list_page", "owners")])
def test_cosmetic_reference_data_failure_is_a_warning_not_a_partial_briefing(
        conn, fake_client, attribute, name):
    """Owners and the account-info lookup only build links and labels; five
    signals depend on `stages`, nothing depends on these."""
    two_deal_pages(fake_client)
    real = getattr(fake_client, attribute)

    def boom(object_type=None, *args, **kwargs):
        if object_type in (None, "owners"):
            raise http_error(403)
        return real(object_type, *args, **kwargs)

    setattr(fake_client, attribute, boom)
    report = sync_incremental(conn, fake_client, ["deals"])
    assert report.errors == []
    assert any(w.startswith(f"{name}:") for w in report.warnings)
    assert _signals_phase(conn) == "complete"


def test_stale_pipelines_still_make_the_briefing_partial(conn, fake_client):
    two_deal_pages(fake_client)

    def boom(object_type):
        raise http_error(403)

    fake_client.list_pipelines = boom
    report = sync_incremental(conn, fake_client, ["deals"])
    assert any(e.startswith("pipelines:") for e in report.errors)
    assert _signals_phase(conn) == "partial"


def test_a_malformed_date_is_confined_to_its_own_type(conn, fake_client):
    fake_client.set_pages("deals", [FakePage(records=[
        deal("4001", "not-a-date")])])
    fake_client.set_pages("companies", [FakePage(records=[
        {"id": "7001", "properties": {"name": "Acme",
                                      "hs_lastmodifieddate": MOD_1_ISO}}])])
    report = sync_incremental(conn, fake_client, ["deals", "companies"])

    assert any(e.startswith("deals:") for e in report.errors)
    assert watermark(conn, "deals") == 0
    assert conn.execute("select count(*) from companies").fetchone()[0] == 1
    assert watermark(conn, "companies") == MOD_1


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
    assert [c.object_type for c in fake_client.list_calls
            if not c.archived] == ["owners"]
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


def test_reverse_side_links_are_stored_in_the_canonical_direction(
        conn, fake_client):
    """A changed contact is on the `to` side of both its pairs: the rows it
    writes must still read as deals -> contacts and companies -> contacts,
    which is what makes "whose deals are these" answerable."""
    fake_client.set_pages("contacts", [FakePage(records=[
        {"id": "5001", "properties": {"firstname": "Ada",
                                      "lastmodifieddate": MOD_1_ISO}}])])
    fake_client.associations[("contacts", "deals")] = {"5001": ["4001"]}
    fake_client.associations[("contacts", "companies")] = {"5001": ["7001"]}
    sync_incremental(conn, fake_client, ["contacts"])
    assert assoc_rows(conn, "contacts") == [
        ("deals", "4001", "contacts", "5001"),
        ("companies", "7001", "contacts", "5001"),
    ]


def test_reverse_side_rewrite_replaces_only_that_records_links(
        conn, fake_client):
    with conn:
        conn.execute("insert into associations values ('deals','4001','contacts','5001')")
        conn.execute("insert into associations values ('deals','4009','contacts','5001')")
    fake_client.set_pages("contacts", [FakePage(records=[
        {"id": "5001", "properties": {"firstname": "Ada",
                                      "lastmodifieddate": MOD_1_ISO}}])])
    fake_client.associations[("contacts", "deals")] = {"5001": ["4002"]}
    sync_incremental(conn, fake_client, ["contacts"])
    assert assoc_rows(conn, "contacts") == [("deals", "4002", "contacts", "5001")]


def test_rewriting_one_pair_leaves_the_records_other_pair_alone(conn):
    """`_rewrite_pair` directly: the delete must be scoped to its own pair, or
    rewriting a deal's company links silently drops its contact links."""
    with conn:
        conn.execute("insert into associations values ('deals','4001','companies','9')")
        conn.execute("insert into associations values ('deals','4001','contacts','8')")
    _rewrite_pair(conn, ("deals", "companies"), "deals", {"4001": ["10"]})
    assert assoc_rows(conn, "companies") == [("deals", "4001", "companies", "10")]
    assert assoc_rows(conn, "contacts") == [("deals", "4001", "contacts", "8")]


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
    # each endpoint's rows carry that endpoint's name, not one shared value
    stored = {row["type"] for row in conn.execute("select type from engagements")}
    assert stored == set(ENGAGEMENT_TYPES)


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


def test_never_backfilled_type_is_not_synced_through_search(conn, fake_client):
    from crm.sync import sync_incremental
    fake_client.set_pages("deals", 1)
    report = sync_incremental(conn, fake_client, ["deals"])   # phase is NULL
    assert fake_client.search_calls == []
    assert any("deals" in e and "never backfilled" in e for e in report.errors)
    assert report.per_type["deals"] == 0
    assert conn.execute("select count(*) from deals").fetchone()[0] == 0


@pytest.fixture(autouse=True)
def _backfilled(request, conn):
    """Incremental sync needs completed backfills; the one test about a never
    backfilled type opts out by name."""
    if "never_backfilled" not in request.node.name:
        mark_backfilled(conn)


def test_a_type_wedged_on_the_result_cap_does_not_abort_the_run(conn, fake_client):
    """The cap condition is one type's problem, like every other failure here:
    it must not take the other types and the signal re-evaluation with it."""
    fake_client.set_pages("companies", [FakePage(records=[
        {"id": "7001", "properties": {"hs_lastmodifieddate": MOD_1_ISO}}])])
    real = fake_client.search_modified_since

    def search(object_type, since_ms, after):
        if object_type != "deals":
            return real(object_type, since_ms, after)
        return Page(records=[deal("4001", MOD_1_ISO)], after="next")

    fake_client.search_modified_since = search
    report = sync_incremental(conn, fake_client, ["deals", "companies"])
    assert any("search result cap" in e and e.startswith("deals:")
               for e in report.errors)
    assert report.per_type["companies"] == 1
    assert conn.execute("SELECT count(*) FROM sync_state WHERE object_type ="
                        " 'signals'").fetchone()[0] == 1
