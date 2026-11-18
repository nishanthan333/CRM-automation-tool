import math

import httpx
import pytest

from crm.hubspot import ENGAGEMENT_TYPES, RateLimited
from crm.sync import (
    BACKFILL_ORDER, Estimate, backfill, cursor, estimate_backfill,
    sync_incremental, watermark,
)
from tests.fakes import FakePage


def phase(conn, object_type):
    row = conn.execute(
        "SELECT phase FROM sync_state WHERE object_type = ?", (object_type,)
    ).fetchone()
    return row["phase"] if row else None


def ids(conn, table="deals"):
    return [r[0] for r in conn.execute(f"SELECT id FROM {table} ORDER BY id")]


def trace_cursor_writes(conn, object_type):
    """Log every cursor value committed to sync_state for one type."""
    conn.execute("CREATE TABLE cursor_log (object_type TEXT, cursor TEXT)")
    for event in ("INSERT", "UPDATE OF cursor"):
        name = "log_" + event.split()[0].lower()
        conn.execute(
            f"CREATE TRIGGER {name} AFTER {event} ON sync_state "
            "BEGIN INSERT INTO cursor_log VALUES (new.object_type, new.cursor); END")
    conn.commit()
    return lambda: [r[0] for r in conn.execute(
        "SELECT cursor FROM cursor_log WHERE object_type = ?", (object_type,))]


def test_backfill_order_puts_briefing_objects_before_engagements():
    assert BACKFILL_ORDER == ["owners", "companies", "deals", "contacts",
                              "engagements"]


def test_backfill_refreshes_pipelines_before_walking(conn, fake_client):
    order = []
    original_pipelines = fake_client.list_pipelines
    original_list = fake_client.list_page
    fake_client.list_pipelines = lambda t: (order.append("pipelines"), original_pipelines(t))[1]
    fake_client.list_page = lambda *a, **k: (order.append("list"), original_list(*a, **k))[1]
    backfill(conn, fake_client)
    assert order[0] == "pipelines"
    assert fake_client.pipeline_calls == 1


def test_backfill_walks_types_in_the_documented_order(conn, fake_client):
    backfill(conn, fake_client)
    seen = list(dict.fromkeys(c.object_type for c in fake_client.list_calls))
    assert seen == ["owners", "companies", "deals", "contacts", *ENGAGEMENT_TYPES]


def test_backfill_uses_list_endpoint_never_search(conn, fake_client):
    fake_client.pages["deals"] = 3
    backfill(conn, fake_client)
    assert fake_client.search_calls == []
    assert [c.object_type for c in fake_client.list_calls].count("deals") == 3


def test_cursor_written_after_every_page(conn, fake_client):
    fake_client.pages["deals"] = 3
    log = trace_cursor_writes(conn, "deals")
    backfill(conn, fake_client)
    written = log()
    # One checkpoint per page: the two intermediate cursors, then cleared.
    assert [c for c in written if c is not None] == [
        "deals-page-2-cursor", "deals-page-3-cursor"]
    assert written[-1] is None


def test_each_page_and_its_cursor_commit_together(conn, fake_client):
    """Failing on page 3 of 3: pages 1-2 and the cursor to page 3 are durable."""
    fake_client.pages["deals"] = 3
    fake_client.fail_on_page = 3
    with pytest.raises(RateLimited):
        backfill(conn, fake_client)
    assert len(ids(conn)) == 4
    assert cursor(conn, "deals") == "deals-page-3-cursor"


def test_429_midway_resumes_without_skipping_or_duplicating(conn, fake_client):
    fake_client.pages["deals"] = 4
    expected = sorted(f"deals-{p}-{i}" for p in range(4) for i in range(2))
    fake_client.fail_on_page = 3
    with pytest.raises(RateLimited):
        backfill(conn, fake_client)
    assert phase(conn, "deals") != "complete"
    fake_client.fail_on_page = None
    fake_client.reset_calls()
    backfill(conn, fake_client)
    # Resumed at the failed page (3): not page 1, and not page 4.
    deal_calls = [c.after for c in fake_client.list_calls if c.object_type == "deals"]
    assert deal_calls == ["deals-page-3-cursor", "deals-page-4-cursor"]
    assert ids(conn) == expected
    assert len(ids(conn)) == len(set(ids(conn)))
    assert phase(conn, "deals") == "complete"


def test_types_finished_before_the_failure_are_not_redone(conn, fake_client):
    fake_client.pages["deals"] = 3
    fake_client.fail_on_page = 2
    with pytest.raises(RateLimited):
        backfill(conn, fake_client)
    assert phase(conn, "owners") == "complete"
    assert phase(conn, "companies") == "complete"
    assert phase(conn, "contacts") is None          # never reached
    fake_client.fail_on_page = None
    fake_client.reset_calls()
    backfill(conn, fake_client)
    walked = {c.object_type for c in fake_client.list_calls}
    assert "companies" not in walked and "owners" not in walked


def test_completed_type_is_not_rewalked_on_rerun(conn, fake_client):
    backfill(conn, fake_client)
    for object_type in ("owners", "companies", "deals", "contacts",
                        *ENGAGEMENT_TYPES):
        assert phase(conn, object_type) == "complete"
    fake_client.reset_calls()
    backfill(conn, fake_client)
    assert fake_client.list_calls == []
    assert fake_client.assoc_calls == []


def test_completion_clears_cursor_and_phase_survives_incremental(conn, fake_client):
    backfill(conn, fake_client)
    assert cursor(conn, "deals") is None
    sync_incremental(conn, fake_client, ["deals"])
    assert phase(conn, "deals") == "complete"


def test_watermark_is_walk_start_not_max_modified(conn, fake_client, monkeypatch):
    """List order is by id, so a record walked early and edited later can carry
    a modified date below the max seen. The watermark must be when the walk
    began, so the first incremental run re-reads everything since then."""
    monkeypatch.setattr("crm.sync._now_ms", lambda: 5_000)
    fake_client.set_pages("deals", [FakePage(records=[
        {"id": "1", "properties": {"hs_lastmodifieddate": "2030-01-01T00:00:00Z"}},
    ])])
    backfill(conn, fake_client)
    assert watermark(conn, "deals") == 5_000


def test_watermark_survives_a_resume(conn, fake_client, monkeypatch):
    now = {"t": 1_000}
    monkeypatch.setattr("crm.sync._now_ms", lambda: now["t"])
    fake_client.pages["deals"] = 3
    fake_client.fail_on_page = 2
    with pytest.raises(RateLimited):
        backfill(conn, fake_client)
    started = watermark(conn, "deals")
    assert started == 1_000
    now["t"] = 9_000
    fake_client.fail_on_page = None
    backfill(conn, fake_client)
    assert watermark(conn, "deals") == started


def test_rows_are_normalised_through_the_shared_helper(conn, fake_client):
    fake_client.set_pages("deals", [FakePage(records=[{
        "id": "7", "properties": {
            "dealname": "Big", "hs_lastmodifieddate": "2024-03-01T00:00:00Z"}}])])
    backfill(conn, fake_client)
    row = conn.execute("SELECT * FROM deals WHERE id = '7'").fetchone()
    assert row["dealname"] == "Big"
    assert row["hs_lastmodifieddate"] == 1709251200000


def test_backfill_fetches_associations_for_walked_records(conn, fake_client):
    fake_client.pages["deals"] = 1
    fake_client.associations[("deals", "companies")] = {"deals-0-0": ["c9"]}
    backfill(conn, fake_client)
    links = conn.execute(
        "SELECT from_id, to_id FROM associations WHERE from_type = 'deals' "
        "AND to_type = 'companies'").fetchall()
    assert [tuple(r) for r in links] == [("deals-0-0", "c9")]


def test_engagements_fan_out_into_one_table_with_per_type_state(conn, fake_client):
    fake_client.set_pages("notes", [FakePage(records=[
        {"id": "n1", "properties": {}}])])
    fake_client.set_pages("calls", [FakePage(records=[
        {"id": "c1", "properties": {}}])])
    report = backfill(conn, fake_client)
    assert {"c1", "n1"} <= set(ids(conn, "engagements"))
    assert report.per_type["engagements"] == len(ids(conn, "engagements"))
    for api_type in ENGAGEMENT_TYPES:
        assert phase(conn, api_type) == "complete"


def test_http_error_keeps_the_checkpoint_and_other_types_continue(conn, fake_client):
    fake_client.pages["deals"] = 3
    original = fake_client.list_page

    def flaky(object_type, after, archived=False):
        if object_type == "deals" and after == "deals-page-2-cursor":
            raise httpx.HTTPStatusError(
                "boom", request=httpx.Request("GET", "http://x"),
                response=httpx.Response(400))
        return original(object_type, after, archived)

    fake_client.list_page = flaky
    report = backfill(conn, fake_client)
    assert any(e.startswith("deals:") for e in report.errors)
    # Progress is kept: a cleared cursor would re-walk potentially millions.
    assert cursor(conn, "deals") == "deals-page-2-cursor"
    assert phase(conn, "deals") != "complete"
    assert phase(conn, "contacts") == "complete"


def test_estimate_reports_record_count_and_request_count_per_type(conn, fake_client):
    fake_client.counts.update({"companies": 250, "deals": 101, "contacts": 0})
    est = estimate_backfill(conn, fake_client)
    assert all(isinstance(e, Estimate) for e in est)
    assert [e.object_type for e in est] == BACKFILL_ORDER
    assert est[0].object_type == "owners"
    assert est[0].requests == math.ceil(est[0].records / 100)
    by_type = {e.object_type: e for e in est}
    assert (by_type["companies"].records, by_type["companies"].requests) == (250, 3)
    assert (by_type["deals"].records, by_type["deals"].requests) == (101, 2)
    assert by_type["contacts"].requests == 0


def test_estimate_excludes_owners_and_sums_engagements(conn, fake_client):
    fake_client.counts.update({"calls": 150, "notes": 1, "meetings": 0})
    est = {e.object_type: e for e in estimate_backfill(conn, fake_client)}
    assert "owners" not in fake_client.count_calls
    assert est["engagements"].records == 151
    assert est["engagements"].requests == 2 + 1     # per-type ceil, summed


def test_estimate_makes_no_writes_and_no_walk(conn, fake_client):
    fake_client.counts["deals"] = 10_000
    before = conn.total_changes
    estimate_backfill(conn, fake_client)
    assert conn.total_changes == before
    assert conn.execute("SELECT count(*) FROM sync_state").fetchone()[0] == 0
    assert fake_client.list_calls == [] and fake_client.search_calls == []


def test_progress_callback_invoked_per_page(conn, fake_client):
    fake_client.pages["deals"] = 3
    seen = []
    backfill(conn, fake_client, progress=lambda t, n: seen.append((t, n)))
    assert [n for t, n in seen if t == "deals"] == [2, 4, 6]   # running totals
    assert {t for t, _ in seen} >= {"owners", "companies", "deals", "contacts"}
