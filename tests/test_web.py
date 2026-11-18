import re
import warnings
from pathlib import Path

import httpx
import pytest

with warnings.catch_warnings():
    # Starlette 1.x warns, at import, that its TestClient wants httpx2. That is
    # a third-party deprecation, not ours; scope the ignore to this one import
    # so `-W error` stays strict everywhere else.
    warnings.filterwarnings("ignore", message=".*starlette.testclient.*")
    from fastapi.testclient import TestClient

from crm.config import Config
from crm.hubspot import ENGAGEMENT_TYPES
from crm.signals import evaluate_all
from crm.web import create_app
from tests.conftest import (DEFAULT_THRESHOLDS, NOW, UTC, add_company,
                            add_deal, add_owner, add_stage, days_ago)

ROOT = Path(__file__).parent.parent


def make_cfg(db_path):
    return Config(hubspot_token="t", db_path=db_path, timezone="local",
                  week_starts_on="monday", ollama_model="test-model",
                  ollama_url="http://ollama.test", sync_interval_minutes=15)


def stub(text="select count(*) from deals"):
    return httpx.Client(transport=httpx.MockTransport(
        lambda req: httpx.Response(200, json={"response": text})))


def refusing():
    def handler(req):
        raise httpx.ConnectError("refused", request=req)
    return httpx.Client(transport=httpx.MockTransport(handler))


def build(conn, fake_client, http=None):
    path = Path(conn.execute("PRAGMA database_list").fetchone()["file"])
    app = create_app(make_cfg(path), client=fake_client,
                     http=http or stub(), now_ms=lambda: NOW)
    return TestClient(app)


def name_deal(conn, deal_id, name):
    with conn:
        conn.execute("UPDATE deals SET dealname = ? WHERE id = ?", (name, deal_id))


@pytest.fixture(autouse=True)
def home(monkeypatch):
    monkeypatch.setenv("WATCHDOG_HOME", str(ROOT / "crm" / "defaults"))


@pytest.fixture
def seeded(conn):
    """d1: stalled, past close, big and quiet. d2: unowned. c1: untouched."""
    add_stage(conn, "s_open", label="Qualify")
    with conn:
        conn.execute("INSERT INTO pipelines (id, label) VALUES ('p1', 'Sales')")
    add_owner(conn, "o1", "Rhea", "Mehta")
    add_deal(conn, "d1", amount=150000.0, activity=days_ago(20),
             closedate=days_ago(10))
    name_deal(conn, "d1", "Acme renewal")
    add_deal(conn, "d2", amount=50000.0, owner=None)
    name_deal(conn, "d2", "Orphan deal")
    add_company(conn, "c1", activity=days_ago(60), open_deals=["d1"])
    with conn:
        conn.execute("UPDATE companies SET name = 'Acme Corp' WHERE id = 'c1'")
        conn.execute("INSERT INTO sync_state (object_type, last_run_at, phase)"
                     " VALUES ('deals', ?, 'complete')", (NOW - 4 * 60_000,))
    evaluate_all(conn, DEFAULT_THRESHOLDS, NOW, tz=UTC)
    return conn


@pytest.fixture
def client(seeded, fake_client):
    return build(seeded, fake_client)


@pytest.fixture
def client_empty(conn, fake_client):
    return build(conn, fake_client)


@pytest.fixture
def client_no_ollama(seeded, fake_client):
    return build(seeded, fake_client, http=refusing())


def cards(html):
    return re.findall(
        r'<details class="card[^"]*"[^>]*>\s*<summary>\s*<span class="card-title">([^<]+)<',
        html)


# --- the briefing leads --------------------------------------------------------

def test_index_renders_on_empty_database(client_empty):
    r = client_empty.get("/")
    assert r.status_code == 200
    assert "No data yet" in r.text and "watchdog sync --backfill" in r.text


def test_index_lists_signals_most_urgent_first(client):
    assert cards(client.get("/").text) == [
        "Big and quiet", "Past its close date", "Stalled deals",
        "Untouched accounts", "Unowned records"]


def test_briefing_comes_before_pipeline_and_ask(client):
    body = client.get("/").text
    assert (body.index("Needs attention") < body.index("Pipeline")
            < body.index("Ask the data"))


def test_index_shows_deduplicated_total_at_risk(client):
    # d1 is in three signals but counts once: 150,000 + d2's 50,000.
    assert "$200,000 at risk" in client.get("/").text


def test_last_sync_time_shown(client):
    assert "last sync 4m ago" in client.get("/").text


def test_last_sync_never_on_fresh_database(client_empty):
    assert "last sync never" in client_empty.get("/").text


def test_index_says_when_data_is_incomplete(seeded, fake_client):
    with seeded:
        seeded.execute(
            "INSERT INTO sync_state (object_type, phase, status) VALUES"
            " ('signals', 'partial',"
            " 'data incomplete: quiet_pipeline not re-evaluated')")
    body = build(seeded, fake_client).get("/").text
    assert "<strong>Data incomplete.</strong>" in body
    assert "quiet_pipeline not re-evaluated" in body
    assert "data incomplete: " not in body      # not said twice


def test_index_without_partial_marker_does_not_claim_incomplete(client):
    assert "data incomplete" not in client.get("/").text


def test_index_still_renders_when_ollama_is_down(client_no_ollama):
    r = client_no_ollama.get("/")
    assert r.status_code == 200 and "Stalled deals" in r.text


def test_pipeline_shows_open_stage_with_count_and_value(client):
    body = client.get("/").text
    assert "Qualify" in body and "2 deals" in body


def test_pipeline_owner_filter(client):
    assert "2 deals ·" in client.get("/").text
    filtered = client.get("/?owner=o1").text
    assert "1 deal ·" in filtered and "2 deals" not in filtered
    assert "0 deals ·" in client.get("/?owner=nobody").text


# --- every flag explains itself --------------------------------------------------

def test_signal_page_shows_owner_rollup_before_records(client):
    body = client.get("/signal/stalled_deal").text
    assert body.index("Rhea Mehta") < body.index("Acme renewal")


def test_signal_page_shows_rule_in_plain_english_and_its_sql(client):
    body = client.get("/signal/stalled_deal").text
    assert "more than 14 days ago" in body       # the rule, thresholds filled in
    assert "archived = 0" in body and "FROM deals d" in body   # the SQL


def test_index_card_also_carries_rule_and_sql(client):
    body = client.get("/").text
    assert "more than 14 days ago" in body and "archived = 0" in body


def test_unknown_or_disabled_signal_is_404(client):
    assert client.get("/signal/nope").status_code == 404
    assert client.get("/signal/stage_regression").status_code == 404


def test_signal_record_list_is_paginated(seeded, fake_client):
    for i in range(60):
        add_deal(seeded, f"x{i}", activity=days_ago(30))
    evaluate_all(seeded, DEFAULT_THRESHOLDS, NOW, tz=UTC)
    c = build(seeded, fake_client)
    page1 = c.get("/signal/stalled_deal").text
    assert page1.count("<tr") <= 51
    assert "page=2" in page1
    page2 = c.get("/signal/stalled_deal?page=2").text
    assert 1 < page2.count("<tr") <= 51
    assert c.get("/signal/stalled_deal?page=abc").status_code == 422


def test_owner_filter_narrows_records(client):
    body = client.get("/signal/unowned_record?owner=_none").text
    assert "Orphan deal" in body
    assert "Acme Corp" not in body


# --- records --------------------------------------------------------------------

def test_record_page_uses_hubspot_property_labels(client):
    body = client.get("/record/deals/d1").text
    assert "Amount in company currency" in body and "Deal Stage" in body
    assert "Qualify" in body and "Rhea Mehta" in body   # ids resolved to labels


def test_record_page_links_back_to_hubspot_with_portal_and_ui_domain(seeded, fake_client):
    from crm.sync import sync_incremental
    sync_incremental(seeded, fake_client)     # learns portalId / uiDomain
    c = build(seeded, fake_client)
    assert ('href="https://app-eu1.hubspot.com/contacts/4242/record/0-3/d1"'
            in c.get("/record/deals/d1").text)
    assert "/record/0-2/c1" in c.get("/record/companies/c1").text


def test_record_page_has_no_link_until_account_is_known(client):
    body = client.get("/record/deals/d1").text
    assert "hubspot.com" not in body
    assert "Run a sync to enable the link" in body


def test_hubspot_link_ignores_a_hostile_ui_domain(seeded, fake_client):
    with seeded:
        seeded.executemany("INSERT INTO meta VALUES (?, ?)", [
            ("portal_id", "4242"), ("ui_domain", "evil.example/x")])
    assert "evil" not in build(seeded, fake_client).get("/record/deals/d1").text


def test_record_page_explains_why_it_is_flagged(client):
    body = client.get("/record/deals/d1").text
    assert "Stalled deals" in body and "/signal/stalled_deal" in body


def test_record_page_fetches_engagements_on_demand(seeded, fake_client):
    # HubSpot v3 has no generic "engagements" object: the five real types are
    # read one at a time and their ids unioned.
    fake_client.associations[("deals", "calls")] = {"d1": ["e1"]}
    fake_client.associations[("deals", "notes")] = {"d1": ["e2"]}
    with seeded:
        seeded.executemany(
            "INSERT INTO engagements (id, type, timestamp) VALUES (?, ?, ?)",
            [("e1", "calls", days_ago(2)), ("e2", "notes", days_ago(1))])
    body = build(seeded, fake_client).get("/record/deals/d1").text
    assert [c.to_type for c in fake_client.assoc_calls] == list(ENGAGEMENT_TYPES)
    assert all(c.ids == ["d1"] for c in fake_client.assoc_calls)
    assert "calls" in body and "notes" in body
    assert "2 associated" in body
    assert "unavailable" not in body


def test_record_page_degrades_when_one_engagement_type_fails(seeded, fake_client):
    fake_client.associations[("deals", "calls")] = {"d1": ["e1"]}
    with seeded:
        seeded.execute("INSERT INTO engagements (id, type, timestamp) VALUES"
                       " ('e1', 'calls', ?)", (days_ago(2),))
    real = fake_client.batch_associations

    def flaky(from_type, to_type, ids):
        if to_type == "tasks":
            raise RuntimeError("hubspot down")
        return real(from_type, to_type, ids)

    fake_client.batch_associations = flaky
    r = build(seeded, fake_client).get("/record/deals/d1")
    assert r.status_code == 200
    assert "calls" in r.text                      # what did arrive is shown
    assert "tasks" in r.text                      # and the gap is named
    assert "Activity history unavailable" not in r.text


def test_record_page_renders_without_engagements_when_fetch_fails(client, fake_client):
    def boom(*a, **k):
        raise RuntimeError("hubspot down")
    fake_client.batch_associations = boom
    r = client.get("/record/deals/d1")
    assert r.status_code == 200 and "Deal Stage" in r.text
    assert "activity history unavailable" in r.text.lower()


def test_record_page_404s(client):
    assert client.get("/record/deals/nope").status_code == 404
    assert client.get("/record/widgets/d1").status_code == 404


def test_hubspot_supplied_text_is_escaped(seeded, fake_client):
    name_deal(seeded, "d1", "<script>alert(1)</script>")
    c = build(seeded, fake_client)
    for url in ("/", "/signal/stalled_deal", "/record/deals/d1"):
        assert "<script>alert(1)" not in c.get(url).text, url
    assert "&lt;script&gt;" in c.get("/record/deals/d1").text


# --- ask ------------------------------------------------------------------------

def test_ask_shows_generated_sql_above_results(client):
    body = client.post("/ask", data={"question": "how many deals?"}).text
    assert body.index("select count(*)") < body.index("Results")
    assert "Needs attention" in body     # the rest of the page is still there


def test_ask_model_prose_is_escaped_and_not_treated_as_success(seeded, fake_client):
    c = build(seeded, fake_client, http=stub("<script>alert(1)</script> sorry"))
    body = c.post("/ask", data={"question": "hi"}).text
    assert "<script>alert(1)" not in body
    assert "&lt;script&gt;" in body
    assert "Results" not in body


def test_ask_when_ollama_is_down_degrades(client_no_ollama):
    r = client_no_ollama.post("/ask", data={"question": "how many deals?"})
    assert r.status_code == 200
    assert "Stalled deals" in r.text and "Results" not in r.text


def test_ask_result_cells_are_escaped(seeded, fake_client):
    name_deal(seeded, "d1", "<b>bold</b>")
    c = build(seeded, fake_client, http=stub("select dealname from deals"))
    body = c.post("/ask", data={"question": "names"}).text
    assert "<b>bold</b>" not in body and "&lt;b&gt;bold" in body


# --- actions --------------------------------------------------------------------

def test_snooze_redirects_and_hides_record(client):
    r = client.post("/snooze", data={"signal_id": "stalled_deal",
                                     "record_id": "d1", "days": 7},
                    follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/signal/stalled_deal"
    assert "Acme renewal" not in client.get("/signal/stalled_deal").text


def test_snooze_rejects_bad_input_with_4xx(client):
    bad = [{"signal_id": "nope", "record_id": "d1", "days": 7},
           {"signal_id": "stalled_deal", "record_id": "d1", "record_type": "x",
            "days": 7},
           {"signal_id": "stalled_deal", "record_id": "d1", "days": 0},
           {"signal_id": "stalled_deal", "record_id": "d1", "days": "soon"}]
    for form in bad:
        assert 400 <= client.post("/snooze", data=form).status_code < 500, form


def test_sync_button_triggers_incremental_not_backfill(client, fake_client):
    r = client.post("/sync", follow_redirects=False)
    assert r.status_code == 303
    assert fake_client.search_calls
    assert all(not c.archived for c in fake_client.list_calls)


def test_sync_reports_errors_from_the_run(seeded, fake_client):
    from crm.hubspot import RateLimited

    def boom(*a, **k):
        raise RateLimited("slow down")
    fake_client.list_pipelines = boom
    c = build(seeded, fake_client)
    r = c.post("/sync", follow_redirects=True)
    assert r.status_code == 200
    assert "Sync finished with errors" in r.text and "slow down" in r.text


def test_sync_notice_is_shown_once_then_cleared(seeded, fake_client):
    def boom(*a, **k):
        raise RuntimeError("kaboom")
    fake_client.search_modified_since = boom
    c = build(seeded, fake_client)
    assert "Sync failed" in c.post("/sync", follow_redirects=True).text
    assert "Sync failed" not in c.get("/").text


def test_sync_crash_does_not_500(seeded, fake_client):
    def boom(*a, **k):
        raise RuntimeError("kaboom")
    fake_client.search_modified_since = boom
    c = build(seeded, fake_client)
    r = c.post("/sync", follow_redirects=True)
    assert r.status_code == 200 and "Sync failed" in r.text


def test_sync_button_on_a_never_backfilled_database_walks_nothing(conn, fake_client):
    c = build(conn, fake_client)
    r = c.post("/sync", follow_redirects=True)
    assert r.status_code == 200
    assert fake_client.search_calls == []
    assert "never backfilled" in r.text


def test_sync_button_shares_its_lock_with_the_timer(seeded, fake_client):
    from crm.sync import SYNC_LOCK
    c = build(seeded, fake_client)
    with SYNC_LOCK:
        r = c.post("/sync", follow_redirects=True)
    assert "already running" in r.text
    assert fake_client.search_calls == []


# --- same-origin guard -------------------------------------------------------------

POSTS = [("/sync", {}), ("/ask", {"question": "x"}),
         ("/snooze", {"signal_id": "stalled_deal", "record_id": "d1", "days": 7})]


def test_cross_origin_posts_are_refused(client, fake_client):
    for url, data in POSTS:
        for header in ({"Origin": "http://evil.example"},
                       {"Origin": "null"},
                       {"Referer": "http://evil.example/page"}):
            r = client.post(url, data=data, headers=header, follow_redirects=False)
            assert r.status_code == 403, (url, header)
    assert not fake_client.search_calls
    assert "Acme renewal" in client.get("/signal/stalled_deal").text   # not snoozed


def test_same_origin_posts_are_allowed(client):
    for url, data in POSTS:
        r = client.post(url, data=data, follow_redirects=False,
                        headers={"Origin": "http://testserver",
                                 "Referer": "http://testserver/"})
        assert r.status_code in (200, 303), url


# --- more escaping, and the engagement failure reasons -------------------------------

def test_owner_stage_and_company_names_are_escaped(seeded, fake_client):
    evil = "<img src=x onerror=alert(1)>"
    with seeded:
        seeded.execute("UPDATE owners SET first_name = ?, last_name = ''", (evil,))
        seeded.execute("UPDATE stages SET label = ?", (evil,))
        seeded.execute("UPDATE companies SET name = ?", (evil,))
        seeded.execute("UPDATE pipelines SET label = ?", (evil,))
    c = build(seeded, fake_client)
    urls = ["/", "/?owner=o1", "/signal/stalled_deal", "/signal/untouched_account",
            "/record/deals/d1", "/record/companies/c1"]
    for url in urls:
        body = c.get(url).text
        assert evil not in body, url
    assert "&lt;img src=x onerror=alert(1)&gt;" in c.get("/").text
    assert "&lt;img" in c.get("/record/deals/d1").text


def test_engagement_failure_reason_distinguishes_a_bad_token(client, fake_client, caplog):
    def denied(*a, **k):
        request = httpx.Request("POST", "https://api.hubapi.com/x")
        raise httpx.HTTPStatusError("401", request=request,
                                    response=httpx.Response(401, request=request))
    fake_client.batch_associations = denied
    with caplog.at_level("WARNING", logger="crm.web"):
        body = client.get("/record/deals/d1").text
    assert "rejected the token" in body
    assert "HTTPStatusError" in caplog.text        # not swallowed silently
