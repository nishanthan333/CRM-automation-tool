import json
from pathlib import Path

import httpx
import pytest

from crm.hubspot import MODIFIED_PROPERTY, HubSpotClient, Page, RateLimited

FIXTURES = Path(__file__).parent / "fixtures" / "hubspot"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def make_client(handler, sleeps=None, **kwargs):
    """Client wired to a MockTransport; every request lands in `requests`."""
    requests: list[httpx.Request] = []

    def recording(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    http = httpx.Client(transport=httpx.MockTransport(recording))
    sleeps = sleeps if sleeps is not None else []
    client = HubSpotClient("pat-na1-abc", http=http, sleep=sleeps.append, **kwargs)
    return client, requests


def deals_handler(request: httpx.Request) -> httpx.Response:
    if request.url.params.get("after") == "NTI1Cg%3D%3D":
        return httpx.Response(200, json=fixture("deals_page2.json"))
    return httpx.Response(200, json=fixture("deals_page1.json"))


def test_auth_header_is_bearer_token():
    client, requests = make_client(deals_handler)
    client.list_page("deals", after=None)
    assert requests[0].headers["Authorization"] == "Bearer pat-na1-abc"


def test_list_page_returns_records_and_cursor():
    client, requests = make_client(deals_handler)
    page = client.list_page("deals", after=None)
    assert isinstance(page, Page)
    assert page.after == "NTI1Cg%3D%3D"
    assert page.records[0]["id"] == "4001"
    assert requests[0].method == "GET"
    assert requests[0].url.path == "/crm/v3/objects/deals"
    assert "after" not in requests[0].url.params
    assert requests[0].url.params["archived"] == "false"


def test_list_page_requests_exactly_the_schema_properties():
    client, requests = make_client(deals_handler)
    client.list_page("deals", after=None)
    props = requests[0].url.params["properties"].split(",")
    assert props == [
        "dealname", "dealstage", "pipeline", "amount_in_home_currency",
        "closedate", "hubspot_owner_id", "hs_last_activity_date",
        "hs_lastmodifieddate", "createdate",
    ]


def test_list_page_sends_cursor_and_archived_flag():
    client, requests = make_client(deals_handler)
    client.list_page("deals", after="NTI1Cg%3D%3D", archived=True)
    assert requests[0].url.params["after"] == "NTI1Cg%3D%3D"
    assert requests[0].url.params["archived"] == "true"


def test_final_page_has_no_cursor():
    client, _ = make_client(deals_handler)
    page = client.list_page("deals", after="NTI1Cg%3D%3D")
    assert page.after is None
    assert [r["id"] for r in page.records] == ["4003"]


def test_list_ids_page_omits_properties():
    client, requests = make_client(deals_handler)
    page = client.list_ids_page("deals", after=None)
    assert "properties" not in requests[0].url.params
    assert requests[0].url.path == "/crm/v3/objects/deals"
    assert page.after == "NTI1Cg%3D%3D"


def test_owners_use_their_own_path():
    client, requests = make_client(
        lambda r: httpx.Response(200, json=fixture("owners_page.json"))
    )
    page = client.list_page("owners", after=None)
    assert requests[0].url.path == "/crm/v3/owners"
    assert page.records[0]["email"] == "sam@example.com"


def test_modified_property_differs_for_contacts():
    assert MODIFIED_PROPERTY["contacts"] == "lastmodifieddate"
    assert MODIFIED_PROPERTY["deals"] == "hs_lastmodifieddate"
    assert MODIFIED_PROPERTY["companies"] == "hs_lastmodifieddate"
    assert MODIFIED_PROPERTY["engagements"] == "hs_lastmodifieddate"


def test_search_filters_on_modified_property_in_epoch_ms():
    client, requests = make_client(
        lambda r: httpx.Response(200, json=fixture("contacts_search.json"))
    )
    page = client.search_modified_since("contacts", 1760000000000, None)
    req = requests[0]
    assert req.method == "POST"
    assert req.url.path == "/crm/v3/objects/contacts/search"
    body = json.loads(req.content)
    assert body["filterGroups"] == [{"filters": [{
        "propertyName": "lastmodifieddate",
        "operator": "GT",
        "value": "1760000000000",
    }]}]
    assert body["sorts"] == [{"propertyName": "lastmodifieddate", "direction": "ASCENDING"}]
    assert "lastmodifieddate" in body["properties"]
    assert "after" not in body
    assert page.after == "100"
    assert page.records[0]["id"] == "9001"


def test_search_sends_cursor():
    client, requests = make_client(
        lambda r: httpx.Response(200, json=fixture("contacts_search.json"))
    )
    client.search_modified_since("contacts", 1, "100")
    assert json.loads(requests[0].content)["after"] == "100"


def test_429_is_retried_with_backoff_then_succeeds():
    responses = iter([
        httpx.Response(429, json=fixture("rate_limited_429.json")),
        httpx.Response(429, json=fixture("rate_limited_429.json")),
        httpx.Response(200, json=fixture("deals_page1.json")),
    ])
    sleeps: list[float] = []
    client, requests = make_client(lambda r: next(responses), sleeps)
    page = client.list_page("deals", after="NTI1Cg%3D%3D")
    assert len(requests) == 3
    assert page.records[0]["id"] == "4001"
    assert sleeps == sorted(sleeps) and len(sleeps) == 2
    assert sleeps[0] < sleeps[1]
    # retries resend the same cursor: resume, not restart
    assert {r.url.params["after"] for r in requests} == {"NTI1Cg%3D%3D"}


def test_5xx_is_retried():
    responses = iter([httpx.Response(503), httpx.Response(200, json=fixture("deals_page2.json"))])
    client, requests = make_client(lambda r: next(responses))
    assert client.list_page("deals", after=None).records[0]["id"] == "4003"
    assert len(requests) == 2


def test_429_gives_up_after_max_attempts_and_raises_rate_limited():
    sleeps: list[float] = []
    client, requests = make_client(
        lambda r: httpx.Response(429, json=fixture("rate_limited_429.json")),
        sleeps,
        max_attempts=4,
    )
    with pytest.raises(RateLimited):
        client.list_page("deals", after=None)
    assert len(requests) == 4
    assert len(sleeps) == 3  # no sleep after the final failed attempt


def test_non_retryable_error_raises_immediately():
    client, requests = make_client(lambda r: httpx.Response(401, json={"message": "bad"}))
    with pytest.raises(httpx.HTTPStatusError):
        client.list_page("deals", after=None)
    assert len(requests) == 1


def test_batch_associations_chunks_at_100_ids():
    def handler(request):
        ids = [i["id"] for i in json.loads(request.content)["inputs"]]
        return httpx.Response(200, json={"status": "COMPLETE", "results": [
            {"from": {"id": i}, "to": [{"toObjectId": int(i) + 1000}]} for i in ids
        ]})

    client, requests = make_client(handler)
    out = client.batch_associations("deals", "companies", [str(i) for i in range(250)])
    assert len(requests) == 3
    assert [len(json.loads(r.content)["inputs"]) for r in requests] == [100, 100, 50]
    assert requests[0].url.path == "/crm/v4/associations/deals/companies/batch/read"
    assert len(out) == 250
    assert out["7"] == ["1007"]


def test_batch_associations_parses_fixture_and_fills_missing_ids():
    client, _ = make_client(lambda r: httpx.Response(200, json=fixture("associations_batch.json")))
    out = client.batch_associations("deals", "companies", ["4001", "4002", "4999"])
    assert out == {"4001": ["7001", "7002"], "4002": ["7003"], "4999": []}


def test_batch_associations_empty_input_makes_no_request():
    client, requests = make_client(lambda r: httpx.Response(500))
    assert client.batch_associations("deals", "companies", []) == {}
    assert requests == []


def test_list_pipelines_returns_results():
    client, requests = make_client(lambda r: httpx.Response(200, json=fixture("pipelines.json")))
    pipelines = client.list_pipelines("deals")
    assert requests[0].url.path == "/crm/v3/pipelines/deals"
    assert pipelines[0]["id"] == "default"
    assert pipelines[0]["stages"][1]["metadata"]["isClosed"] == "true"


def test_client_never_issues_write_methods():
    def handler(request):
        path = request.url.path
        if path.endswith("/batch/read"):
            return httpx.Response(200, json=fixture("associations_batch.json"))
        if path.endswith("/search"):
            return httpx.Response(200, json=fixture("contacts_search.json"))
        if "/pipelines/" in path:
            return httpx.Response(200, json=fixture("pipelines.json"))
        return httpx.Response(200, json=fixture("deals_page2.json"))

    client, requests = make_client(handler)
    client.list_page("deals", after=None)
    client.list_ids_page("contacts", after=None)
    client.list_page("owners", after=None)
    client.search_modified_since("contacts", 1, None)
    client.batch_associations("deals", "companies", ["4001"])
    client.list_pipelines("deals")

    # Guard against a vacuous pass: every public path really was exercised.
    assert {r.method for r in requests} == {"GET", "POST"}
    assert len(requests) == 6
    assert {r.method for r in requests} <= {"GET", "POST"}
    assert all(
        "/search" in str(r.url) or "/batch/read" in str(r.url)
        for r in requests if r.method == "POST"
    )
