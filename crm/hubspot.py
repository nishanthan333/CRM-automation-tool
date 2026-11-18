"""Read-only HubSpot client. The only module that talks to HubSpot."""
import random
import time
from dataclasses import dataclass
from typing import Callable

import httpx

__all__ = ["HubSpotClient", "MODIFIED_PROPERTY", "Page", "RateLimited"]

BASE_URL = "https://api.hubapi.com"
PAGE_SIZE = 100
ASSOC_CHUNK = 100

# Must match the columns in crm/db.py.
PROPERTIES: dict[str, list[str]] = {
    "deals": [
        "dealname", "dealstage", "pipeline", "amount_in_home_currency",
        "closedate", "hubspot_owner_id", "hs_last_activity_date",
        "hs_lastmodifieddate", "createdate",
    ],
    "companies": [
        "name", "domain", "hubspot_owner_id", "hs_last_activity_date",
        "hs_lastmodifieddate",
    ],
    "contacts": [
        "firstname", "lastname", "email", "hubspot_owner_id",
        "hs_last_activity_date", "lastmodifieddate",
    ],
    "engagements": ["type", "timestamp", "hs_lastmodifieddate"],
    # Owners carry email/firstName/lastName as top-level fields, not properties.
    "owners": [],
}

MODIFIED_PROPERTY: dict[str, str] = {
    "deals": "hs_lastmodifieddate",
    "companies": "hs_lastmodifieddate",
    "contacts": "lastmodifieddate",
    "engagements": "hs_lastmodifieddate",
}

# Owners are not CRM objects; everything else lives under /crm/v3/objects.
_LIST_PATH = {"owners": "/crm/v3/owners"}


class RateLimited(Exception):
    """Retries exhausted on 429/5xx. Callers resume from their saved cursor."""


@dataclass
class Page:
    records: list[dict]
    after: str | None


class HubSpotClient:
    def __init__(
        self,
        token: str,
        http: httpx.Client | None = None,
        *,
        max_attempts: int = 6,
        base_delay: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._token = token
        self._http = http or httpx.Client(base_url=BASE_URL, timeout=30.0)
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._sleep = sleep

    def _request(self, method: str, path: str, **kwargs) -> dict:
        headers = {"Authorization": f"Bearer {self._token}"}
        for attempt in range(self._max_attempts):
            resp = self._http.request(method, BASE_URL + path, headers=headers, **kwargs)
            if resp.status_code != 429 and resp.status_code < 500:
                resp.raise_for_status()
                return resp.json()
            if attempt < self._max_attempts - 1:
                delay = self._base_delay * 2**attempt
                self._sleep(delay + random.uniform(0, self._base_delay))
        raise RateLimited(
            f"{method} {path}: HTTP {resp.status_code} after {self._max_attempts} attempts"
        )

    @staticmethod
    def _page(data: dict) -> Page:
        after = data.get("paging", {}).get("next", {}).get("after")
        return Page(records=data.get("results", []), after=after)

    def _list(self, object_type: str, after: str | None, archived: bool, properties: list[str]) -> Page:
        params: dict[str, str | int] = {"limit": PAGE_SIZE, "archived": str(archived).lower()}
        if properties:
            params["properties"] = ",".join(properties)
        if after is not None:
            params["after"] = after
        path = _LIST_PATH.get(object_type, f"/crm/v3/objects/{object_type}")
        return self._page(self._request("GET", path, params=params))

    def list_page(self, object_type: str, after: str | None, archived: bool = False) -> Page:
        return self._list(object_type, after, archived, PROPERTIES[object_type])

    def list_ids_page(self, object_type: str, after: str | None) -> Page:
        return self._list(object_type, after, False, [])

    def search_modified_since(self, object_type: str, since_ms: int, after: str | None) -> Page:
        prop = MODIFIED_PROPERTY[object_type]
        body: dict = {
            "filterGroups": [{"filters": [
                {"propertyName": prop, "operator": "GT", "value": str(since_ms)}
            ]}],
            "sorts": [{"propertyName": prop, "direction": "ASCENDING"}],
            "properties": PROPERTIES[object_type],
            "limit": PAGE_SIZE,
        }
        if after is not None:
            body["after"] = after
        return self._page(
            self._request("POST", f"/crm/v3/objects/{object_type}/search", json=body)
        )

    def batch_associations(self, from_type: str, to_type: str, ids: list[str]) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {i: [] for i in ids}
        path = f"/crm/v4/associations/{from_type}/{to_type}/batch/read"
        for start in range(0, len(ids), ASSOC_CHUNK):
            chunk = ids[start:start + ASSOC_CHUNK]
            data = self._request("POST", path, json={"inputs": [{"id": i} for i in chunk]})
            for row in data.get("results", []):
                out[str(row["from"]["id"])] = [str(t["toObjectId"]) for t in row.get("to", [])]
        return out

    def list_pipelines(self, object_type: str) -> list[dict]:
        return self._request("GET", f"/crm/v3/pipelines/{object_type}").get("results", [])
