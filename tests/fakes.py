"""Shared in-memory fake of `crm.hubspot.HubSpotClient`.

No test may make a live network call, and the sync layer is tested against this
fake rather than a mock transport: the HTTP details are Task 2's concern and are
covered in `tests/test_hubspot.py`.

Tests set data up on the fake and then assert against the recorded calls:

    client = FakeClient()
    client.set_pages("deals", [FakePage(records=[...], after="page-2-cursor"),
                               FakePage(records=[...])])
    client.fail_on_page = 2          # 2nd request for a type raises RateLimited

    client.search_calls   -> [Call(kind="search", object_type=..., after=...,
                                   since_ms=..., archived=...)]
    client.list_calls     -> list_page() calls (with .archived)
    client.ids_only_calls -> list_ids_page() calls
    client.assoc_calls    -> [Call(kind="assoc", from_type=..., to_type=...,
                                   ids=[...])]
    client.count_calls    -> [object_type, ...] for count() calls
    client.pipeline_calls -> int, number of list_pipelines() calls
"""
from dataclasses import dataclass, field

from crm.hubspot import Page, RateLimited


@dataclass
class Call:
    """One recorded client call. Unused fields stay at their defaults."""
    kind: str
    object_type: str | None = None
    after: str | None = None
    since_ms: int | None = None
    archived: bool = False
    from_type: str | None = None
    to_type: str | None = None
    ids: list[str] = field(default_factory=list)


@dataclass
class FakePage:
    """One page of results. `after` is the cursor for the page that follows."""
    records: list[dict]
    after: str | None = None


class FakeClient:
    def __init__(self) -> None:
        # object_type -> list[FakePage] | list[list[dict]] | int (page count)
        self.pages: dict[str, object] = {}
        self.archived: dict[str, object] = {}       # archived=True listings
        self.remote_ids: dict[str, list[str]] = {}  # list_ids_page results
        self.pipelines: list[dict] = []
        self.counts: dict[str, int] = {}            # count() results
        self.count_calls: list[str] = []
        # (from_type, to_type) -> {from_id: [to_id, ...]}
        self.associations: dict[tuple[str, str], dict[str, list[str]]] = {}
        self.fail_on_page: int | None = None
        # None serves the whole id set as one page; an int splits it.
        self.ids_page_size: int | None = None
        self.search_calls: list[Call] = []
        self.list_calls: list[Call] = []
        self.ids_only_calls: list[Call] = []
        self.assoc_calls: list[Call] = []
        self.pipeline_calls = 0
        self._requests: dict[str, int] = {}

    # -- setup -----------------------------------------------------------
    def set_pages(self, object_type: str, pages) -> None:
        self.pages[object_type] = pages

    def reset_calls(self) -> None:
        self.search_calls.clear()
        self.list_calls.clear()
        self.ids_only_calls.clear()
        self.assoc_calls.clear()
        self.pipeline_calls = 0
        self.count_calls.clear()
        self._requests.clear()

    # -- paging ----------------------------------------------------------
    @staticmethod
    def _normalise(spec, object_type: str) -> list[FakePage]:
        """Accept explicit pages, bare record lists, or a page count."""
        if spec is None:
            return [FakePage(records=[])]
        if isinstance(spec, int):
            spec = [[{"id": f"{object_type}-{p}-{i}", "properties": {}}
                     for i in range(2)] for p in range(spec)]
        if spec and all(isinstance(r, dict) for r in spec):
            spec = [spec]   # a bare record list is one page
        pages = [p if isinstance(p, FakePage) else FakePage(records=list(p))
                 for p in spec]
        if not pages:
            return [FakePage(records=[])]
        for n, page in enumerate(pages[:-1]):
            if page.after is None:
                page.after = f"{object_type}-page-{n + 2}-cursor"
        pages[-1].after = None
        return pages

    def _serve(self, source: dict, object_type: str, after: str | None) -> Page:
        """Return the page that follows cursor `after`, honouring fail_on_page."""
        self._requests[object_type] = self._requests.get(object_type, 0) + 1
        if self.fail_on_page is not None and self._requests[object_type] == self.fail_on_page:
            raise RateLimited(f"fake 429 on page {self.fail_on_page} of {object_type}")
        pages = self._normalise(source.get(object_type), object_type)
        if after is None:
            index = 0
        else:
            prior = [n for n, p in enumerate(pages) if p.after == after]
            if not prior:
                raise AssertionError(
                    f"{object_type}: no page follows cursor {after!r}")
            index = prior[0] + 1
        page = pages[index]
        return Page(records=list(page.records), after=page.after)

    # -- client surface --------------------------------------------------
    def search_modified_since(self, object_type: str, since_ms: int,
                              after: str | None) -> Page:
        self.search_calls.append(Call(
            kind="search", object_type=object_type, after=after, since_ms=since_ms))
        return self._serve(self.pages, object_type, after)

    def list_page(self, object_type: str, after: str | None,
                  archived: bool = False) -> Page:
        self.list_calls.append(Call(
            kind="list", object_type=object_type, after=after, archived=archived))
        source = self.archived if archived else self.pages
        return self._serve(source, object_type, after)

    def list_ids_page(self, object_type: str, after: str | None) -> Page:
        self.ids_only_calls.append(Call(
            kind="ids", object_type=object_type, after=after))
        key = f"ids:{object_type}"
        self._requests[key] = self._requests.get(key, 0) + 1
        if self.fail_on_page is not None and self._requests[key] == self.fail_on_page:
            raise RateLimited(f"fake 429 on id page {self.fail_on_page} of {object_type}")
        ids = [str(i) for i in self.remote_ids.get(object_type, [])]
        size = self.ids_page_size or max(len(ids), 1)
        start = 0 if after is None else int(after.rsplit("-", 1)[1])
        end = start + size
        return Page(records=[{"id": i} for i in ids[start:end]],
                    after=f"ids-{end}" if end < len(ids) else None)

    def batch_associations(self, from_type: str, to_type: str,
                           ids: list[str]) -> dict[str, list[str]]:
        self.assoc_calls.append(Call(
            kind="assoc", from_type=from_type, to_type=to_type, ids=list(ids)))
        links = self.associations.get((from_type, to_type), {})
        return {i: list(links.get(i, [])) for i in ids}

    def list_pipelines(self, object_type: str) -> list[dict]:
        self.pipeline_calls += 1
        return [dict(p) for p in self.pipelines]

    def count(self, object_type: str) -> int:
        self.count_calls.append(object_type)
        return self.counts.get(object_type, 0)
