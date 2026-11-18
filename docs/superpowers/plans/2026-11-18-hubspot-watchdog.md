# HubSpot Watchdog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A local, read-only tool that syncs one HubSpot account into SQLite, materialises rule-based signals over it, and serves a per-owner morning briefing plus a question box answered by a local model.

**Architecture:** One Python process. `hubspot.py` talks to the API; `sync.py` drives backfill, incremental and reconcile passes into SQLite via `db.py`, checkpointing a page cursor as it goes; `signals.py` evaluates SQL rules into a materialised `signal_results` table at the end of every sync; `web.py` serves a server-rendered FastAPI dashboard that only ever reads materialised results. `ask.py` sends the schema (never data) to Ollama, guards the returned SQL, and runs it read-only.

**Tech Stack:** Python 3.12+, SQLite (stdlib `sqlite3`), `httpx`, FastAPI, Jinja2, uvicorn, pytest. Config via stdlib `tomllib`. Ollama over its HTTP API — no SDK.

**Spec:** `docs/superpowers/specs/2026-11-18-hubspot-watchdog-design.md`

## Global Constraints

- Python 3.12+. SQLite via the standard library.
- **The tool never writes to HubSpot.** No `POST`/`PATCH`/`PUT`/`DELETE` to any CRM object endpoint, in any task.
- **No CRM data leaves the machine.** The only outbound network calls are to HubSpot and to a local Ollama. The Ollama prompt carries table and column names only — never row data.
- **Activity means `hs_last_activity_date`** on the record. No signal joins against `engagements`.
- **Deal value is `amount_in_home_currency`**, never `amount`.
- **Every signal query filters `archived = 0`.**
- Modified-date property is `lastmodifieddate` for contacts and `hs_lastmodifieddate` for every other object. All HubSpot timestamps are epoch milliseconds, UTC.
- Date bucketing uses the machine's local timezone; weeks start **Monday**; quarters are **calendar** quarters. All three read from `config.toml`.
- The search endpoint caps at ~10,000 results and is **never** used for backfill.
- A `429` triggers exponential backoff and resumes from the checkpointed cursor.
- Signal thresholds live in `signals.toml`, never in code.
- Record views present fields under HubSpot's own property labels.

## Review Focus

Five conditions the spec implies that no task's happy path exercises. Each has a test added to the task that owns the code.

1. **A fresh database with no backfill yet** — the dashboard must render an empty briefing and say so, not raise. (Task 9)
2. **Ollama not running** — the question box reports it plainly; every other part of the page works. (Task 8)
3. **Deals with null `amount_in_home_currency`, `closedate` or `hubspot_owner_id`** — signals must still flag the record and must not sum nulls into a total. (Task 6)
4. **A `429` arriving mid-page during backfill** — resume must neither skip nor double-count records. (Task 4)
5. **The model returning prose, a code fence, or two statements instead of one `SELECT`** — the guard rejects it and the page shows what was returned. (Task 8)

---

### Task 1: Config and database schema

**Files:**
- Create: `crm/config.py`, `crm/db.py`, `config.toml.example`, `signals.toml`, `.gitignore`, `pyproject.toml`
- Test: `tests/test_config.py`, `tests/test_db.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `load_config(path: Path) -> Config` (dataclass with `hubspot_token: str`, `db_path: Path`, `timezone: str`, `week_starts_on: str`, `ollama_model: str`, `ollama_url: str`, `sync_interval_minutes: int`); `load_thresholds(path: Path) -> dict[str, dict[str, float]]`; `connect(db_path: Path) -> sqlite3.Connection`; `connect_readonly(db_path: Path) -> sqlite3.Connection`; `migrate(conn) -> None`; `upsert(conn, table: str, rows: list[dict]) -> int`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_config.py
def test_load_config_reads_token_and_defaults(tmp_path):
    (tmp_path / "config.toml").write_text(
        '[hubspot]\ntoken = "pat-na1-abc"\n'
    )
    cfg = load_config(tmp_path / "config.toml")
    assert cfg.hubspot_token == "pat-na1-abc"
    assert cfg.week_starts_on == "monday"          # spec default
    assert cfg.sync_interval_minutes == 15

def test_load_config_rejects_missing_token(tmp_path):
    (tmp_path / "config.toml").write_text("[hubspot]\n")
    with pytest.raises(ValueError, match="token"):
        load_config(tmp_path / "config.toml")

# tests/test_db.py
def test_migrate_creates_all_tables(tmp_path):
    conn = connect(tmp_path / "t.db"); migrate(conn)
    names = {r[0] for r in conn.execute(
        "select name from sqlite_master where type='table'")}
    assert {"contacts", "companies", "deals", "engagements", "owners",
            "associations", "sync_state", "pipelines", "stages",
            "signal_results", "snoozes"} <= names

def test_migrate_is_idempotent(tmp_path):
    conn = connect(tmp_path / "t.db"); migrate(conn); migrate(conn)

def test_required_indexes_exist(tmp_path):
    conn = connect(tmp_path / "t.db"); migrate(conn)
    sql = " ".join(r[0] or "" for r in conn.execute(
        "select sql from sqlite_master where type='index'"))
    for col in ("hubspot_owner_id", "hs_last_activity_date", "dealstage",
                "closedate", "archived", "amount_in_home_currency"):
        assert col in sql

def test_upsert_replaces_on_id_and_preserves_raw(tmp_path):
    conn = connect(tmp_path / "t.db"); migrate(conn)
    upsert(conn, "deals", [{"id": "1", "dealname": "A", "raw": '{"x":1}'}])
    upsert(conn, "deals", [{"id": "1", "dealname": "B", "raw": '{"x":2}'}])
    row = conn.execute("select dealname, raw from deals").fetchone()
    assert row == ("B", '{"x":2}')
    assert conn.execute("select count(*) from deals").fetchone()[0] == 1

def test_readonly_connection_rejects_writes(tmp_path):
    p = tmp_path / "t.db"; migrate(connect(p))
    with pytest.raises(sqlite3.OperationalError):
        connect_readonly(p).execute("delete from deals")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_config.py tests/test_db.py -v`
Expected: FAIL — `ModuleNotFoundError: crm.config`

- [ ] **Step 3: Implement `crm/config.py` and `crm/db.py`**

Schema, with `id TEXT PRIMARY KEY`, `raw TEXT`, `archived INTEGER NOT NULL DEFAULT 0` on every object table, plus the typed columns the signals need:

- `deals`: `dealname`, `dealstage`, `pipeline`, `amount_in_home_currency REAL`, `closedate INTEGER`, `hubspot_owner_id`, `hs_last_activity_date INTEGER`, `hs_lastmodifieddate INTEGER`, `createdate INTEGER`
- `companies`: `name`, `domain`, `hubspot_owner_id`, `hs_last_activity_date INTEGER`, `hs_lastmodifieddate INTEGER`
- `contacts`: `firstname`, `lastname`, `email`, `hubspot_owner_id`, `hs_last_activity_date INTEGER`, `lastmodifieddate INTEGER`
- `engagements`: `type`, `timestamp INTEGER`, `hs_lastmodifieddate INTEGER`
- `owners`: `email`, `first_name`, `last_name`
- `pipelines`: `id`, `label`; `stages`: `id`, `pipeline_id`, `label`, `display_order INTEGER`, `is_closed INTEGER`, `probability REAL`
- `associations(from_type, from_id, to_type, to_id)`, primary key over all four
- `sync_state(object_type PRIMARY KEY, last_modified_ms INTEGER, cursor TEXT, phase TEXT, last_run_at INTEGER, status TEXT)`
- `signal_results(signal_id, record_id, record_type, owner_id, value REAL, computed_at INTEGER)`
- `snoozes(signal_id, record_id, until INTEGER, created_at INTEGER, fingerprint TEXT)`, primary key `(signal_id, record_id)` — `fingerprint` is what Task 7 compares to decide a snooze is stale

`connect` sets `PRAGMA journal_mode=WAL` and `foreign_keys=ON`. `connect_readonly` opens via `file:...?mode=ro` with `uri=True`. `upsert` builds one parameterised `INSERT ... ON CONFLICT(id) DO UPDATE` per table and executes it with `executemany` inside a transaction.

`signals.toml` ships the spec's default thresholds: `stalled_days = 14`, `big_deal_quiet_days = 7`, `big_deal_value = 50000`, `untouched_account_days = 30`, `quiet_pipeline_drop_pct = 40`.

`.gitignore`: `config.toml`, `data/`, `__pycache__/`, `.venv/`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py tests/test_db.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/config.py crm/db.py tests/test_config.py tests/test_db.py \
        config.toml.example signals.toml .gitignore pyproject.toml
git commit -m "feat: config loading and SQLite schema"
```

---

### Task 2: HubSpot API client

**Files:**
- Create: `crm/hubspot.py`
- Test: `tests/test_hubspot.py`, `tests/fixtures/hubspot/*.json`

**Interfaces:**
- Consumes: `Config` from Task 1.
- Produces: `HubSpotClient(token: str, http: httpx.Client | None = None)` with `list_page(object_type: str, after: str | None, archived: bool = False) -> Page`, `search_modified_since(object_type: str, since_ms: int, after: str | None) -> Page`, `batch_associations(from_type: str, to_type: str, ids: list[str]) -> dict[str, list[str]]`, `list_pipelines(object_type: str) -> list[dict]`, `list_ids_page(object_type: str, after: str | None) -> Page`, and `MODIFIED_PROPERTY: dict[str, str]`. `Page` is a dataclass of `records: list[dict]` and `after: str | None`.

- [ ] **Step 1: Write the failing tests**

Record fixtures by hand as trimmed real-shaped payloads: a deals list page with an `after` cursor, a final page without one, a search response, a batch-associations response, a pipelines response, and a `429` body. Drive the client with `httpx.MockTransport`.

```python
def test_auth_header_is_bearer_token():
    # assert the recorded request carried Authorization: Bearer pat-na1-abc

def test_list_page_returns_records_and_cursor():
    page = client.list_page("deals", after=None)
    assert page.after == "NTI1Cg%3D%3D"
    assert page.records[0]["id"] == "4001"

def test_final_page_has_no_cursor():
    assert client.list_page("deals", after="NTI1Cg%3D%3D").after is None

def test_modified_property_differs_for_contacts():
    assert MODIFIED_PROPERTY["contacts"] == "lastmodifieddate"
    assert MODIFIED_PROPERTY["deals"] == "hs_lastmodifieddate"

def test_search_filters_on_modified_property_in_epoch_ms():
    client.search_modified_since("contacts", 1760000000000, None)
    # assert posted body filters propertyName == "lastmodifieddate",
    # operator "GT", value "1760000000000"

def test_429_is_retried_with_backoff_then_succeeds(monkeypatch):
    # transport yields 429, 429, 200; assert 3 attempts, result returned,
    # and sleep called with increasing values
    assert sleeps == sorted(sleeps) and len(sleeps) == 2

def test_429_gives_up_after_max_attempts_and_raises_rate_limited():
    with pytest.raises(RateLimited):
        ...

def test_batch_associations_chunks_at_100_ids():
    client.batch_associations("deals", "companies", [str(i) for i in range(250)])
    assert len(requests) == 3

def test_client_never_issues_write_methods():
    assert {r.method for r in requests} <= {"GET", "POST"}
    assert all("/search" in str(r.url) or "/batch/read" in str(r.url)
               for r in requests if r.method == "POST")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_hubspot.py -v`
Expected: FAIL — `ModuleNotFoundError: crm.hubspot`

- [ ] **Step 3: Implement `crm/hubspot.py`**

One private `_request` method carrying the bearer header and the retry loop: on `429` or `5xx`, sleep `base * 2**attempt` with jitter up to a configurable max attempts, then raise `RateLimited`. Paths: `/crm/v3/objects/{type}` for lists, `/crm/v3/objects/{type}/search` for deltas, `/crm/v4/associations/{from}/{to}/batch/read` for associations, `/crm/v3/pipelines/{type}` for pipelines. `list_ids_page` is `list_page` with `properties` omitted, for the reconcile pass. Each object type requests exactly the properties Task 1's schema declares.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_hubspot.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/hubspot.py tests/test_hubspot.py tests/fixtures/
git commit -m "feat: HubSpot client with paging and 429 backoff"
```

---

### Task 3: Incremental sync

**Files:**
- Create: `crm/sync.py`
- Test: `tests/test_sync_incremental.py`

**Interfaces:**
- Consumes: `HubSpotClient` (Task 2), `connect`/`upsert`/`migrate` (Task 1).
- Produces: `sync_incremental(conn, client, object_types: list[str] | None = None) -> SyncReport`; `refresh_pipelines(conn, client) -> None`; `refresh_associations(conn, client, object_type: str, ids: list[str]) -> None`; `WATERMARK_REWIND_MS = 60_000`; `SyncReport` dataclass with `per_type: dict[str, int]` and `errors: list[str]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_search_uses_stored_watermark_rewound_by_60s(seeded_conn, fake_client):
    set_watermark(seeded_conn, "deals", 1_700_000_000_000)
    sync_incremental(seeded_conn, fake_client, ["deals"])
    assert fake_client.search_calls[0].since_ms == 1_700_000_000_000 - 60_000

def test_watermark_advances_to_max_modified_only_after_type_completes(...):
    # two pages; assert watermark unchanged after page 1, set after page 2

def test_crash_midway_leaves_watermark_unmoved_and_cursor_checkpointed(...):
    fake_client.fail_on_page = 2
    with pytest.raises(RateLimited):
        sync_incremental(conn, fake_client, ["deals"])
    assert watermark(conn, "deals") == 1_700_000_000_000
    assert cursor(conn, "deals") == "page-2-cursor"

def test_rerun_after_crash_resumes_from_checkpointed_cursor(...):
    assert fake_client.search_calls[-1].after == "page-2-cursor"

def test_pipelines_refreshed_on_every_run(...):
    assert fake_client.pipeline_calls == 1

def test_associations_refetched_only_for_changed_ids(...):
    assert set(fake_client.assoc_calls[0].ids) == {"4001", "4002"}

def test_associations_rewritten_not_appended(...):
    # a deal previously linked to company 9 and now to 10 ends with one row
    assert rows == [("deals", "4001", "companies", "10")]

def test_engagement_associations_not_bulk_synced(...):
    assert all(c.from_type != "engagements" for c in fake_client.assoc_calls)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sync_incremental.py -v`
Expected: FAIL — `cannot import name 'sync_incremental'`

- [ ] **Step 3: Implement `sync_incremental`, `refresh_pipelines` and `refresh_associations`**

Per object type: read `last_modified_ms` and `cursor` from `sync_state`; search from `watermark - WATERMARK_REWIND_MS`, resuming at `cursor` if set; upsert each page and write the new cursor in the same transaction; track the max modified value seen. On exhaustion, clear the cursor and set the watermark in one transaction. Then `refresh_associations` for the ids seen, over the pairs deal↔company, deal↔contact, company↔contact only — deleting a record's existing rows for that pair before inserting the new set. `refresh_pipelines` runs first, every call.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sync_incremental.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/sync.py tests/test_sync_incremental.py
git commit -m "feat: incremental sync with cursor checkpointing"
```

---

### Task 4: Backfill

**Files:**
- Modify: `crm/sync.py`
- Test: `tests/test_sync_backfill.py`

**Interfaces:**
- Consumes: everything from Task 3.
- Produces: `backfill(conn, client, progress: Callable[[str, int], None] | None = None) -> SyncReport`; `estimate_backfill(conn, client) -> list[Estimate]`; `BACKFILL_ORDER: list[str]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_backfill_order_puts_briefing_objects_before_engagements():
    assert BACKFILL_ORDER == ["owners", "pipelines", "companies", "deals",
                              "contacts", "engagements"]

def test_backfill_uses_list_endpoint_never_search(fake_client):
    backfill(conn, fake_client)
    assert fake_client.search_calls == []

def test_cursor_written_after_every_page(fake_client):
    fake_client.pages["deals"] = 3
    backfill(conn, fake_client)
    assert cursor_writes(conn, "deals") == 3

# Review Focus 4
def test_429_midway_resumes_without_skipping_or_duplicating(fake_client):
    fake_client.fail_on_page = 2
    with pytest.raises(RateLimited):
        backfill(conn, fake_client)
    fake_client.fail_on_page = None
    backfill(conn, fake_client)
    ids = [r[0] for r in conn.execute("select id from deals order by id")]
    assert ids == EXPECTED_ALL_IDS            # complete
    assert len(ids) == len(set(ids))          # and no duplicates

def test_completed_type_is_not_rewalked_on_rerun(fake_client):
    backfill(conn, fake_client)
    fake_client.reset_calls()
    backfill(conn, fake_client)
    assert fake_client.list_calls == []

def test_estimate_reports_record_count_and_request_count_per_type(fake_client):
    est = estimate_backfill(conn, fake_client)
    assert est[0].object_type == "owners"
    assert est[0].requests == math.ceil(est[0].records / 100)

def test_progress_callback_invoked_per_page(fake_client):
    seen = []
    backfill(conn, fake_client, progress=lambda t, n: seen.append((t, n)))
    assert seen
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sync_backfill.py -v`
Expected: FAIL — `cannot import name 'backfill'`

- [ ] **Step 3: Implement `backfill` and `estimate_backfill`**

Walk `BACKFILL_ORDER`, skipping types whose `sync_state.phase` is already `complete`. Per type, page the list endpoint from the checkpointed cursor, upserting and writing the cursor in one transaction per page, then mark `phase = complete` and set the watermark to the max modified value seen. `estimate_backfill` requests one page per type with `limit=1` to read `total`, and derives request counts from a page size of 100 — it makes no writes.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sync_backfill.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/sync.py tests/test_sync_backfill.py
git commit -m "feat: resumable checkpointed backfill with cost estimate"
```

---

### Task 5: Archives and reconciliation

**Files:**
- Modify: `crm/sync.py`
- Test: `tests/test_sync_archives.py`

**Interfaces:**
- Consumes: Tasks 3 and 4.
- Produces: `sync_archived(conn, client, object_types: list[str] | None = None) -> SyncReport`; `reconcile(conn, client) -> dict[str, int]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_sync_archived_sets_flag_on_existing_records(conn, fake_client):
    upsert(conn, "deals", [{"id": "4001", "archived": 0}])
    fake_client.archived["deals"] = [{"id": "4001"}]
    sync_archived(conn, fake_client, ["deals"])
    assert conn.execute("select archived from deals").fetchone()[0] == 1

def test_sync_archived_requests_archived_true(fake_client):
    sync_archived(conn, fake_client, ["deals"])
    assert fake_client.list_calls[0].archived is True

def test_reconcile_archives_locals_absent_from_remote_id_set(conn, fake_client):
    upsert(conn, "deals", [{"id": "1"}, {"id": "2"}])
    fake_client.remote_ids["deals"] = ["1"]
    assert reconcile(conn, fake_client)["deals"] == 1
    assert conn.execute(
        "select archived from deals where id='2'").fetchone()[0] == 1

def test_reconcile_never_deletes_rows(conn, fake_client):
    ...
    assert conn.execute("select count(*) from deals").fetchone()[0] == 2

def test_reconcile_uses_id_only_endpoint(fake_client):
    reconcile(conn, fake_client)
    assert fake_client.ids_only_calls and fake_client.list_calls == []

def test_incremental_run_includes_archived_pass(conn, fake_client):
    sync_incremental(conn, fake_client, ["deals"])
    assert any(c.archived for c in fake_client.list_calls)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sync_archives.py -v`
Expected: FAIL — `cannot import name 'sync_archived'`

- [ ] **Step 3: Implement `sync_archived` and `reconcile`, and call `sync_archived` from `sync_incremental`**

`sync_archived` pages the list endpoint with `archived=True` and upserts with `archived = 1`. `reconcile` pages `list_ids_page` per type, collects the full remote id set, and sets `archived = 1` on local ids absent from it. Neither deletes rows.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sync_archives.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/sync.py tests/test_sync_archives.py
git commit -m "feat: archive tracking and id-set reconciliation"
```

---

### Task 6: Signals

**Files:**
- Create: `crm/signals.py`
- Modify: `crm/sync.py` (call `evaluate_all` at the end of each sync)
- Test: `tests/test_signals.py`

**Interfaces:**
- Consumes: `connect` (Task 1), thresholds from `load_thresholds` (Task 1).
- Produces: `SIGNALS: list[Signal]` (`Signal` has `id`, `title`, `severity`, `record_type`, `sql`, `explanation`); `evaluate_all(conn, thresholds, now_ms: int) -> int`; `briefing(conn, now_ms: int) -> list[SignalSummary]` (`SignalSummary` has `signal_id`, `title`, `severity`, `count`, `total_value`, `by_owner: list[OwnerRollup]`); `total_at_risk(conn) -> float`.

- [ ] **Step 1: Write the failing tests**

One seeded fixture database per test, with `now_ms` passed in so nothing depends on the wall clock.

```python
def test_stalled_deal_flagged_past_threshold_not_before(conn):
    # two open deals, last activity 20 and 3 days ago, threshold 14
    assert flagged(conn, "stalled_deal") == {"d_stale"}

def test_closed_deals_never_flagged(conn): ...
def test_archived_records_never_flagged(conn): ...

def test_every_signal_sql_filters_archived():
    for s in SIGNALS:
        assert "archived = 0" in s.sql

def test_no_signal_sql_touches_engagements():
    for s in SIGNALS:
        assert "engagements" not in s.sql.lower()

def test_no_signal_sql_uses_bare_amount_column():
    for s in SIGNALS:
        assert "amount_in_home_currency" in s.sql or "amount" not in s.sql

def test_past_close_date_uses_local_timezone_boundary(conn): ...
def test_big_and_quiet_respects_value_threshold(conn): ...
def test_untouched_account_requires_an_open_deal(conn): ...
def test_unowned_record_flags_null_owner(conn): ...

def test_quiet_pipeline_flags_sharp_drop_against_4_week_average(conn):
    # 10,10,10,10 then 4 this week with drop_pct 40 -> flagged

def test_quiet_pipeline_silent_on_normal_week(conn): ...

# Review Focus 3
def test_null_amount_deal_is_flagged_but_contributes_zero_to_total(conn):
    s = summary(conn, "stalled_deal")
    assert "d_no_amount" in s.record_ids
    assert s.total_value == 50000.0        # the other deal only

def test_null_closedate_deal_is_not_flagged_as_past_close(conn): ...

def test_null_owner_rolls_up_under_unassigned(conn):
    assert any(o.owner_id is None and o.label == "Unassigned"
               for o in summary(conn, "stalled_deal").by_owner)

def test_briefing_sorted_by_severity_then_value(conn): ...

def test_by_owner_sorted_by_value_descending(conn): ...

def test_total_at_risk_counts_a_deal_once_across_overlapping_signals(conn):
    # one 200k deal in both stalled_deal and big_and_quiet
    assert total_at_risk(conn) == 200000.0

def test_evaluate_all_replaces_previous_results(conn):
    evaluate_all(conn, thresholds, now_ms); n = evaluate_all(conn, thresholds, now_ms)
    assert conn.execute("select count(*) from signal_results").fetchone()[0] == n
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_signals.py -v`
Expected: FAIL — `ModuleNotFoundError: crm.signals`

- [ ] **Step 3: Implement `crm/signals.py`**

Seven signals from the spec: `stalled_deal`, `past_close_date`, `big_and_quiet`, `untouched_account`, `unowned_record`, `quiet_pipeline`, and `stage_regression` — which is declared with `enabled = false` and a comment pointing at the spec's open question on stage history, so it ships inert rather than wrong. Each `sql` is a parameterised `SELECT id, hubspot_owner_id, amount_in_home_currency` with `:now_ms` and its threshold bound, joined to `stages` on `dealstage` and filtered `is_closed = 0` for "open deal".

`evaluate_all` deletes and reinserts `signal_results` inside one transaction. `briefing` reads only `signal_results`, left-joined to `owners` for labels and anti-joined to unexpired `snoozes`; nulls roll up under the literal label `Unassigned`. `total_at_risk` sums `amount_in_home_currency` over `DISTINCT record_id`, treating null as 0.

Then call `evaluate_all` at the end of `sync_incremental` and `backfill`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_signals.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/signals.py crm/sync.py tests/test_signals.py
git commit -m "feat: SQL signals materialised with per-owner rollup"
```

---

### Task 7: Snooze

**Files:**
- Create: `crm/snooze.py`
- Test: `tests/test_snooze.py`

**Interfaces:**
- Consumes: Task 1's connection, Task 6's `briefing`.
- Produces: `snooze(conn, signal_id: str, record_id: str, until_ms: int) -> None`; `active_snoozes(conn, now_ms: int) -> set[tuple[str, str]]`; `drop_stale_snoozes(conn, record_type: str, changed_ids: list[str]) -> int`; `SNOOZE_FIELDS: dict[str, tuple[str, ...]]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_snoozed_record_hidden_from_briefing(conn):
    snooze(conn, "stalled_deal", "d1", now_ms + WEEK_MS)
    assert "d1" not in flagged_in_briefing(conn, now_ms)

def test_expired_snooze_reappears(conn):
    snooze(conn, "stalled_deal", "d1", now_ms - 1)
    assert "d1" in flagged_in_briefing(conn, now_ms)

def test_snooze_is_per_signal_not_global(conn):
    snooze(conn, "stalled_deal", "d1", now_ms + WEEK_MS)
    assert "d1" in flagged_in_briefing(conn, now_ms, signal="big_and_quiet")

def test_snooze_dropped_when_value_stage_or_owner_changes(conn):
    snooze(conn, "stalled_deal", "d1", now_ms + WEEK_MS)
    upsert(conn, "deals", [{"id": "d1", "amount_in_home_currency": 99999}])
    assert drop_stale_snoozes(conn, "deals", ["d1"]) == 1

def test_snooze_survives_an_unrelated_field_change(conn):
    upsert(conn, "deals", [{"id": "d1", "dealname": "renamed"}])
    assert drop_stale_snoozes(conn, "deals", ["d1"]) == 0

def test_snoozing_writes_nothing_to_hubspot(fake_client):
    assert fake_client.all_calls == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_snooze.py -v`
Expected: FAIL — `ModuleNotFoundError: crm.snooze`

- [ ] **Step 3: Implement `crm/snooze.py` and wire `drop_stale_snoozes` into sync**

`SNOOZE_FIELDS` names the material fields per record type — for deals, `("amount_in_home_currency", "dealstage", "hubspot_owner_id")`. `snooze` stores a fingerprint of those fields alongside the row; `drop_stale_snoozes` deletes any snooze for a changed id whose fingerprint no longer matches. Call it from `sync_incremental` with the ids that changed, before `evaluate_all`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_snooze.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/snooze.py crm/sync.py tests/test_snooze.py
git commit -m "feat: per-signal snooze dropped on material change"
```

---

### Task 8: Question box

**Files:**
- Create: `crm/ask.py`
- Test: `tests/test_ask.py`

**Interfaces:**
- Consumes: `connect_readonly` (Task 1), `Config` (Task 1).
- Produces: `schema_prompt(conn) -> str`; `generate_sql(question: str, schema: str, cfg: Config, http=None) -> str`; `guard_sql(sql: str) -> str`; `run_query(db_path: Path, sql: str, timeout_s: float, row_cap: int) -> QueryResult`; `ask(question, conn, cfg) -> AskResult` (`sql`, `rows`, `columns`, `error`); exceptions `UnsafeSQL`, `QueryTimeout`, `ModelUnavailable`.

- [ ] **Step 1: Write the failing tests**

```python
def test_schema_prompt_contains_table_and_column_names(conn):
    p = schema_prompt(conn)
    assert "deals" in p and "amount_in_home_currency" in p

def test_schema_prompt_contains_no_row_data(conn):
    upsert(conn, "deals", [{"id": "d1", "dealname": "Acme Secret Project"}])
    assert "Acme Secret Project" not in schema_prompt(conn)

@pytest.mark.parametrize("bad", [
    "delete from deals",
    "update deals set amount_in_home_currency = 0",
    "select 1; drop table deals",
    "attach database '/etc/passwd' as p",
    "pragma writable_schema = 1",
    "with x as (select 1) insert into deals select * from x",
    "SELECT 1 -- ; delete from deals",
])
def test_guard_rejects_non_select(bad):
    with pytest.raises(UnsafeSQL):
        guard_sql(bad)

def test_guard_accepts_single_select():
    assert guard_sql("select count(*) from deals").startswith("select")

# Review Focus 5
@pytest.mark.parametrize("raw", [
    "Here is the SQL you asked for:\n```sql\nselect 1 from deals\n```",
    "```\nselect 1 from deals\n```",
])
def test_fenced_or_prefaced_output_is_unwrapped(raw):
    assert guard_sql(strip_model_output(raw)) == "select 1 from deals"

def test_prose_with_no_sql_raises_unsafe():
    with pytest.raises(UnsafeSQL):
        guard_sql(strip_model_output("I'm not sure what you mean."))

def test_run_query_is_readonly(tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        run_query(tmp_path / "t.db", "delete from deals", 1.0, 100)

def test_run_query_caps_rows(tmp_path):
    assert len(run_query(tmp_path/"t.db", "select * from deals", 5.0, 10).rows) == 10

def test_run_query_times_out_on_long_query(tmp_path):
    with pytest.raises(QueryTimeout):
        run_query(tmp_path/"t.db", SLOW_CROSS_JOIN, 0.2, 100)

# Review Focus 2
def test_ask_reports_model_unavailable_when_ollama_refuses(conn, cfg):
    r = ask("anything", conn, cfg)   # transport raises ConnectError
    assert r.error and "Ollama" in r.error and r.rows == []

def test_ask_returns_sql_even_when_query_fails(conn, cfg):
    r = ask("bad question", conn, cfg)
    assert r.sql                     # always shown to the user
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_ask.py -v`
Expected: FAIL — `ModuleNotFoundError: crm.ask`

- [ ] **Step 3: Implement `crm/ask.py`**

`schema_prompt` reads `sqlite_master` and emits `table(col, col, ...)` lines only. `generate_sql` posts to `{ollama_url}/api/generate` with the schema, four example question→SQL pairs, and an instruction to return SQL only; `httpx.ConnectError` becomes `ModelUnavailable`. `strip_model_output` removes fences and any leading prose up to the first `select`/`with`. `guard_sql` normalises whitespace, rejects anything whose single statement — after `sqlite3.complete_statement` confirms exactly one — does not begin with `select` or `with`, and rejects `attach`, `pragma` and any DML keyword at statement level. `run_query` opens read-only and installs a `set_progress_handler` callback that raises `QueryTimeout` past the deadline.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_ask.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/ask.py tests/test_ask.py
git commit -m "feat: Ollama question box with SQL guard and timeout"
```

---

### Task 9: Dashboard

**Files:**
- Create: `crm/web.py`, `crm/templates/base.html`, `crm/templates/index.html`, `crm/templates/signal.html`, `crm/templates/record.html`, `crm/static/style.css`
- Test: `tests/test_web.py`

**Interfaces:**
- Consumes: `briefing`/`total_at_risk` (Task 6), `snooze` (Task 7), `ask` (Task 8), `sync_incremental` (Task 3).
- Produces: `create_app(cfg: Config) -> FastAPI`; routes `GET /`, `GET /signal/{signal_id}`, `GET /record/{record_type}/{record_id}`, `POST /ask`, `POST /sync`, `POST /snooze`.

- [ ] **Step 1: Write the failing tests**

Drive with `fastapi.testclient.TestClient` against a seeded temporary database.

```python
# Review Focus 1
def test_index_renders_on_empty_database(client_empty):
    r = client_empty.get("/")
    assert r.status_code == 200
    assert "No data yet" in r.text and "sync --backfill" in r.text

def test_index_lists_signals_highest_severity_first(client):
    assert order_of_cards(client.get("/").text) == [
        "Stalled deals", "Past close date", "Big and quiet",
        "Untouched accounts", "No owner assigned"]

def test_index_shows_deduplicated_total_at_risk(client):
    assert "$200,000" in client.get("/").text

def test_signal_card_shows_owner_rollup_before_records(client):
    body = client.get("/signal/stalled_deal").text
    assert body.index("R. Mehta") < body.index("Acme renewal")

def test_signal_page_shows_rule_in_plain_english_and_its_sql(client):
    body = client.get("/signal/stalled_deal").text
    assert "no activity in 14 days" in body and "archived = 0" in body

def test_signal_record_list_is_paginated(client):
    assert client.get("/signal/stalled_deal").text.count("<tr") <= 51

def test_record_page_uses_hubspot_property_labels(client):
    body = client.get("/record/deals/d1").text
    assert "Amount in company currency" in body and "Deal Stage" in body

def test_record_page_links_back_to_hubspot(client):
    assert "app.hubspot.com" in client.get("/record/deals/d1").text

def test_record_page_fetches_engagements_on_demand(client, fake_client):
    client.get("/record/deals/d1")
    assert fake_client.assoc_calls[-1].to_type == "engagements"
    assert fake_client.assoc_calls[-1].ids == ["d1"]

def test_record_page_renders_without_engagements_when_fetch_fails(client, fake_client):
    fake_client.raise_on_assoc = True
    r = client.get("/record/deals/d1")
    assert r.status_code == 200 and "Deal Stage" in r.text

def test_ask_shows_generated_sql_above_results(client):
    body = client.post("/ask", data={"question": "how many deals?"}).text
    assert body.index("select") < body.index("Results")

# Review Focus 2
def test_index_still_renders_when_ollama_is_down(client_no_ollama):
    r = client_no_ollama.get("/")
    assert r.status_code == 200 and "Stalled deals" in r.text

def test_snooze_redirects_and_hides_record(client):
    client.post("/snooze", data={"signal_id": "stalled_deal",
                                 "record_id": "d1", "days": 7})
    assert "d1" not in client.get("/signal/stalled_deal").text

def test_sync_button_triggers_incremental_not_backfill(client, fake_client):
    client.post("/sync")
    assert fake_client.search_calls and fake_client.list_calls == []

def test_last_sync_time_shown(client):
    assert "last sync" in client.get("/").text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_web.py -v`
Expected: FAIL — `ModuleNotFoundError: crm.web`

- [ ] **Step 3: Implement `crm/web.py` and the templates**

Server-rendered Jinja2, following the spec's layout sketch: header with last sync time and a Sync now button, the briefing as severity-ordered cards that expand to owner rollups then records, the pipeline bar chart from `stages.display_order`, and the ask form last. Record pages render the HubSpot property label for each field (a `LABELS` map in `web.py`) and link to `app.hubspot.com`. Record lists paginate at 50. No front-end framework; the ask form is a plain `POST` and the expanders are `<details>`.

Engagement history on a record page is fetched **on demand** — `batch_associations(record_type, "engagements", [record_id])` for that one record, then the engagement rows read locally. This is why engagement associations are not synced in bulk (Task 3). A failure or timeout here degrades the page to "activity history unavailable"; it never fails the page, because the briefing's correctness does not depend on it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_web.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crm/web.py crm/templates/ crm/static/ tests/test_web.py
git commit -m "feat: per-owner briefing dashboard"
```

---

### Task 10: CLI, timer and README

**Files:**
- Create: `crm/cli.py`, `crm/__main__.py`, `README.md`
- Test: `tests/test_cli.py`
- Modify: `pyproject.toml` (console script `watchdog = crm.cli:main`)

**Interfaces:**
- Consumes: every prior task.
- Produces: `main(argv: list[str] | None = None) -> int` with subcommands `init`, `estimate`, `backfill`, `sync`, `reconcile`, `serve`.

- [ ] **Step 1: Write the failing tests**

```python
def test_init_writes_config_from_example_without_overwriting(tmp_path): ...

def test_estimate_prints_per_type_counts_and_makes_no_writes(tmp_path, fake_client):
    assert main(["estimate"]) == 0
    assert db_unchanged(tmp_path)

def test_backfill_is_not_reachable_without_an_explicit_subcommand():
    assert main([]) != 0        # prints usage, syncs nothing

def test_sync_runs_incremental_then_evaluates_signals(fake_client):
    assert main(["sync"]) == 0
    assert signal_results_rowcount() > 0

def test_serve_starts_timer_at_configured_interval(monkeypatch):
    assert timer_intervals == [15 * 60]

def test_missing_config_exits_with_actionable_message(capsys):
    assert main(["sync"]) == 2
    assert "watchdog init" in capsys.readouterr().err
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: crm.cli`

- [ ] **Step 3: Implement `crm/cli.py`, `crm/__main__.py` and `README.md`**

`argparse` subcommands wiring the existing functions; no subcommand prints usage and returns non-zero. `serve` runs `uvicorn` and arms a `threading.Timer` at `sync_interval_minutes` that calls `sync_incremental`, re-arming after each run and logging failures rather than dying. README covers creating the HubSpot private app and the scopes it needs, `watchdog init`, `watchdog estimate` before `watchdog backfill`, that the backfill takes hours and is resumable, `watchdog serve`, and that `watchdog reconcile` is worth a weekly run.

- [ ] **Step 4: Run the full suite**

Run: `pytest -v`
Expected: PASS, all tasks' tests

- [ ] **Step 5: Commit**

```bash
git add crm/cli.py crm/__main__.py README.md pyproject.toml tests/test_cli.py
git commit -m "feat: CLI, background sync timer and setup docs"
```

---

## Notes for the executor

- No test may make a live HubSpot or Ollama call. Task 2's fixtures and the fake client built in Task 3 are the seams; reuse them rather than adding new mocking approaches.
- `now_ms` is a parameter everywhere it appears. No signal or test reads the wall clock.
- `stage_regression` ships declared but disabled. Enabling it is a follow-up that first has to resolve whether stage history is affordable at this account size.
- Task 6's tests use `conftest.py` helpers — `flagged(conn, signal_id)`, `summary(conn, signal_id)`, `flagged_in_briefing(conn, now_ms, signal=None)` — thin wrappers over `briefing()` that return ids and one summary. Build them in Task 6 and reuse them in Task 7; don't reimplement per test file.
- The fake HubSpot client is built once in Task 3 (`tests/fakes.py`) and extended by later tasks: Task 4 adds `pages`/`fail_on_page`, Task 5 adds `archived`/`remote_ids`/`ids_only_calls`, Task 9 adds `raise_on_assoc`. One fake, grown; not one per task.
