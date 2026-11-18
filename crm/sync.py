"""Incremental sync: HubSpot -> SQLite.

Orchestration only. HTTP lives in `crm.hubspot`, SQL in `crm.db`.

Resume semantics, which the whole tool's trustworthiness rests on:

- The page cursor is checkpointed in the *same transaction* as the page's rows,
  so a crash between pages resumes from the last completed page: no gap, and
  no duplicate (writes are upserts keyed on HubSpot id).
- The watermark advances only once an object type pages to exhaustion. A crash
  leaves it where it was, so the next run re-reads from the same point.
- The ~60s rewind is applied to what is *read* (the search filter), never to
  what is stored. Re-processing a few records is free; missing one is not.
"""
import json
import math
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import httpx

from crm.config import home_dir, load_thresholds, signal_settings
from crm.db import upsert, upsert_sql
from crm.hubspot import (
    ENGAGEMENT_TYPES,
    MODIFIED_PROPERTY,
    PAGE_SIZE,
    PROPERTIES,
    RateLimited,
)
from crm.signals import evaluate_all
from crm.snooze import drop_stale_snoozes

__all__ = [
    "reconcile", "sync_archived", "SYNC_LOCK", "WeeklyResult", "blocked_types",
    "WEEKLY_INTERVAL_MS", "WEEKLY_STATE", "weekly_checked_at", "weekly_due",
    "never_backfilled", "weekly_pass", "archive_eligible", "backfill_state",
    "BACKFILL_ORDER", "Estimate", "backfill", "estimate_backfill",
    "ASSOCIATION_PAIRS", "INCREMENTAL_TYPES", "SEARCH_RESULT_CAP",
    "SEARCH_WINDOW_LIMIT", "SearchWindowStuck", "SyncReport",
    "WATERMARK_REWIND_MS", "cursor",
    "normalise_record", "refresh_associations", "refresh_owners",
    "refresh_account", "refresh_pipelines", "set_cursor", "set_watermark", "sync_incremental",
    "to_epoch_ms", "watermark",
]

# Rewind applied to the stored watermark when reading, for clock skew and
# same-millisecond writes.
WATERMARK_REWIND_MS = 60_000

# The search endpoint caps at ~10,000 results per query, so a window is cut off
# well short of it and re-opened from the newest record seen.
SEARCH_RESULT_CAP = 10_000
SEARCH_WINDOW_LIMIT = SEARCH_RESULT_CAP - 1_000

# Types the modified-date search can walk. Owners have no /search endpoint and
# pipelines are two small requests, so both are refreshed in full every run.
INCREMENTAL_TYPES = ["companies", "deals", "contacts", "engagements"]

# The only pairs the signals use. Engagement associations are deliberately not
# synced in bulk; a record's engagements are fetched on demand.
ASSOCIATION_PAIRS = [
    ("deals", "companies"),
    ("deals", "contacts"),
    ("companies", "contacts"),
]

# Columns each local table carries beyond id/raw/archived. For the three main
# objects these are exactly the properties the client requests.
TABLE_COLUMNS: dict[str, list[str]] = {
    **{t: list(PROPERTIES[t]) for t in ("deals", "companies", "contacts")},
    "engagements": ["type", "timestamp", "hs_lastmodifieddate"],
    "owners": ["email", "first_name", "last_name"],
}

# Column -> payload key, where the two differ.
_SOURCE_KEY = {
    "timestamp": "hs_timestamp",
    "first_name": "firstName",
    "last_name": "lastName",
}

# HubSpot sends these as ISO-8601 strings; the columns are INTEGER epoch ms.
_DATE_COLUMNS = {
    "closedate", "createdate", "hs_last_activity_date", "hs_lastmodifieddate",
    "lastmodifieddate", "timestamp",
}
_REAL_COLUMNS = {"amount_in_home_currency"}

# Owners carry their fields at the top level, not under `properties`.
_TOP_LEVEL = {"owners"}


class SearchWindowStuck(RuntimeError):
    """More records than a search window holds share one millisecond.

    Near-impossible, and unfixable from here, but it is one type's problem like
    every other failure in this layer: it is recorded against that type and the
    remaining types, and the signal re-evaluation, still run.
    """


@dataclass
class SyncReport:
    """`errors` means the briefing may be wrong; `warnings` means it is fine.

    Only `errors` mark the signals `partial` and fail the CLI's exit code. A
    failure that costs nothing but a link or a label (owners, account info)
    belongs in `warnings`: a token without the account-info scope, a likely
    first-run state, must not raise "Data incomplete." on every page forever.
    """
    per_type: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# --- normalisation -----------------------------------------------------------

def to_epoch_ms(value) -> int | None:
    """ISO-8601 (or an already-epoch value) -> epoch ms. Missing stays NULL."""
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.lstrip("-").isdigit():
        return int(text)
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def table_for(object_type: str) -> str:
    """All five engagement endpoints feed the one `engagements` table."""
    return "engagements" if object_type in ENGAGEMENT_TYPES else object_type


def api_types(object_type: str) -> list[str]:
    """The client-level types one requested object type expands into."""
    return list(ENGAGEMENT_TYPES) if object_type == "engagements" else [object_type]


def normalise_record(object_type: str, record: dict) -> dict:
    """One HubSpot payload -> one row for its local table."""
    table = table_for(object_type)
    source = record if table in _TOP_LEVEL else (record.get("properties") or {})
    row: dict = {
        "id": str(record["id"]),
        "raw": json.dumps(record, sort_keys=True),
        "archived": 1 if record.get("archived") else 0,
    }
    for column in TABLE_COLUMNS[table]:
        if column == "type":
            row["type"] = object_type
            continue
        value = source.get(_SOURCE_KEY.get(column, column))
        if column in _DATE_COLUMNS:
            row[column] = to_epoch_ms(value)
        elif column in _REAL_COLUMNS:
            row[column] = None if value in (None, "") else float(value)
        else:
            row[column] = value
    return row


# --- sync_state --------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _evaluate_signals(conn: sqlite3.Connection, complete: bool) -> None:
    """Re-materialise the briefing, honouring config.toml's timezone and week.

    `complete` is False while the data is known to be partial (a backfill is
    unfinished, or a type errored). `quiet_pipeline` is a ratio over creation
    dates and would report a fabricated collapse, so it is skipped and its
    previous result left standing. The state is recorded under
    sync_state['signals'] (status 'partial: ...') for the dashboard to show.
    """
    home = home_dir()
    tz, week = signal_settings(home / "config.toml")
    # A new amount/stage/owner voids a snooze, whichever path wrote it; an
    # expired one is purged rather than re-checked for ever.
    drop_stale_snoozes(conn, _now_ms())
    skip = frozenset() if complete else frozenset({"quiet_pipeline"})
    evaluate_all(conn, load_thresholds(home / "signals.toml"), _now_ms(),
                 tz=tz, week_starts_on=week, skip=skip)
    with conn:
        conn.execute(*_state_sql("signals", {
            "last_run_at": _now_ms(),
            "phase": "complete" if complete else "partial",
            "status": None if complete else
                      "data incomplete: quiet_pipeline not re-evaluated"}))


def _state_sql(object_type: str, fields: dict) -> tuple[str, list]:
    """`sync_state` is keyed on object_type, so db.upsert cannot write it."""
    columns = list(fields)
    marks = ", ".join("?" * len(columns))
    sets = ", ".join(f"{c}=excluded.{c}" for c in columns)
    sql = (f"INSERT INTO sync_state (object_type, {', '.join(columns)}) "
           f"VALUES (?, {marks}) "
           f"ON CONFLICT(object_type) DO UPDATE SET {sets}")
    return sql, [object_type, *fields.values()]


def _state(conn: sqlite3.Connection, object_type: str, column: str):
    row = conn.execute(
        f"SELECT {column} FROM sync_state WHERE object_type = ?", (object_type,)
    ).fetchone()
    return row[column] if row else None


def watermark(conn: sqlite3.Connection, object_type: str) -> int:
    return _state(conn, object_type, "last_modified_ms") or 0


def cursor(conn: sqlite3.Connection, object_type: str) -> str | None:
    return _state(conn, object_type, "cursor")


def set_watermark(conn: sqlite3.Connection, object_type: str, value: int) -> None:
    with conn:
        conn.execute(*_state_sql(object_type, {"last_modified_ms": value}))


def set_cursor(conn: sqlite3.Connection, object_type: str, value: str | None) -> None:
    with conn:
        conn.execute(*_state_sql(object_type, {"cursor": value}))


# --- paging ------------------------------------------------------------------

def _write_page(conn: sqlite3.Connection, table: str, rows: list[dict],
                object_type: str, next_cursor: str | None) -> None:
    """Rows and the cursor that follows them, committed together or not at all."""
    # `phase` is deliberately untouched: it records how far the backfill got,
    # and an incremental run must not reset that.
    sql, params = _state_sql(object_type, {
        "cursor": next_cursor, "status": "running", "last_run_at": _now_ms(),
    })
    with conn:
        if rows:
            conn.executemany(upsert_sql(conn, table, list(rows[0])), rows)
        conn.execute(sql, params)


def _complete_type(conn: sqlite3.Connection, object_type: str,
                   max_modified: int, phase: str | None = None) -> None:
    """Clear the cursor and advance the watermark: one transaction, at the end.

    `phase` is written only when given (the backfill's `complete`), in the same
    transaction, so a type is never complete without its cursor cleared.
    """
    fields = {
        "cursor": None, "last_modified_ms": max_modified,
        "status": "ok", "last_run_at": _now_ms(),
    }
    if phase is not None:
        fields["phase"] = phase
    with conn:
        conn.execute(*_state_sql(object_type, fields))


def _sync_one_type(conn: sqlite3.Connection, client,
                   object_type: str) -> tuple[int, list[str], int]:
    """Page the modified-date search to exhaustion.

    Returns (count, ids seen, max modified). It deliberately does *not* advance
    the watermark: the caller does that only once the associations for these
    records have landed too.
    """
    table = table_for(object_type)
    stored = watermark(conn, object_type)
    since_ms = max(stored - WATERMARK_REWIND_MS, 0)
    after = cursor(conn, object_type)
    modified_column = MODIFIED_PROPERTY[object_type]
    max_modified = stored
    seen: list[str] = []

    # `in_window` tracks how deep into the search's ~10,000-result cap the next
    # request would sit. A resumed `after` token is opaque — we cannot tell what
    # offset it already stands at — so it counts as a full window and is
    # re-windowed at the first page boundary. Re-walking is cheap and idempotent;
    # paging into the cap wedges the type for good. `read` counts only what this
    # window actually read, which is what distinguishes a genuine
    # same-millisecond pile-up from that pessimistic seed.
    in_window = SEARCH_WINDOW_LIMIT if after is not None else 0
    read = 0

    while True:
        page = client.search_modified_since(object_type, since_ms, after)
        rows = [normalise_record(object_type, record) for record in page.records]
        for row in rows:
            modified = row.get(modified_column)
            if modified is not None:
                max_modified = max(max_modified, modified)
        in_window += len(rows)
        read += len(rows)

        # The client sorts ascending on the modified property, so a fresh window
        # from the newest record seen loses nothing: `GT (max - 1)` re-reads the
        # boundary millisecond rather than skipping past it.
        next_since = max(max_modified - 1, 0)
        full = in_window >= SEARCH_WINDOW_LIMIT and page.after is not None
        restart = full and next_since > since_ms
        _write_page(conn, table, rows, object_type,
                    None if restart else page.after)
        seen.extend(row["id"] for row in rows)

        if page.after is None:
            return len(seen), seen, max_modified
        if restart:
            since_ms, after, in_window, read = next_since, None, 0, 0
            continue
        if full and read >= SEARCH_WINDOW_LIMIT:
            raise SearchWindowStuck(
                f"{object_type}: {read} records share one millisecond at "
                f"{max_modified}; cannot advance past the search result cap")
        after = page.after


# --- full refreshes ----------------------------------------------------------

def refresh_pipelines(conn: sqlite3.Connection, client) -> None:
    """Pipelines and stages are small and everything downstream needs them."""
    pipelines, stages = [], []
    for pipeline in client.list_pipelines("deals"):
        pipelines.append({
            "id": str(pipeline["id"]),
            "raw": json.dumps(pipeline, sort_keys=True),
            "archived": 1 if pipeline.get("archived") else 0,
            "label": pipeline.get("label"),
        })
        for stage in pipeline.get("stages") or []:
            metadata = stage.get("metadata") or {}
            probability = metadata.get("probability")
            stages.append({
                "id": str(stage["id"]),
                "raw": json.dumps(stage, sort_keys=True),
                "archived": 1 if stage.get("archived") else 0,
                "pipeline_id": str(pipeline["id"]),
                "label": stage.get("label"),
                "display_order": stage.get("displayOrder"),
                "is_closed": 1 if str(metadata.get("isClosed")).lower() == "true" else 0,
                "probability": None if probability is None else float(probability),
            })
    upsert(conn, "pipelines", pipelines)
    upsert(conn, "stages", stages)


def refresh_account(conn: sqlite3.Connection, client) -> None:
    """Remember the portal id and UI domain (EU accounts are not app.hubspot.com)
    so the dashboard can link a record back to HubSpot without a request."""
    details = client.account_details()
    portal, domain = details.get("portalId"), details.get("uiDomain")
    if portal is None or not domain:
        raise ValueError("account details lacked portalId or uiDomain")
    with conn:
        conn.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [("portal_id", str(portal)), ("ui_domain", str(domain))])


def refresh_owners(conn: sqlite3.Connection, client) -> int:
    """Owners have no /search endpoint, so they are refreshed in full."""
    after, count = None, 0
    while True:
        page = client.list_page("owners", after)
        rows = [normalise_record("owners", record) for record in page.records]
        count += upsert(conn, "owners", rows)
        if page.after is None:
            return count
        after = page.after


# --- associations ------------------------------------------------------------

_DELETE_FORWARD = ("DELETE FROM associations "
                   "WHERE from_type = ? AND from_id = ? AND to_type = ?")
_DELETE_REVERSE = ("DELETE FROM associations "
                   "WHERE to_type = ? AND to_id = ? AND from_type = ?")
_INSERT_LINK = ("INSERT OR IGNORE INTO associations "
                "(from_type, from_id, to_type, to_id) VALUES (?, ?, ?, ?)")


def _rewrite_pair(conn: sqlite3.Connection, pair: tuple[str, str],
                  object_type: str, links: dict[str, list[str]]) -> None:
    """Replace each record's links for one pair: delete, then insert.

    Rows are stored in the pair's canonical direction only, so a record found on
    the `to` side has its stale rows deleted from that side.
    """
    from_type, to_type = pair
    forward = object_type == from_type
    deletes, inserts = [], []
    for record_id, targets in links.items():
        record_id = str(record_id)
        if forward:
            deletes.append((from_type, record_id, to_type))
            inserts.extend(
                (from_type, record_id, to_type, str(t)) for t in targets)
        else:
            deletes.append((to_type, record_id, from_type))
            inserts.extend(
                (from_type, str(t), to_type, record_id) for t in targets)
    with conn:
        conn.executemany(_DELETE_FORWARD if forward else _DELETE_REVERSE, deletes)
        conn.executemany(_INSERT_LINK, inserts)


def refresh_associations(conn: sqlite3.Connection, client, object_type: str,
                         ids: list[str]) -> None:
    """Re-fetch the signal pairs for exactly the ids that changed."""
    changed = list(dict.fromkeys(str(i) for i in ids))
    if not changed:
        return
    for pair in ASSOCIATION_PAIRS:
        if object_type not in pair:
            continue
        other = pair[1] if pair[0] == object_type else pair[0]
        links = client.batch_associations(object_type, other, changed)
        _rewrite_pair(conn, pair, object_type, links)


# --- entry point -------------------------------------------------------------

# A 4xx, or a record we cannot parse, is one type's problem and not the run's:
# both are recorded and the remaining types still sync. RateLimited means the
# backoff schedule was exhausted, so it propagates rather than hammering on.
_TYPE_ERRORS = (httpx.HTTPStatusError, ValueError, SearchWindowStuck)


# Reference data refreshed in full every run, and what its failure costs.
# `stages` is what `is_closed` comes from, so five signals are wrong without a
# fresh one: that is an error. Owner names and the portal id only decorate the
# page, so they are warnings.
_REFERENCE_DATA = (("pipelines", refresh_pipelines, True),
                   ("owners", refresh_owners, False),
                   ("account", refresh_account, False))


def _refresh_reference_data(conn: sqlite3.Connection, client,
                            report: SyncReport) -> None:
    """Pipelines, owners and account info are refreshed in full, and must not
    gate the object sync they merely support, so even a RateLimited is recorded
    here rather than raised. See `SyncReport` on errors versus warnings."""
    for name, refresh, gating in _REFERENCE_DATA:
        try:
            refresh(conn, client)
        except (RateLimited, *_TYPE_ERRORS) as exc:
            bucket = report.errors if gating else report.warnings
            bucket.append(f"{name}: {exc}")


def _sync_object_type(conn: sqlite3.Connection, client, object_type: str,
                      report: SyncReport) -> int:
    """Pages, then associations, then — last — the watermarks."""
    count, ids = 0, []
    pending: list[tuple[str, int]] = []
    try:
        for api_type in api_types(object_type):
            found, seen, max_modified = _sync_one_type(conn, client, api_type)
            count += found
            ids.extend(seen)
            pending.append((api_type, max_modified))
        # Associations land *before* the watermark moves. A record past the
        # watermark is never searched again, so advancing first would strand
        # stale association rows permanently if this refresh failed.
        refresh_associations(conn, client, object_type, ids)
        for api_type, max_modified in pending:
            _complete_type(conn, api_type, max_modified)
    except _TYPE_ERRORS as exc:
        report.errors.append(f"{object_type}: {exc}")
        # Whatever the window accounting says, a cursor this type failed on may
        # be poisoned — deep past the result cap, say. The watermark has not
        # moved, so dropping the cursor re-walks from there next run: no records
        # skipped, and the upserts make the repeat free of side effects.
        # A RateLimited is different: it propagates, and resuming from its
        # checkpoint is the whole point of the backoff.
        for api_type in api_types(object_type):
            set_cursor(conn, api_type, None)
    return count


# One incremental sync at a time per process: the dashboard's Sync now button
# and the serve timer both take this, so they never overlap.
SYNC_LOCK = threading.Lock()


def sync_incremental(conn: sqlite3.Connection, client,
                     object_types: list[str] | None = None) -> SyncReport:
    report = SyncReport()
    requested = object_types or INCREMENTAL_TYPES
    for object_type in requested:
        if object_type not in INCREMENTAL_TYPES:
            # owners and pipelines have no search endpoint and are refreshed in
            # full; anything else is a caller mistake, not a silent no-op.
            raise ValueError(f"{object_type!r} cannot be synced incrementally")

    _refresh_reference_data(conn, client, report)
    synced = False
    for object_type in requested:
        if backfill_pending(conn, object_type):
            # A backfill owns this type's cursor and watermark. A search run
            # would hand its list cursor to /search, clear it on the rejection,
            # and advance the (provisional) watermark past unwalked records.
            report.errors.append(
                f"{object_type}: backfill not complete; run backfill first")
            report.per_type[object_type] = 0
            continue
        if not all(_is_complete(conn, t) for t in api_types(object_type)):
            # Phase NULL: no list cursor exists, so a search run would walk the
            # whole type through capped windows, a slow, wrong-shaped backfill.
            report.errors.append(
                f"{object_type}: never backfilled; run `watchdog estimate`, "
                "then `watchdog backfill`")
            report.per_type[object_type] = 0
            continue
        synced = True
        report.per_type[object_type] = _sync_object_type(
            conn, client, object_type, report)
    if synced:
        # Nothing changed if every type was skipped, so a full re-scan would
        # buy nothing.
        _evaluate_signals(conn, complete=not report.errors and not any(
            backfill_pending(conn, o) for o in BACKFILL_ORDER))
    return report


# --- archives and reconciliation ----------------------------------------------
# Weekly operations, deliberately NOT part of sync_incremental: the archived
# listing has no modified filter, so every pass re-reads all of it, and
# reconcile pages every id. On a large account either would exhaust the daily
# request cap if run every 15 minutes. A record archived in HubSpot therefore
# lingers in the briefing for up to a week.

def _pages(fetch: Callable[[str | None], object]):
    """Yield each page from `fetch(after)` until the cursor runs out."""
    after = None
    while True:
        page = fetch(after)
        yield page
        if page.after is None:
            return
        after = page.after


def sync_archived(conn: sqlite3.Connection, client,
                  object_types: list[str] | None = None) -> SyncReport:
    """Page the archived listing of each type and upsert with `archived = 1`.

    Stateless: it touches neither cursor nor watermark, so it is safe whatever
    the type's phase (`sync_incremental` still only calls it for types it
    syncs). The listing has no modified filter, so each pass re-reads it in
    full; upserts make that idempotent.
    """
    report = SyncReport()
    for object_type in object_types or INCREMENTAL_TYPES:
        count = 0
        try:
            for api_type in api_types(object_type):
                for page in _pages(lambda after: client.list_page(
                        api_type, after, archived=True)):
                    rows = [{**normalise_record(api_type, r), "archived": 1}
                            for r in page.records]
                    count += upsert(conn, table_for(api_type), rows)
        except (RateLimited, *_TYPE_ERRORS) as exc:
            # One type's failure must not starve the others.
            report.errors.append(f"{object_type} archived: {exc}")
        report.per_type[object_type] = count
    return report


# A walk returning fewer than this share of HubSpot's own record count is
# treated as truncated. 1% absorbs deletes landing mid-walk.
RECONCILE_MIN_COVERAGE = 0.99


class ReconcileResult(dict):
    """{object_type: rows newly archived}, plus `skipped`: {object_type: reason}
    and `unarchived`: {object_type: rows brought back}.

    A type is in exactly one of the mapping or `skipped`, so a guarded or
    ungated type is never mistaken for a clean zero.
    """

    def __init__(self) -> None:
        super().__init__()
        self.skipped: dict[str, str] = {}
        self.unarchived: dict[str, int] = {}


def reconcile(conn: sqlite3.Connection, client) -> ReconcileResult:
    """Weekly: flag local rows whose id HubSpot no longer lists as archived.

    Catches *hard* deletes, which appear in neither the search nor the archived
    listing. Pages every id, so it is not a per-run operation.

    The walk is the authoritative set of ids HubSpot currently lists as live,
    so it is read in both directions: an id it holds that is archived locally
    is un-archived. Without that the 1% tolerance below would be a one-way
    ratchet — the incremental search only returns records modified since the
    watermark, so a record wrongly archived here, or archived in HubSpot and
    later restored, would never come back.

    Safety, since a wrong archive silently drops a live deal from the briefing:
    - only types whose backfill is `complete` are reconciled;
    - the local ids are snapshotted *before* the remote walk, so a record
      created during the (long) walk and synced locally meanwhile is never a
      candidate, and neither is one archived locally meanwhile;
    - the whole remote id set is collected before anything is written, so a
      failed walk (any exception, RateLimited included, which propagates)
      writes nothing for that type;
    - a walk that succeeds but is short (fewer ids than 99% of the baseline,
      or empty against live local rows) is treated as truncated and refused.
      The baseline is `max(client.count, local live rows)`: the local count
      costs no request and no `count` cap can shrink it, so a genuine
      mass-archive in HubSpot trips the guard and is reported as a skip
      rather than silently emptying the briefing;
    - an object type is written all-or-nothing across its api types;
    - rows are never deleted, only `archived` flipped.
    Types not reconciled are listed in `result.skipped` with a reason.
    """
    result = ReconcileResult()
    for object_type in INCREMENTAL_TYPES:
        if not all(_is_complete(conn, t) for t in api_types(object_type)):
            result.skipped[object_type] = "backfill not complete"
            continue
        table = table_for(object_type)
        plan: list[tuple[str]] = []
        revive: list[tuple[str]] = []
        reason = None
        for api_type in api_types(object_type):
            scope, params = "", []
            if table == "engagements":
                scope, params = " AND type = ?", [api_type]
            local = [r["id"] for r in conn.execute(
                f"SELECT id FROM {table} WHERE archived = 0{scope}", params)]
            archived_local = [r["id"] for r in conn.execute(
                f"SELECT id FROM {table} WHERE archived = 1{scope}", params)]
            remote: set[str] = set()
            for page in _pages(lambda after: client.list_ids_page(api_type, after)):
                remote.update(str(r["id"]) for r in page.records)
            counted = client.count(api_type)
            expected = max(counted, len(local))
            if (not remote and local) or len(remote) < expected * RECONCILE_MIN_COVERAGE:
                reason = (f"{api_type}: id walk returned {len(remote)} ids, "
                          f"HubSpot counts {counted} and {len(local)} live rows "
                          "are held locally; treated as truncated")
                break
            plan.extend((i,) for i in local if i not in remote)
            # `remote` is the live listing only (archived=false), so a record
            # HubSpot really has archived is absent from it and stays archived.
            revive.extend((i,) for i in archived_local if i in remote)
        if reason:
            result.skipped[object_type] = reason
            continue
        with conn:
            conn.executemany(f"UPDATE {table} SET archived = 1 WHERE id = ?", plan)
            conn.executemany(f"UPDATE {table} SET archived = 0 WHERE id = ?", revive)
        result[object_type] = len(plan)
        result.unarchived[object_type] = len(revive)
    return result


@dataclass
class WeeklyResult:
    archived_found: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    reconciled: ReconcileResult = field(default_factory=ReconcileResult)


# When the weekly pass last ran, kept in `sync_state` under this pseudo-type,
# matching the existing ['signals'] convention. It is not an object type: the
# dashboard's "last sync" and every type loop exclude it.
WEEKLY_STATE = "weekly"
WEEKLY_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000


def weekly_checked_at(conn: sqlite3.Connection) -> int | None:
    """When the weekly archive/delete pass last completed, or None."""
    return _state(conn, WEEKLY_STATE, "last_run_at")


def weekly_due(conn: sqlite3.Connection, now_ms: int) -> bool:
    """True if archives have never been checked, or not for a week.

    This module promises a record archived in HubSpot lingers in the briefing
    "for up to a week"; with nothing scheduling the pass that read "until the
    operator remembers".
    """
    last = weekly_checked_at(conn)
    return last is None or now_ms - last >= WEEKLY_INTERVAL_MS


def _stamp_weekly(conn: sqlite3.Connection, result: WeeklyResult) -> None:
    """Record that the pass ran, and what it managed.

    Stamped whatever the outcome: leaving it unstamped after a partial pass
    would have the serve timer repeat this job, which reads every id of every
    type, at every sync interval. `status` names what was skipped, so "checked
    today" is never read as "everything was checked".
    """
    skipped = sorted(result.reconciled.skipped)
    detail = "; ".join(filter(None, [
        f"skipped: {', '.join(skipped)}" if skipped else "",
        f"{len(result.errors)} error(s)" if result.errors else ""]))
    with conn:
        conn.execute(*_state_sql(WEEKLY_STATE, {
            "last_run_at": _now_ms(), "status": detail or "ok",
            "phase": "partial" if detail else COMPLETE}))


def weekly_pass(conn: sqlite3.Connection, client) -> WeeklyResult:
    """The weekly operation: archived listing, then hard-delete reconcile.

    The archived pass runs only for `archive_eligible` types. An empty list is
    never passed on: `sync_archived` reads it as "every type". Completion is
    stamped so the dashboard can say how stale the archive check is, and the
    serve timer can run this when it falls due.
    """
    result = WeeklyResult()
    eligible = archive_eligible(conn)
    if eligible:
        report = sync_archived(conn, client, eligible)
        result.archived_found, result.errors = report.per_type, report.errors
    result.reconciled = reconcile(conn, client)
    _stamp_weekly(conn, result)
    return result


# --- backfill ----------------------------------------------------------------

# Briefing objects first, so an interrupted backfill still leaves them usable;
# engagements are the bulk and come last. Pipelines are not walkable: they are
# refreshed before the walk. Owners are a small full refresh.
BACKFILL_ORDER = ["owners", "companies", "deals", "contacts", "engagements"]

COMPLETE = "complete"
# While a walk is in flight `last_modified_ms` is only the walk's start time, a
# provisional value. `phase` says so; NULL means the type was never backfilled.
IN_PROGRESS = "backfill"
BLOCKED = "blocked"   # `status` then reads "blocked: <error>"; cursor kept


def backfill_pending(conn: sqlite3.Connection, object_type: str) -> bool:
    """True if any api type of `object_type` is mid-backfill or blocked, i.e.
    its watermark and cursor belong to the backfill, not to incremental."""
    return any(_state(conn, t, "phase") in (IN_PROGRESS, BLOCKED)
               for t in api_types(object_type))


def backfill_state(conn: sqlite3.Connection) -> dict[str, tuple[str | None, str | None]]:
    """{api_type: (phase, status)} for every type a backfill covers.

    `phase` is the authority: None (never backfilled), `backfill`, `blocked`,
    `complete`. For a blocked type `status` is "blocked: <message>".
    """
    return {t: (_state(conn, t, "phase"), _state(conn, t, "status"))
            for o in BACKFILL_ORDER for t in api_types(o)}


def never_backfilled(conn: sqlite3.Connection) -> list[str]:
    """api types whose phase is NULL: no backfill has ever started."""
    return [t for t, (phase, _) in backfill_state(conn).items() if phase is None]


def blocked_types(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """(api type, "blocked: <message>") for every blocked type."""
    return [(t, status or "blocked") for t, (phase, status)
            in backfill_state(conn).items() if phase == BLOCKED]


def archive_eligible(conn: sqlite3.Connection) -> list[str]:
    """Types the weekly archived pass may touch: every api type `complete`.

    Stricter than "no pending backfill": a never-backfilled type (phase NULL)
    is not pending either, yet archived-only rows inserted into it would be
    read by no signal. `sync_archived` has no gate of its own, and given an
    empty list it would fall back to *all* types, so callers must check.
    """
    return [o for o in INCREMENTAL_TYPES
            if all(_is_complete(conn, t) for t in api_types(o))]


def _begin_walk(conn: sqlite3.Connection, api_type: str, fresh: bool) -> None:
    """Mark the type in progress; on a fresh walk also stamp its start time,
    in the same transaction so the watermark is never unlabelled."""
    # `status` is cleared so a resumed, formerly blocked type is not mislabelled.
    fields: dict = {"phase": IN_PROGRESS, "status": None}
    if fresh:
        # Also drop any stale cursor, which may be a search cursor left by an
        # incremental run and would be rejected by the list endpoint.
        fields["last_modified_ms"] = _now_ms()
        fields["cursor"] = None
    with conn:
        conn.execute(*_state_sql(api_type, fields))


def _block(conn: sqlite3.Connection, api_type: str, exc: Exception) -> None:
    """Durably record a deterministic failure, keeping the cursor."""
    with conn:
        conn.execute(*_state_sql(api_type, {
            "phase": BLOCKED, "status": f"blocked: {exc}",
            "last_run_at": _now_ms(),
        }))


@dataclass
class Estimate:
    object_type: str
    records: int | None   # None when the API cannot say (owners)
    requests: int         # list requests only; associations are extra


def _is_complete(conn: sqlite3.Connection, api_type: str) -> bool:
    return _state(conn, api_type, "phase") == COMPLETE


def _walk_type(conn: sqlite3.Connection, client, api_type: str,
               progress: Callable[[str, int], None] | None) -> int:
    """Page one api type's *list* endpoint from its checkpoint to exhaustion.

    Per page, in this order: associations, then rows + cursor in one
    transaction. A crash before the commit re-reads the page (idempotent); a
    crash after resumes at the next. Associations come first so a committed
    cursor never outruns them.
    """
    table = table_for(api_type)
    # Only a cursor a backfill wrote is resumable. Anything else on a type that
    # is not mid-backfill (a search cursor from a rate-limited incremental run)
    # is stale: walk fresh.
    resumable = _state(conn, api_type, "phase") in (IN_PROGRESS, BLOCKED)
    after = cursor(conn, api_type) if resumable else None
    # List order is by id, not modified date, so the max modified seen is
    # not a safe watermark: a record walked early and edited later can sit
    # below it and be missed forever. The walk's start time is safe, and
    # incremental sync re-reads everything modified since. A resumed walk
    # (cursor set) keeps the start time stored by its first run.
    _begin_walk(conn, api_type, fresh=after is None)
    count = 0
    while True:
        page = client.list_page(api_type, after)
        rows = [normalise_record(api_type, record) for record in page.records]
        refresh_associations(conn, client, api_type, [r["id"] for r in rows])
        _write_page(conn, table, rows, api_type, page.after)
        count += len(rows)
        if progress:
            progress(api_type, count)
        if page.after is None:
            _complete_type(conn, api_type, watermark(conn, api_type), COMPLETE)
            return count
        after = page.after


def _backfill_owners(conn: sqlite3.Connection, client,
                     progress: Callable[[str, int], None] | None) -> int:
    count = refresh_owners(conn, client)
    _complete_type(conn, "owners", 0, COMPLETE)
    if progress:
        progress("owners", count)
    return count


def backfill(conn: sqlite3.Connection, client,
             progress: Callable[[str, int], None] | None = None) -> SyncReport:
    """Resumable initial load over the list endpoints (never search).

    Types whose phase is `complete` are skipped, so a re-run costs nothing and
    an interrupted one picks up at the last checkpointed page. `RateLimited`
    propagates with the checkpoint intact. A 4xx or unparseable record is
    recorded against its type and the checkpoint is *kept*: the pages already
    written are valuable, and a cleared cursor would re-walk them, possibly
    millions of records, to hit the same deterministic error.
    """
    report = SyncReport()
    try:
        refresh_pipelines(conn, client)
    except (RateLimited, *_TYPE_ERRORS) as exc:
        # Two cheap requests must not gate an hours-long job.
        report.errors.append(f"pipelines: {exc}")
    for object_type in BACKFILL_ORDER:
        count = 0
        for api_type in api_types(object_type):
            if _is_complete(conn, api_type):
                continue
            try:
                if api_type == "owners":
                    count += _backfill_owners(conn, client, progress)
                else:
                    count += _walk_type(conn, client, api_type, progress)
            except _TYPE_ERRORS as exc:
                # Durable, so a later process (the CLI) can see the type is
                # wedged and where. The cursor is kept; see the docstring.
                _block(conn, api_type, exc)
                report.errors.append(f"{api_type}: {exc}")
                break
        report.per_type[object_type] = count
    _evaluate_signals(conn, complete=not report.errors and all(
        _is_complete(conn, t) for o in BACKFILL_ORDER for t in api_types(o)))
    return report


def estimate_backfill(conn: sqlite3.Connection, client) -> list[Estimate]:
    """Size the job before committing hours to it. Reads only: no writes."""
    estimates = []
    for object_type in BACKFILL_ORDER:
        if object_type == "owners":
            estimates.append(Estimate("owners", None, 0))
            continue
        counts = [client.count(t) for t in api_types(object_type)]
        estimates.append(Estimate(
            object_type, sum(counts),
            sum(math.ceil(n / PAGE_SIZE) for n in counts)))
    return estimates
