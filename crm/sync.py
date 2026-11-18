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
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

from crm.db import upsert, upsert_sql
from crm.hubspot import (
    ENGAGEMENT_TYPES,
    MODIFIED_PROPERTY,
    PROPERTIES,
    RateLimited,
)

__all__ = [
    "ASSOCIATION_PAIRS", "INCREMENTAL_TYPES", "SEARCH_RESULT_CAP",
    "SEARCH_WINDOW_LIMIT", "SyncReport", "WATERMARK_REWIND_MS", "cursor", "normalise_record", "refresh_associations",
    "refresh_owners", "refresh_pipelines", "set_cursor", "set_watermark",
    "sync_incremental", "to_epoch_ms", "watermark",
]

# Rewind applied to the stored watermark when reading, for clock skew and
# same-millisecond writes.
WATERMARK_REWIND_MS = 60_000

# The search endpoint caps at ~10,000 results per query, so a window is cut off
# well short of it and re-opened from the newest record seen.
SEARCH_RESULT_CAP = 10_000
SEARCH_WINDOW_LIMIT = 9_000

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


@dataclass
class SyncReport:
    per_type: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


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
                   max_modified: int) -> None:
    """Clear the cursor and advance the watermark: one transaction, at the end."""
    with conn:
        conn.execute(*_state_sql(object_type, {
            "cursor": None, "last_modified_ms": max_modified,
            "status": "ok", "last_run_at": _now_ms(),
        }))


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
    in_window = 0

    while True:
        page = client.search_modified_since(object_type, since_ms, after)
        rows = [normalise_record(object_type, record) for record in page.records]
        for row in rows:
            modified = row.get(modified_column)
            if modified is not None:
                max_modified = max(max_modified, modified)
        in_window += len(rows)

        # The search endpoint refuses `after` beyond ~10,000 results. Because the
        # client sorts ascending on the modified property, we can start a fresh
        # window from the newest record seen instead of paging into the cap —
        # gap-free, since `GT (max - 1)` re-reads the boundary millisecond.
        restart = in_window >= SEARCH_WINDOW_LIMIT and page.after is not None
        _write_page(conn, table, rows, object_type,
                    None if restart else page.after)
        seen.extend(row["id"] for row in rows)

        if page.after is None:
            return len(seen), seen, max_modified
        if not restart:
            after = page.after
            continue

        next_since = max(max_modified - 1, 0)
        if next_since <= since_ms:
            raise RuntimeError(
                f"{object_type}: {in_window} records share one millisecond at "
                f"{max_modified}; cannot advance past the search result cap")
        since_ms, after, in_window = next_since, None, 0


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
_TYPE_ERRORS = (httpx.HTTPStatusError, ValueError)


def _refresh_reference_data(conn: sqlite3.Connection, client,
                            report: SyncReport) -> None:
    """Pipelines and owners are refreshed in full, and must not gate the
    object sync they merely support, so even a RateLimited is recorded here."""
    for name, refresh in (("pipelines", refresh_pipelines),
                          ("owners", refresh_owners)):
        try:
            refresh(conn, client)
        except (RateLimited, *_TYPE_ERRORS) as exc:
            report.errors.append(f"{name}: {exc}")


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
    return count


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
    for object_type in requested:
        report.per_type[object_type] = _sync_object_type(
            conn, client, object_type, report)
    return report
