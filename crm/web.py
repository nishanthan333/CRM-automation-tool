"""The dashboard: a server-rendered FastAPI app over the local SQLite file.

Routes fetch and hand over; the markup lives in `crm/templates`. The page only
ever reads materialised signal results (written by `sync_incremental`), so it
renders on an empty database and never waits on HubSpot, except the record page,
which fetches that one record's engagement list on demand and degrades if it
fails. Everything that originates in HubSpot or the model is HTML-escaped by
Jinja's autoescape; nothing here marks a value `safe`.
"""

from __future__ import annotations

import logging
import math
import re
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Callable
from urllib.parse import quote, urlsplit

import httpx

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape

from crm.ask import ask
from crm.config import Config, home_dir, load_thresholds
from crm.db import connect, migrate
from crm.hubspot import ENGAGEMENT_TYPES, HubSpotClient
from crm.signals import (SIGNALS, UNASSIGNED, briefing, explain, flagged_page,
                         flags_for_record, owner_rollups, total_at_risk)
from crm.snooze import SNOOZE_FIELDS, snooze
from crm.sync import (SYNC_LOCK, WEEKLY_STATE, sync_incremental,
                      weekly_checked_at)

__all__ = ["LABELS", "PAGE_SIZE", "create_app"]

HERE = Path(__file__).parent
log = logging.getLogger(__name__)
PAGE_SIZE = 50
OWNERS_SHOWN = 5            # owner rows on an index card before "… N more"
ENGAGEMENTS_SHOWN = 20
# Past this, the weekly archive/delete pass is overdue (it is due weekly).
ARCHIVE_CHECK_WARN_DAYS = 10
NO_OWNER = "_none"          # ?owner= value for the Unassigned bucket
DAY_MS = 86_400_000
# HubSpot's object type ids, for the "open in HubSpot" link.
HUBSPOT_TYPE_ID = {"deals": "0-3", "companies": "0-2", "contacts": "0-1"}
RECORD_TABLES = tuple(HUBSPOT_TYPE_ID)
SIGNAL_BY_ID = {s.id: s for s in SIGNALS if s.enabled}

# Record fields under HubSpot's own property labels. The only place a label is
# defined; a column with no entry falls back to its raw name rather than a
# label we made up.
LABELS: dict[str, dict[str, str]] = {
    "deals": {
        "dealname": "Deal Name", "dealstage": "Deal Stage",
        "pipeline": "Pipeline",
        "amount_in_home_currency": "Amount in company currency",
        "closedate": "Close Date", "hubspot_owner_id": "Deal owner",
        "hs_last_activity_date": "Last Activity Date",
        "hs_lastmodifieddate": "Last Modified Date",
        "createdate": "Create Date",
    },
    "companies": {
        "name": "Company name", "domain": "Company Domain Name",
        "hubspot_owner_id": "Company owner",
        "hs_last_activity_date": "Last Activity Date",
        "hs_lastmodifieddate": "Last Modified Date",
    },
    "contacts": {
        "firstname": "First Name", "lastname": "Last Name", "email": "Email",
        "hubspot_owner_id": "Contact owner",
        "hs_last_activity_date": "Last Activity Date",
        "lastmodifieddate": "Last Modified Date",
    },
}
_DAY_COLUMNS = {"closedate"}
_TIME_COLUMNS = {"createdate", "hs_last_activity_date", "hs_lastmodifieddate",
                 "lastmodifieddate"}


# --- presentation helpers --------------------------------------------------------

def money(value: float | None) -> str:
    return f"${(value or 0):,.0f}"


def ago(ms: int | None, now_ms: int) -> str:
    if not ms:
        return "never"
    seconds = max((now_ms - ms) // 1000, 0)
    if seconds < 60:
        return "just now"
    for size, unit in ((86_400, "d"), (3_600, "h"), (60, "m")):
        if seconds >= size:
            return f"{seconds // size}{unit} ago"
    return "just now"  # pragma: no cover


def _stamp(ms: int, day_only: bool) -> str:
    when = datetime.fromtimestamp(ms / 1000, timezone.utc)
    return when.strftime("%Y-%m-%d" if day_only else "%Y-%m-%d %H:%M UTC")


_HUBSPOT_HOST = re.compile(r"^[a-z0-9.-]+\.hubspot\.com$")


def hubspot_url(conn: sqlite3.Connection, record_type: str,
                record_id: str) -> str | None:
    """Link to the record in HubSpot, or None until a sync has learned the
    portal id and UI domain (EU accounts are not app.hubspot.com)."""
    meta = {r["key"]: r["value"] for r in conn.execute(
        "SELECT key, value FROM meta WHERE key IN ('portal_id', 'ui_domain')")}
    portal, domain = meta.get("portal_id"), meta.get("ui_domain")
    if not (portal and portal.isdigit() and domain and _HUBSPOT_HOST.match(domain)):
        return None
    return (f"https://{domain}/contacts/{portal}/record/"
            f"{HUBSPOT_TYPE_ID[record_type]}/{quote(record_id, safe='')}")


def _owner_label(row: sqlite3.Row | None, owner_id: str | None) -> str:
    if not owner_id:
        return UNASSIGNED
    if row is None:
        return owner_id
    name = " ".join(p for p in (row["first_name"], row["last_name"]) if p)
    return name or row["email"] or owner_id


# --- reads -----------------------------------------------------------------------

def _owner_labels(conn: sqlite3.Connection) -> dict[str, str]:
    return {r["id"]: _owner_label(r, r["id"])
            for r in conn.execute("SELECT * FROM owners ORDER BY id")}


def _has_data(conn: sqlite3.Connection) -> bool:
    return any(conn.execute(f"SELECT 1 FROM {t} LIMIT 1").fetchone()
               for t in ("deals", "companies"))


def _state(conn: sqlite3.Connection, object_type: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM sync_state WHERE object_type = ?",
                        (object_type,)).fetchone()


def _archives(conn: sqlite3.Connection, now_ms: int) -> dict:
    """How stale the weekly archive/delete check is.

    A reader looking at a clean briefing must be able to tell whether archives
    were checked yesterday or five weeks ago: acting on flags for records that
    no longer exist is the failure the design doc calls worse than no tool.
    """
    last = weekly_checked_at(conn)
    if not last:
        return {"text": "archives never checked", "stale": True}
    days = max((now_ms - last) // DAY_MS, 0)
    when = {0: "today", 1: "1 day ago"}.get(days, f"{days} days ago")
    return {"text": f"archives checked {when}",
            "stale": days >= ARCHIVE_CHECK_WARN_DAYS}


def _chrome(conn: sqlite3.Connection, now_ms: int, notice) -> dict:
    """What every page's header needs: last sync, archive-check age,
    data-incomplete marker."""
    last = conn.execute(
        "SELECT MAX(last_run_at) AS t FROM sync_state"
        " WHERE object_type NOT IN ('signals', ?)", (WEEKLY_STATE,)).fetchone()["t"]
    signals = _state(conn, "signals")
    partial = None
    if signals is not None and signals["phase"] == "partial":
        # The banner supplies the "Data incomplete." heading itself.
        partial = re.sub(r"^\s*data incomplete:?\s*", "",
                         signals["status"] or "", flags=re.I) or "Some data is missing"
    return {"last_sync": ago(last, now_ms), "partial": partial,
            "notice": notice, "archives": _archives(conn, now_ms)}


def _thresholds() -> dict | None:
    try:
        return load_thresholds(home_dir() / "signals.toml")
    except (OSError, ValueError):
        return None


def _rule(sig, thresholds: dict | None) -> dict:
    """A signal's rule in English, its SQL, and the thresholds bound into it."""
    try:
        text = explain(sig, thresholds)
        params = {k: thresholds["thresholds"][k] for k in sig.thresholds}
    except (KeyError, TypeError):
        text, params = sig.explanation, {}
    return {"id": sig.id, "title": sig.title, "severity": sig.severity,
            "rule": text, "sql": sig.sql.strip(), "params": params}


def _pipeline(conn: sqlite3.Connection, owner: str | None) -> list[dict]:
    """Open deals per open stage, in HubSpot's stage order, grouped by pipeline."""
    join, args = "", []
    if owner == NO_OWNER:
        join = " AND COALESCE(d.hubspot_owner_id, '') = ''"
    elif owner:
        join, args = " AND d.hubspot_owner_id = ?", [owner]
    rows = conn.execute(f"""
        SELECT COALESCE(p.label, s.pipeline_id, '(no pipeline)') AS pipeline,
               s.label AS stage, COUNT(d.id) AS n,
               COALESCE(SUM(d.amount_in_home_currency), 0) AS value
        FROM stages s
        LEFT JOIN pipelines p ON p.id = s.pipeline_id
        LEFT JOIN deals d ON d.dealstage = s.id AND d.archived = 0{join}
        WHERE COALESCE(s.is_closed, 0) = 0
        GROUP BY s.id ORDER BY pipeline, s.display_order, s.id""", args).fetchall()
    widest = max((r["n"] for r in rows), default=0) or 1
    groups: dict[str, list[dict]] = {}
    for r in rows:
        groups.setdefault(r["pipeline"], []).append({
            "stage": r["stage"] or "(unlabelled stage)", "count": r["n"],
            "value": r["value"], "pct": round(100 * r["n"] / widest)})
    return [{"name": k, "stages": v} for k, v in groups.items()]


def _index_context(conn: sqlite3.Connection, now_ms: int,
                   owner: str | None) -> dict:
    ctx: dict = {"empty": not _has_data(conn)}
    if ctx["empty"]:
        return ctx
    thresholds = _thresholds()
    summaries = briefing(conn, now_ms)
    cards = []
    for s in summaries:
        card = _rule(SIGNAL_BY_ID[s.signal_id], thresholds)
        card.update(count=s.count, value=s.total_value,
                    owners=s.by_owner[:OWNERS_SHOWN],
                    more_owners=max(len(s.by_owner) - OWNERS_SHOWN, 0))
        cards.append(card)
    flagged = {s.signal_id for s in summaries}
    ctx.update(
        cards=cards,
        total_flags=sum(s.count for s in summaries),
        total_at_risk=total_at_risk(conn, now_ms),
        clear=[s.title for s in SIGNAL_BY_ID.values() if s.id not in flagged],
        signals_ran=_state(conn, "signals") is not None,
        pipeline=_pipeline(conn, owner), owner=owner,
        owners=sorted(_owner_labels(conn).items(), key=lambda kv: kv[1]))
    return ctx


def _names(conn: sqlite3.Connection, record_type: str,
           ids: list[str]) -> dict[str, str]:
    column = {"deals": "dealname", "companies": "name"}.get(record_type)
    if not column or not ids:
        return {}
    marks = ", ".join("?" for _ in ids)
    return {r["id"]: r["n"] for r in conn.execute(
        f"SELECT id, {column} AS n FROM {record_type} WHERE id IN ({marks})", ids)}


def _signal_context(conn: sqlite3.Connection, now_ms: int, signal_id: str,
                    owner: str | None, page: int) -> dict:
    sig = SIGNAL_BY_ID[signal_id]
    rollups = owner_rollups(conn, now_ms, signal_id)
    flagged_total = sum(o.count for o in rollups)
    if owner == NO_OWNER:
        total = sum(o.count for o in rollups if o.owner_id is None)
    elif owner:
        total = sum(o.count for o in rollups if o.owner_id == owner)
    else:
        total = flagged_total
    pages = max(math.ceil(total / PAGE_SIZE), 1)
    page = min(page, pages)
    _, shown = flagged_page(
        conn, now_ms, signal_id, owner_id=None if owner == NO_OWNER else owner,
        unassigned=owner == NO_OWNER, limit=PAGE_SIZE,
        offset=(page - 1) * PAGE_SIZE)
    names: dict[str, dict[str, str]] = {}
    for record_type in {r["record_type"] for r in shown}:
        names[record_type] = _names(
            conn, record_type,
            [r["record_id"] for r in shown if r["record_type"] == record_type])
    records = [{
        "type": r["record_type"], "id": r["record_id"],
        "name": names.get(r["record_type"], {}).get(r["record_id"]) or r["record_id"],
        "owner": _owner_label(r, r["owner_id"]), "value": r["value"],
        "link": r["record_type"] in RECORD_TABLES,
        "snoozable": r["record_type"] in SNOOZE_FIELDS,
    } for r in shown]
    summary = SimpleNamespace(
        count=flagged_total, total_value=sum(o.total_value for o in rollups),
        by_owner=rollups) if rollups else None
    return {
        "signal": _rule(sig, _thresholds()),
        "summary": summary, "owner": owner, "records": records,
        "total": total, "page": page, "pages": pages, "page_size": PAGE_SIZE,
        "owner_names": {o.owner_id or NO_OWNER: o.label for o in rollups},
    }


def _present(conn: sqlite3.Connection, column: str, value) -> str:
    if value in (None, ""):
        return "—"
    if column == "hubspot_owner_id":
        row = conn.execute("SELECT * FROM owners WHERE id = ?", (value,)).fetchone()
        return _owner_label(row, str(value))
    if column == "dealstage":
        row = conn.execute("SELECT label FROM stages WHERE id = ?",
                           (value,)).fetchone()
        return row["label"] if row and row["label"] else str(value)
    if column == "pipeline":
        row = conn.execute("SELECT label FROM pipelines WHERE id = ?",
                           (value,)).fetchone()
        return row["label"] if row and row["label"] else str(value)
    if column == "amount_in_home_currency":
        return money(value)
    if column in _DAY_COLUMNS or column in _TIME_COLUMNS:
        return _stamp(value, day_only=column in _DAY_COLUMNS)
    return str(value)


def _fetch_reason(exc: Exception) -> str:
    if (isinstance(exc, httpx.HTTPStatusError)
            and exc.response.status_code in (401, 403)):
        return ("HubSpot rejected the token or its scopes; "
                "check the private app's read access")
    return "HubSpot could not be reached just now"


def _engagements(client, conn: sqlite3.Connection, record_type: str,
                 record_id: str) -> dict:
    """This record's engagements, fetched now. `unavailable` holds the reason.

    HubSpot v3 has no generic `engagements` object (see `crm.hubspot`), so the
    five real types are read one at a time and their ids unioned: five batch
    reads for this one record, on a page a person opened.

    The briefing never depends on this, so any failure (rate limit, network,
    a bad token) degrades to a notice instead of failing the page. One type
    failing costs only that type: what arrived is still shown, with the gap
    named, and only an all-five failure is reported as unavailable.
    """
    ids: list[str] = []
    failed: list[str] = []
    first_error: Exception | None = None
    for engagement_type in ENGAGEMENT_TYPES:
        try:
            links = client.batch_associations(
                record_type, engagement_type, [record_id])
        except Exception as exc:  # noqa: BLE001 - never fails the page
            log.warning("%s engagement fetch for %s %s failed: %s: %s",
                        engagement_type, record_type, record_id,
                        type(exc).__name__, exc)
            failed.append(engagement_type)
            first_error = first_error or exc
            continue
        ids.extend(str(i) for i in links.get(record_id, []))
    if first_error is not None and len(failed) == len(ENGAGEMENT_TYPES):
        return {"unavailable": _fetch_reason(first_error)}
    ids = list(dict.fromkeys(ids))
    partial = (f"{', '.join(failed)} could not be fetched just now, so this "
               "list is incomplete.") if failed else None
    found: list[sqlite3.Row] = []
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        marks = ", ".join("?" for _ in chunk)
        found += conn.execute(
            f"SELECT id, type, timestamp FROM engagements WHERE id IN ({marks})",
            chunk).fetchall()
    found.sort(key=lambda r: r["timestamp"] or 0, reverse=True)
    return {"total": len(ids), "missing": len(ids) - len(found),
            "partial": partial,
            "rows": [{"type": r["type"], "when": _stamp(r["timestamp"], False)
                      if r["timestamp"] else "—"}
                     for r in found[:ENGAGEMENTS_SHOWN]]}


# --- the app ---------------------------------------------------------------------

def create_app(cfg: Config, *, client=None, http=None,
               now_ms: Callable[[], int] | None = None) -> FastAPI:
    """Build the dashboard.

    `client` (a HubSpotClient or a fake) serves Sync now and record pages;
    `http` is the httpx client for Ollama; `now_ms` is the clock. All three
    default to the real thing and exist so tests make no network call.
    """
    now = now_ms or (lambda: int(time.time() * 1000))
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    seed = connect(cfg.db_path)
    try:
        migrate(seed)
    finally:
        seed.close()

    env = Environment(loader=FileSystemLoader(HERE / "templates"),
                      autoescape=select_autoescape(["html"]))
    env.filters["money"] = money
    app = FastAPI(title="HubSpot Watchdog", docs_url=None, redoc_url=None,
                  openapi_url=None)
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
    app.state.notice = None
    sync_lock = SYNC_LOCK   # shared with the serve timer

    @contextmanager
    def db():
        # One connection per request: sqlite3 connections are thread-bound and
        # sync routes run in a worker pool.
        connection = connect(cfg.db_path)
        try:
            yield connection
        finally:
            connection.close()

    def render(template: str, conn: sqlite3.Connection, **ctx) -> HTMLResponse:
        # A notice describes the last sync once; it must not outlive being shown.
        notice, app.state.notice = app.state.notice, None
        ctx.update(chrome=_chrome(conn, now(), notice))
        return HTMLResponse(env.get_template(template).render(**ctx))

    def hubspot(max_attempts: int | None = None):
        if client is not None:
            return client
        return HubSpotClient(cfg.hubspot_token,
                             **({"max_attempts": max_attempts} if max_attempts else {}))

    def same_origin(request: Request) -> None:
        """Refuse a cross-site POST (CSRF) against this localhost tool.

        A browser sends Origin (or at least Referer) on a cross-site form
        post; when one is present it must name this host. Requests with
        neither (curl, scripts) are not browser CSRF and pass.
        """
        host = request.headers.get("host", "")
        for header in ("origin", "referer"):
            value = request.headers.get(header)
            if value is not None and urlsplit(value).netloc != host:
                raise HTTPException(403, "Cross-origin request refused")

    @app.get("/", response_class=HTMLResponse)
    def index(owner: str | None = Query(None, max_length=64)):
        with db() as conn:
            return render("index.html", conn, ask=None, question="",
                          **_index_context(conn, now(), owner))

    @app.post("/ask", response_class=HTMLResponse, dependencies=[Depends(same_origin)])
    def ask_route(question: str = Form("", max_length=2000)):
        with db() as conn:
            result = ask(question, conn, cfg, http=http)
            return render("index.html", conn, ask=result, question=question,
                          **_index_context(conn, now(), None))

    @app.get("/signal/{signal_id}", response_class=HTMLResponse)
    def signal_page(signal_id: str, owner: str | None = Query(None, max_length=64),
                    page: int = Query(1, ge=1)):
        if signal_id not in SIGNAL_BY_ID:
            raise HTTPException(404, "No such signal")
        with db() as conn:
            return render("signal.html", conn,
                          **_signal_context(conn, now(), signal_id, owner, page))

    @app.get("/record/{record_type}/{record_id}", response_class=HTMLResponse)
    def record_page(record_type: str, record_id: str):
        if record_type not in RECORD_TABLES:
            raise HTTPException(404, "No such record type")
        with db() as conn:
            row = conn.execute(f"SELECT * FROM {record_type} WHERE id = ?",
                               (record_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "No such record")
            labels = LABELS[record_type]
            fields = [{"label": labels.get(c, c),
                       "value": _present(conn, c, row[c])}
                      for c in row.keys() if c not in ("id", "raw", "archived")]
            thresholds = _thresholds()
            flags = [_rule(SIGNAL_BY_ID[f["signal_id"]], thresholds)
                     for f in flags_for_record(conn, now(), record_type, record_id)]
            return render(
                "record.html", conn, record_type=record_type, record_id=record_id,
                title=row[{"deals": "dealname", "companies": "name"}.get(
                    record_type, "email")] or record_id,
                archived=bool(row["archived"]), fields=fields, flags=flags,
                hubspot_link=hubspot_url(conn, record_type, record_id),
                engagements=_engagements(hubspot(2), conn, record_type, record_id))

    @app.post("/snooze", dependencies=[Depends(same_origin)])
    def snooze_route(signal_id: str = Form(..., max_length=64),
                     record_id: str = Form(..., max_length=64),
                     record_type: str = Form("deals", max_length=32),
                     days: int = Form(..., ge=1, le=365)):
        with db() as conn:
            try:
                snooze(conn, signal_id, record_type, record_id,
                       now() + days * DAY_MS, now())
            except ValueError as exc:
                raise HTTPException(400, str(exc)) from exc
        return RedirectResponse(f"/signal/{signal_id}", status_code=303)

    @app.post("/sync", dependencies=[Depends(same_origin)])
    def sync_route():
        if not sync_lock.acquire(blocking=False):
            app.state.notice = "A sync is already running."
            return RedirectResponse("/", status_code=303)
        try:
            with db() as conn:
                report = sync_incremental(conn, hubspot())
            app.state.notice = ("Sync finished with errors: "
                                + "; ".join(report.errors)) if report.errors else None
        except Exception as exc:  # noqa: BLE001 - shown on the page, not a 500
            app.state.notice = f"Sync failed: {exc}"
        finally:
            sync_lock.release()
        return RedirectResponse("/", status_code=303)

    return app
