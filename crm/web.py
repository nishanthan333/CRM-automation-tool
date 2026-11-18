"""The dashboard: a server-rendered FastAPI app over the local SQLite file.

Routes fetch and hand over; the markup lives in `crm/templates`. The page only
ever reads materialised signal results (written by `sync_incremental`), so it
renders on an empty database and never waits on HubSpot, except the record page,
which fetches that one record's engagement list on demand and degrades if it
fails. Everything that originates in HubSpot or the model is HTML-escaped by
Jinja's autoescape; nothing here marks a value `safe`.
"""

from __future__ import annotations

import math
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape

from crm.ask import ask
from crm.config import Config, home_dir, load_thresholds
from crm.db import connect, migrate
from crm.hubspot import HubSpotClient
from crm.signals import (SIGNALS, UNASSIGNED, briefing, explain,
                         flagged_records, total_at_risk)
from crm.snooze import SNOOZE_FIELDS, snooze
from crm.sync import sync_incremental

__all__ = ["LABELS", "PAGE_SIZE", "create_app"]

HERE = Path(__file__).parent
PAGE_SIZE = 50
OWNERS_SHOWN = 5            # owner rows on an index card before "… N more"
ENGAGEMENTS_SHOWN = 20
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


def hubspot_url(record_type: str, record_id: str) -> str:
    # The portal id is not synced, so the link relies on HubSpot resolving "_"
    # to the signed-in user's portal.
    return (f"https://app.hubspot.com/contacts/_/record/"
            f"{HUBSPOT_TYPE_ID[record_type]}/{record_id}")


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


def _chrome(conn: sqlite3.Connection, now_ms: int, notice) -> dict:
    """What every page's header needs: last sync, data-incomplete marker."""
    last = conn.execute(
        "SELECT MAX(last_run_at) AS t FROM sync_state"
        " WHERE object_type != 'signals'").fetchone()["t"]
    signals = _state(conn, "signals")
    partial = None
    if signals is not None and signals["phase"] == "partial":
        partial = signals["status"] or "data incomplete"
    return {"last_sync": ago(last, now_ms), "partial": partial, "notice": notice}


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
    summary = next((s for s in briefing(conn, now_ms)
                    if s.signal_id == signal_id), None)
    rows = flagged_records(conn, now_ms, signal_id)
    if owner == NO_OWNER:
        rows = [r for r in rows if r["owner_id"] is None]
    elif owner:
        rows = [r for r in rows if r["owner_id"] == owner]
    pages = max(math.ceil(len(rows) / PAGE_SIZE), 1)
    page = min(page, pages)
    shown = rows[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]
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
    return {
        "signal": _rule(sig, _thresholds()),
        "summary": summary, "owner": owner, "records": records,
        "total": len(rows), "page": page, "pages": pages, "page_size": PAGE_SIZE,
        "owner_names": {o.owner_id or NO_OWNER: o.label
                        for o in (summary.by_owner if summary else [])},
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


def _engagements(client, conn: sqlite3.Connection, record_type: str,
                 record_id: str) -> dict | None:
    """This record's engagements, fetched now. None means "unavailable".

    The briefing never depends on this, so any failure (rate limit, network,
    a bad token) degrades to a notice instead of failing the page.
    """
    try:
        links = client.batch_associations(record_type, "engagements", [record_id])
        ids = [str(i) for i in links.get(record_id, [])]
    except Exception:  # noqa: BLE001 - deliberately never fails the page
        return None
    found: list[sqlite3.Row] = []
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        marks = ", ".join("?" for _ in chunk)
        found += conn.execute(
            f"SELECT id, type, timestamp FROM engagements WHERE id IN ({marks})",
            chunk).fetchall()
    found.sort(key=lambda r: r["timestamp"] or 0, reverse=True)
    return {"total": len(ids), "missing": len(ids) - len(found),
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
    env.globals["hubspot_url"] = hubspot_url
    app = FastAPI(title="HubSpot Watchdog", docs_url=None, redoc_url=None,
                  openapi_url=None)
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
    app.state.notice = None
    sync_lock = threading.Lock()

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
        ctx.update(chrome=_chrome(conn, now(), app.state.notice))
        return HTMLResponse(env.get_template(template).render(**ctx))

    def hubspot(max_attempts: int | None = None):
        if client is not None:
            return client
        return HubSpotClient(cfg.hubspot_token,
                             **({"max_attempts": max_attempts} if max_attempts else {}))

    @app.get("/", response_class=HTMLResponse)
    def index(owner: str | None = Query(None, max_length=64)):
        with db() as conn:
            return render("index.html", conn, ask=None, question="",
                          **_index_context(conn, now(), owner))

    @app.post("/ask", response_class=HTMLResponse)
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
            flags = []
            for f in flagged_records(conn, now()):
                if f["record_type"] == record_type and f["record_id"] == record_id:
                    sig = SIGNAL_BY_ID[f["signal_id"]]
                    flags.append(_rule(sig, _thresholds()))
            return render(
                "record.html", conn, record_type=record_type, record_id=record_id,
                title=row[{"deals": "dealname", "companies": "name"}.get(
                    record_type, "email")] or record_id,
                archived=bool(row["archived"]), fields=fields, flags=flags,
                hubspot_link=hubspot_url(record_type, record_id),
                engagements=_engagements(hubspot(2), conn, record_type, record_id))

    @app.post("/snooze")
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

    @app.post("/sync")
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
