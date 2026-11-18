"""Snooze: hide a flagged record for a while, unless it materially changes.

Hiding is read in `crm.signals.flagged_records` (an anti-join on unexpired
snoozes). This module is the write path and the early-expiry rule: a snooze
stores a fingerprint of the record's material fields, and a later sync drops it
if that fingerprint no longer matches, because a new amount, stage or owner is
new information rather than the same nag. Nothing here touches HubSpot.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time

from crm.signals import SIGNALS

# record_type (the local table, as stored in snoozes/signal_results) -> columns
# whose change is material. A rename or any other field is deliberately absent.
SNOOZE_FIELDS: dict[str, tuple[str, ...]] = {
    "deals": ("amount_in_home_currency", "dealstage", "hubspot_owner_id"),
    "companies": ("hubspot_owner_id",),
}


def fingerprint(conn: sqlite3.Connection, record_type: str,
                record_id: str) -> str:
    """Stable digest of a record's material fields.

    NULLs hash as JSON null, so an unset field is stable run to run; a record
    that is absent hashes as all-null. Types with no material fields hash to a
    constant and so never go stale.
    """
    fields = SNOOZE_FIELDS.get(record_type, ())
    values: list = [None] * len(fields)
    if fields:
        cols = ", ".join(f'"{c}"' for c in fields)
        row = conn.execute(
            f'SELECT {cols} FROM "{record_type}" WHERE id = ?',
            (record_id,)).fetchone()
        if row is not None:
            values = list(row)
    return hashlib.sha256(
        json.dumps(values, separators=(",", ":")).encode()).hexdigest()


def snooze(conn: sqlite3.Connection, signal_id: str, record_type: str,
           record_id: str, until_ms: int, now_ms: int | None = None) -> None:
    """Hide one record under one signal until `until_ms` (replaces any prior)."""
    if record_type not in SNOOZE_FIELDS:
        raise ValueError(f"cannot snooze record_type {record_type!r}; "
                         f"expected one of {sorted(SNOOZE_FIELDS)}")
    if signal_id not in {sig.id for sig in SIGNALS if sig.enabled}:
        raise ValueError(f"cannot snooze unknown or disabled signal {signal_id!r}")
    created = int(time.time() * 1000) if now_ms is None else now_ms
    record_id = str(record_id)
    with conn:
        conn.execute(
            "INSERT INTO snoozes (signal_id, record_type, record_id, until,"
            " created_at, fingerprint) VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(signal_id, record_type, record_id) DO UPDATE SET"
            " until = excluded.until, created_at = excluded.created_at,"
            " fingerprint = excluded.fingerprint",
            (signal_id, record_type, record_id, until_ms, created,
             fingerprint(conn, record_type, record_id)))


def active_snoozes(conn: sqlite3.Connection,
                   now_ms: int) -> set[tuple[str, str, str]]:
    """(signal_id, record_type, record_id) of every unexpired snooze."""
    return {(r[0], r[1], r[2]) for r in conn.execute(
        "SELECT signal_id, record_type, record_id FROM snoozes WHERE until > ?",
        (now_ms,))}


def drop_stale_snoozes(conn: sqlite3.Connection) -> int:
    """Delete every snooze whose record's material fields have since changed.

    Checks all snoozed rows, so it does not matter which path (incremental,
    backfill, reconcile) changed the record. Returns how many were dropped.
    A snooze with no stored fingerprint cannot be judged and is kept: that is
    the safe direction (at worst it expires on schedule).
    """
    dropped = 0
    with conn:
        for signal_id, record_type, record_id, stored in conn.execute(
                "SELECT signal_id, record_type, record_id, fingerprint"
                " FROM snoozes").fetchall():
            if stored is None or stored == fingerprint(
                    conn, record_type, record_id):
                continue
            conn.execute(
                "DELETE FROM snoozes WHERE signal_id = ? AND record_type = ?"
                " AND record_id = ?", (signal_id, record_type, record_id))
            dropped += 1
    return dropped
