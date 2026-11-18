"""The briefing's rules: a declared table of signals and a small evaluator.

Each `Signal` carries the SQL that produces it, and the SQL is what runs, so the
card that shows "the rule and the SQL behind it" cannot drift from the code.
`evaluate_all` runs every enabled signal and materialises the hits into
`signal_results`; `briefing` and `total_at_risk` read only that table (plus
owner labels and snoozes), never the CRM tables.

Conventions every signal follows (global constraints):
  * every query filters `archived = 0`;
  * "open deal" is a join to `stages` with `is_closed = 0`, never a stage label;
  * activity is the record's own `hs_last_activity_date`, never `engagements`;
  * deal value is `amount_in_home_currency`, never `amount`;
  * thresholds are named SQL parameters bound from `signals.toml`.

Each query returns `id, hubspot_owner_id, amount_in_home_currency`, and
optionally a fourth `record_type` column when one signal spans several types.
A NULL amount or owner is data, not an error: the record is still flagged, adds
nothing to totals, and a NULL owner rolls up under "Unassigned".
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo
import sqlite3

__all__ = [
    "SIGNALS", "OwnerRollup", "Signal", "SignalSummary", "briefing",
    "evaluate_all", "explain", "flagged_records", "total_at_risk",
]

DAY_MS = 86_400_000
UNASSIGNED = "Unassigned"
SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
            "sunday"]


@dataclass(frozen=True)
class Signal:
    id: str
    title: str
    severity: str          # "high" | "medium" | "low"
    record_type: str       # what `id` refers to; a query may override per row
    sql: str
    explanation: str       # plain English; {threshold_name} filled by `explain`
    thresholds: tuple[str, ...] = ()   # signals.toml keys bound into `sql`
    enabled: bool = True


@dataclass
class OwnerRollup:
    owner_id: str | None
    label: str
    count: int
    total_value: float


@dataclass
class SignalSummary:
    signal_id: str
    title: str
    severity: str
    count: int
    total_value: float
    by_owner: list[OwnerRollup]


# --- the rules -----------------------------------------------------------------

_OPEN_DEAL = "JOIN stages s ON s.id = d.dealstage"
# A deal that has never had activity is as old as the deal; with no creation
# date either, it is treated as infinitely stale rather than silently skipped.
_LAST_ACTIVITY = "COALESCE(d.hs_last_activity_date, d.createdate, 0)"

SIGNALS: list[Signal] = [
    Signal(
        id="stalled_deal",
        title="Stalled deals",
        severity="high",
        record_type="deals",
        thresholds=("stalled_days",),
        sql=f"""
SELECT d.id, d.hubspot_owner_id, d.amount_in_home_currency
FROM deals d {_OPEN_DEAL}
WHERE d.archived = 0 AND s.is_closed = 0
  AND {_LAST_ACTIVITY} < :now_ms - :stalled_days * {DAY_MS}
""",
        explanation=(
            "An open deal (its stage is not marked closed in HubSpot) whose last "
            "activity is more than {stalled_days} days ago. A deal that has never "
            "had activity is dated from its creation; with neither recorded it "
            "is treated as infinitely stale. Ranked by deal value."),
    ),
    Signal(
        id="past_close_date",
        title="Past its close date",
        severity="high",
        record_type="deals",
        sql=f"""
SELECT d.id, d.hubspot_owner_id, d.amount_in_home_currency
FROM deals d {_OPEN_DEAL}
WHERE d.archived = 0 AND s.is_closed = 0
  AND d.closedate IS NOT NULL AND d.closedate < :today_date_ms
""",
        explanation=(
            "An open deal whose close date is an earlier calendar day than today "
            "in this machine's local timezone. HubSpot stores a close date as "
            "midnight UTC, so dates are compared as days, not instants: a deal "
            "closing today is not overdue in any timezone. A deal with no close "
            "date is not flagged."),
    ),
    Signal(
        id="big_and_quiet",
        title="Big and quiet",
        severity="high",
        record_type="deals",
        thresholds=("big_deal_value", "big_deal_quiet_days"),
        sql=f"""
SELECT d.id, d.hubspot_owner_id, d.amount_in_home_currency
FROM deals d {_OPEN_DEAL}
WHERE d.archived = 0 AND s.is_closed = 0
  AND d.amount_in_home_currency > :big_deal_value
  AND {_LAST_ACTIVITY} < :now_ms - :big_deal_quiet_days * {DAY_MS}
""",
        explanation=(
            "An open deal worth more than {big_deal_value:,} (amount in home "
            "currency) with no activity in {big_deal_quiet_days} days (a deal "
            "never touched is dated from its creation; with neither recorded "
            "it counts as quiet). Overlaps "
            "stalled deals on purpose: a big deal silent for a week is louder "
            "than a small one silent for a month. A deal with no amount is "
            "never counted as big."),
    ),
    Signal(
        id="untouched_account",
        title="Untouched accounts",
        severity="medium",
        record_type="companies",
        thresholds=("untouched_account_days",),
        sql=f"""
SELECT c.id, c.hubspot_owner_id, SUM(d.amount_in_home_currency) AS amount_in_home_currency
FROM companies c
JOIN associations a ON a.to_type = 'companies' AND a.to_id = c.id AND a.from_type = 'deals'
JOIN deals d ON d.id = a.from_id AND d.archived = 0
{_OPEN_DEAL} AND s.is_closed = 0
WHERE c.archived = 0
  AND COALESCE(c.hs_last_activity_date, 0) < :now_ms - :untouched_account_days * {DAY_MS}
GROUP BY c.id, c.hubspot_owner_id
""",
        explanation=(
            "A company with at least one open deal and no activity in "
            "{untouched_account_days} days (or none recorded at all). Its value "
            "is the sum of its open deals' amounts."),
    ),
    Signal(
        id="unowned_record",
        title="Unowned records",
        severity="medium",
        record_type="deals+companies",
        sql=f"""
SELECT d.id, d.hubspot_owner_id, d.amount_in_home_currency, 'deals' AS record_type
FROM deals d {_OPEN_DEAL}
WHERE d.archived = 0 AND s.is_closed = 0 AND COALESCE(d.hubspot_owner_id, '') = ''
UNION ALL
SELECT c.id, c.hubspot_owner_id, NULL, 'companies'
FROM companies c
WHERE c.archived = 0 AND COALESCE(c.hubspot_owner_id, '') = ''
  AND EXISTS (
    SELECT 1 FROM associations a
    JOIN deals d ON d.id = a.from_id AND d.archived = 0
    JOIN stages s ON s.id = d.dealstage AND s.is_closed = 0
    WHERE a.to_type = 'companies' AND a.to_id = c.id AND a.from_type = 'deals')
""",
        explanation=(
            "An open deal, or a company that has an open deal, with no owner "
            "assigned in HubSpot. Unowned records with nothing open are not "
            "flagged (that is CRM hygiene, not a morning action)."),
    ),
    Signal(
        id="stage_regression",
        title="Stage regression",
        severity="medium",
        record_type="deals",
        enabled=False,
        # Declared but inert. A deal that moved backwards needs stage history,
        # and the design doc leaves open whether that is affordable at this
        # account size (HubSpot's property-history API vs deriving it from
        # changes seen across syncs). Until that is resolved this must not run:
        # the query below can match nothing, so enabling it by mistake is
        # silent rather than wrong.
        sql=f"""
SELECT d.id, d.hubspot_owner_id, d.amount_in_home_currency
FROM deals d {_OPEN_DEAL}
WHERE d.archived = 0 AND s.is_closed = 0 AND 1 = 0
""",
        explanation=(
            "Not evaluated. Needs per-deal stage history, which is not synced "
            "yet; see the design doc's open question on stage history."),
    ),
    Signal(
        id="quiet_pipeline",
        title="Quiet pipeline",
        severity="medium",
        record_type="pipeline",
        thresholds=("quiet_pipeline_drop_pct",),
        # One synthetic row, keyed by the week's start date. This week's new
        # deals are compared with the same elapsed slice of each of the four
        # previous weeks, so a Monday-morning run is not measured against
        # complete weeks it could not have matched.
        sql="""
SELECT :week_label AS id, NULL AS hubspot_owner_id, NULL AS amount_in_home_currency
FROM (
  SELECT
    (SELECT COUNT(*) FROM deals d WHERE d.archived = 0
       AND d.createdate >= :week_start_ms AND d.createdate < :now_ms) AS this_week,
    (SELECT COUNT(*) FROM deals d WHERE d.archived = 0 AND (
         (d.createdate >= :prior1_start_ms AND d.createdate < :prior1_end_ms)
      OR (d.createdate >= :prior2_start_ms AND d.createdate < :prior2_end_ms)
      OR (d.createdate >= :prior3_start_ms AND d.createdate < :prior3_end_ms)
      OR (d.createdate >= :prior4_start_ms AND d.createdate < :prior4_end_ms)
    )) / 4.0 AS prior_avg
)
WHERE prior_avg > 0
  AND this_week < prior_avg * (1 - :quiet_pipeline_drop_pct / 100.0)
""",
        explanation=(
            "New deals created so far this week, compared with the same elapsed "
            "part of each of the previous four weeks. Flagged when this week is "
            "more than {quiet_pipeline_drop_pct}% below their average. Silent "
            "when the previous four weeks had no new deals."),
    ),
]

_ENABLED = [s for s in SIGNALS if s.enabled]
# Fail at import, not inside briefing(), on a mistyped severity.
assert all(s.severity in SEVERITY_RANK for s in SIGNALS), "unknown severity"


def explain(signal: Signal, thresholds: dict) -> str:
    """The signal's rule in plain English, with live threshold values."""
    return signal.explanation.format(**thresholds["thresholds"])


# --- evaluation ----------------------------------------------------------------

def _midnight_ms(day: date, tz: tzinfo | None) -> int:
    naive = datetime.combine(day, time.min)
    # A naive datetime is read as system-local time, DST rules included.
    aware = naive.replace(tzinfo=tz) if tz else naive.astimezone()
    return int(aware.timestamp() * 1000)


def _context(now_ms: int, tz: tzinfo | None, week_starts_on: str) -> dict:
    """Time boundaries bound into queries, all derived from `now_ms`."""
    name = str(week_starts_on).strip().lower()
    if name not in WEEKDAYS:
        raise ValueError(
            f"week_starts_on must be a weekday name, got {week_starts_on!r}")
    today = datetime.fromtimestamp(now_ms / 1000, tz).date()
    week_start = today - timedelta(days=(today.weekday() - WEEKDAYS.index(name)) % 7)
    week_start_ms = _midnight_ms(week_start, tz)
    elapsed = max(now_ms - week_start_ms, 0)
    ctx = {
        "now_ms": now_ms,
        # HubSpot dates are midnight UTC of the chosen day, so "today" is the
        # local calendar date rendered the same way, not local midnight.
        "today_date_ms": _midnight_ms(today, timezone.utc),
        "week_start_ms": week_start_ms,
        "week_label": week_start.isoformat(),
    }
    for k in range(1, 5):
        start = _midnight_ms(week_start - timedelta(weeks=k), tz)
        ctx[f"prior{k}_start_ms"] = start
        ctx[f"prior{k}_end_ms"] = start + elapsed
    return ctx


def evaluate_all(conn: sqlite3.Connection, thresholds: dict, now_ms: int,
                 tz: tzinfo | None = None, week_starts_on: str = "monday",
                 skip: frozenset[str] = frozenset()) -> int:
    """Run every enabled signal and replace `signal_results` with the hits.

    `tz=None` means the machine's local timezone. All queries run before any
    write, and the delete and reinserts share one transaction, so a failing
    signal (a missing threshold, bad SQL) leaves the previous results intact.
    Signals named in `skip` are not run and their previous results are kept
    (used when the data they depend on is known to be incomplete).
    Returns the number of rows written.
    """
    context = _context(now_ms, tz, week_starts_on)
    values = thresholds["thresholds"]
    rows = []
    for sig in _ENABLED:
        if sig.id in skip:
            continue
        params = {**context, **{name: values[name] for name in sig.thresholds}}
        for r in conn.execute(sig.sql, params).fetchall():
            rows.append((
                sig.id, str(r[0]), r[3] if len(r) > 3 else sig.record_type,
                r[1] or None, r[2], now_ms))
    with conn:
        marks = ", ".join("?" for _ in skip)
        conn.execute(f"DELETE FROM signal_results WHERE signal_id NOT IN ({marks})",
                     sorted(skip))
        conn.executemany(
            "INSERT INTO signal_results (signal_id, record_id, record_type,"
            " owner_id, value, computed_at) VALUES (?, ?, ?, ?, ?, ?)", rows)
    return len(rows)


# --- reading the briefing --------------------------------------------------------

def flagged_records(conn: sqlite3.Connection, now_ms: int | None = None,
                    signal_id: str | None = None) -> list[sqlite3.Row]:
    """Materialised flags with owner label columns, highest value first.

    With `now_ms`, records under an unexpired snooze for that signal are left
    out; with None, nothing is hidden. Results of a disabled or since-removed
    signal are never returned.
    """
    ids = [s.id for s in _ENABLED if signal_id in (None, s.id)]
    marks = ", ".join("?" for _ in ids)
    snooze = ("LEFT JOIN snoozes z ON z.signal_id = r.signal_id"
              " AND z.record_type = r.record_type AND z.record_id = r.record_id"
              " AND z.until > ?") if now_ms is not None else ""
    visible = "AND z.signal_id IS NULL" if now_ms is not None else ""
    sql = f"""
SELECT r.signal_id, r.record_id, r.record_type, r.owner_id, r.value,
       o.first_name, o.last_name, o.email
FROM signal_results r
LEFT JOIN owners o ON o.id = r.owner_id
{snooze}
WHERE r.signal_id IN ({marks}) {visible}
ORDER BY r.value DESC, r.record_id
"""
    params = ([now_ms] if now_ms is not None else []) + ids
    return conn.execute(sql, params).fetchall()


def _owner_label(row: sqlite3.Row) -> str:
    if row["owner_id"] is None:
        return UNASSIGNED
    name = " ".join(p for p in (row["first_name"], row["last_name"]) if p)
    return name or row["email"] or row["owner_id"]


def briefing(conn: sqlite3.Connection, now_ms: int) -> list[SignalSummary]:
    """One summary per signal with something flagged, most urgent first.

    Order: severity (high first), then value at stake, then count. Per-signal
    figures are per-signal; for a day-level total use `total_at_risk`.
    """
    by_signal: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in flagged_records(conn, now_ms):
        by_signal[row["signal_id"]].append(row)

    out = []
    for sig in _ENABLED:
        rows = by_signal.get(sig.id)
        if not rows:
            continue
        owners: dict[str | None, OwnerRollup] = {}
        for r in rows:
            rollup = owners.setdefault(
                r["owner_id"], OwnerRollup(r["owner_id"], _owner_label(r), 0, 0.0))
            rollup.count += 1
            rollup.total_value += r["value"] or 0.0
        by_owner = sorted(owners.values(),
                          key=lambda o: (-o.total_value, -o.count, o.label))
        out.append(SignalSummary(
            signal_id=sig.id, title=sig.title, severity=sig.severity,
            count=len(rows), total_value=sum(r["value"] or 0.0 for r in rows),
            by_owner=by_owner))
    out.sort(key=lambda s: (SEVERITY_RANK[s.severity], -s.total_value,
                            -s.count, s.signal_id))
    return out


def total_at_risk(conn: sqlite3.Connection, now_ms: int | None = None) -> float:
    """Money behind all flags, each deal counted once.

    Signals overlap by design (a deal can be both stalled and big-and-quiet), so
    this sums over distinct deal ids. Only deal flags carry money: a company's
    value is its deals' amounts, which are already counted. A NULL amount counts
    as 0. Pass `now_ms` to leave snoozed flags out, matching the briefing.
    """
    per_deal: dict[str, float] = {}
    for r in flagged_records(conn, now_ms):
        if r["record_type"] == "deals":
            per_deal[r["record_id"]] = max(
                per_deal.get(r["record_id"], 0.0), r["value"] or 0.0)
    return float(sum(per_deal.values()))
