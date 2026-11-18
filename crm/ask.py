"""Question box: English question -> SQL (local Ollama model) -> rows.

The model is a small local one, so its job is as narrow as we can make it: it
writes one SELECT against a schema we derive from sqlite_master. The SQL is
always returned to the caller, including when it fails -- seeing the query is
how a person spots that a weak model picked the wrong column.

Defence in depth, because model output is untrusted:
  1. `guard_sql` -- exactly one statement, SELECT/WITH only, no comments, no
     write/ATTACH/PRAGMA keywords outside string literals.
  2. `run_query` opens the database read-only (`mode=ro`), so even a guard
     bypass cannot write.
  3. A SQLite authorizer refuses ATTACH/DETACH/PRAGMA outright.
  4. A row cap and a wall-clock deadline (progress handler).

Only table and column names are ever sent to Ollama, never row data.
"""
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from crm.config import Config
from crm.db import connect_readonly

QUERY_TIMEOUT_S = 5.0
ROW_CAP = 500
MODEL_TIMEOUT_S = 120.0
# Columns the model should never see or query: the raw JSON blob.
_HIDDEN_COLUMNS = {"raw"}


class UnsafeSQL(ValueError):
    """The generated text is not a single read-only SELECT."""


class QueryTimeout(Exception):
    """The query ran past its deadline and was interrupted."""


class ModelUnavailable(Exception):
    """The local model could not be reached or refused the request."""


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple]
    truncated: bool = False


@dataclass
class AskResult:
    """`sql` is whatever the model produced (stripped), set on every path that
    got that far; "" only if no model output exists. `error` is None on
    success. On failure `rows` is [] and `columns` is []."""
    sql: str = ""
    rows: list[tuple] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    error: str | None = None
    truncated: bool = False


# --- prompt ----------------------------------------------------------------

def schema_prompt(conn: sqlite3.Connection) -> str:
    """`table(col, col, ...)` lines from sqlite_master. Names only, no data."""
    lines = []
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
        " AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    for (name,) in [tuple(t) for t in tables]:
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{name}")')
                if r[1] not in _HIDDEN_COLUMNS]
        lines.append(f"{name}({', '.join(cols)})")
    return "\n".join(lines)


_INSTRUCTIONS = """You write one SQLite SELECT query that answers a question about a CRM database.
Reply with the SQL only: no explanation, no markdown, no comments, one statement.
Rules:
- Only read data. Use SELECT (optionally with a WITH clause).
- Dates are epoch milliseconds in UTC.
- Deal value is amount_in_home_currency (never amount).
- Only the object tables (contacts, companies, deals, engagements, owners) have archived; always filter archived = 0 on those and never reference it elsewhere.
- The owner of a record is hubspot_owner_id; names are in owners.

Schema:
{schema}

Examples:
Q: How many open deals are there?
SQL: select count(*) from deals d join stages s on s.id = d.dealstage where d.archived = 0 and s.is_closed = 0

Q: What is the total open pipeline value?
SQL: select sum(d.amount_in_home_currency) from deals d join stages s on s.id = d.dealstage where d.archived = 0 and s.is_closed = 0

Q: Which owners have the most deals?
SQL: select o.first_name, o.last_name, count(*) as deals from deals d join owners o on o.id = d.hubspot_owner_id where d.archived = 0 group by o.id order by deals desc

Q: Which companies have no recorded activity?
SQL: select id, name from companies where archived = 0 and hs_last_activity_date is null

Q: {question}
SQL:"""


def generate_sql(question: str, schema: str, cfg: Config, http=None) -> str:
    """Ask the local model for SQL; returns its raw text.

    `http` is anything with httpx's `post` (an httpx.Client, or the module);
    tests inject a stub transport. Any failure to get an answer from Ollama is
    ModelUnavailable.
    """
    http = http or httpx
    url = f"{cfg.ollama_url.rstrip('/')}/api/generate"
    body = {
        "model": cfg.ollama_model,
        "prompt": _INSTRUCTIONS.format(schema=schema, question=question.strip()),
        "stream": False,
        "options": {"temperature": 0},
    }
    try:
        resp = http.post(url, json=body, timeout=MODEL_TIMEOUT_S)
        resp.raise_for_status()
        return str(resp.json()["response"])
    except httpx.HTTPStatusError as exc:
        raise ModelUnavailable(
            f"Ollama answered {exc.response.status_code} for model "
            f"{cfg.ollama_model!r} (is it pulled?)") from exc
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
        raise ModelUnavailable(f"Ollama is not reachable at {cfg.ollama_url}") from exc


_FENCE = re.compile(r"```[a-zA-Z]*\s*\n?(.*?)```", re.S)
_SQL_LINE_START = re.compile(r"^[ \t]*(select|with)\b", re.I | re.M)


def strip_model_output(raw: str) -> str:
    """Remove code fences and any leading prose up to the first line starting
    with select/with. Raises UnsafeSQL if there is no such line (never guesses
    a mid-sentence match, which would rewrite what the model said)."""
    text = raw.strip().lstrip("﻿")
    fenced = _FENCE.search(text)
    if fenced:
        text = fenced.group(1)
    else:
        text = text.replace("```", "")
    m = _SQL_LINE_START.search(text)
    if not m:
        raise UnsafeSQL("the model did not return SQL")
    return text[m.start():].strip()


# --- guard -----------------------------------------------------------------

_FORBIDDEN = {
    "attach", "detach", "pragma", "insert", "update", "delete", "drop",
    "create", "alter", "vacuum", "reindex", "analyze", "begin",
    "commit", "rollback", "savepoint", "release", "truncate",
}
_CLOSERS = {"'": "'", '"': '"', "`": "`", "[": "]"}


def _scan(sql: str) -> tuple[str, str]:
    """Walk `sql` honouring quotes. Returns (normalised, code) where `code` is
    the text with quoted parts blanked. Raises UnsafeSQL on a comment or an
    unterminated quote outside a literal."""
    out: list[str] = []
    code: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c in _CLOSERS:
            j = sql.find(_CLOSERS[c], i + 1)
            while j != -1 and c != "[" and sql[j + 1:j + 2] == c:  # doubled quote
                j = sql.find(c, j + 2)
            if j == -1:
                raise UnsafeSQL("unterminated quote")
            out.append(sql[i:j + 1])
            code.append(" ")
            i = j + 1
            continue
        if sql.startswith("--", i) or sql.startswith("/*", i):
            raise UnsafeSQL("comments are not allowed: remove the SQL comment")
        if c.isspace():
            if out and out[-1] != " ":
                out.append(" ")
            code.append(" ")
        else:
            out.append(c)
            code.append(c)
        i += 1
    return "".join(out).strip(), "".join(code)


def guard_sql(sql: str) -> str:
    """Return the cleaned single read-only statement, or raise UnsafeSQL."""
    text = sql.lstrip("﻿ \t\r\n").rstrip()
    if not text:
        raise UnsafeSQL("empty query")
    body, code = _scan(text)
    if body.endswith(";"):
        body, code = body[:-1].rstrip(), code.rstrip()[:-1]
    if not body:
        raise UnsafeSQL("empty query")
    if ";" in code:
        raise UnsafeSQL("exactly one statement is allowed")
    if not sqlite3.complete_statement(body + ";"):
        raise UnsafeSQL("incomplete statement")
    words = re.findall(r"[a-z_][a-z0-9_]*", code.lower())
    if not re.match(r"(select|with)\b", body, re.I):
        raise UnsafeSQL("only SELECT queries are allowed")
    if re.search(r"\breplace\b(?!\s*\()", code.lower()):
        raise UnsafeSQL("forbidden keyword: replace")  # `WITH ... REPLACE INTO`
    bad = _FORBIDDEN.intersection(words)
    if bad:
        raise UnsafeSQL(f"forbidden keyword: {sorted(bad)[0]}")
    return body


# --- execution -------------------------------------------------------------

# Keyword-independent backstop: whatever the guard misses, a statement that
# would write or change schema is refused by SQLite itself.
_DENIED_ACTIONS = {
    sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH, sqlite3.SQLITE_PRAGMA,
    sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
    sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_DROP_TABLE,
}


def _authorizer(action, *_args):
    return sqlite3.SQLITE_DENY if action in _DENIED_ACTIONS else sqlite3.SQLITE_OK


def run_query(db_path: Path, sql: str, timeout_s: float,
              row_cap: int) -> QueryResult:
    """Run `sql` on a read-only connection with a deadline and a row cap.

    Raises QueryTimeout past the deadline, sqlite3 errors otherwise. Does not
    guard: callers pass guarded SQL, and this stays safe if they do not.
    """
    conn = connect_readonly(db_path)
    deadline = time.monotonic() + timeout_s
    expired = False

    def check() -> int:
        nonlocal expired
        if time.monotonic() > deadline:
            expired = True
            return 1  # non-zero aborts the statement
        return 0

    try:
        conn.set_authorizer(_authorizer)
        conn.set_progress_handler(check, 1000)
        try:
            cur = conn.execute(sql)
            fetched = cur.fetchmany(row_cap + 1)
        except sqlite3.OperationalError:
            if expired:
                raise QueryTimeout(
                    f"query exceeded {timeout_s:g}s and was stopped") from None
            raise
        columns = [d[0] for d in cur.description or []]
        rows = [tuple(r) for r in fetched[:row_cap]]
        return QueryResult(columns, rows, truncated=len(fetched) > row_cap)
    finally:
        conn.close()


# --- orchestration ---------------------------------------------------------

def _db_path(conn: sqlite3.Connection) -> Path:
    for row in conn.execute("PRAGMA database_list"):
        if row[1] == "main" and row[2]:
            return Path(row[2])
    raise ValueError("ask() needs a file-backed connection, not :memory:")


def ask(question: str, conn: sqlite3.Connection, cfg: Config,
        http=None) -> AskResult:
    """Answer `question`. Never raises for model/SQL failures: returns an
    AskResult with `error` set and `sql` populated whenever the model got far
    enough to produce any."""
    if not question.strip():
        return AskResult(error="Type a question first.")
    db_path = _db_path(conn)
    try:
        raw = generate_sql(question, schema_prompt(conn), cfg, http=http)
    except ModelUnavailable as exc:
        return AskResult(error=str(exc))
    sql = raw.strip()
    try:
        sql = strip_model_output(raw)
        sql = guard_sql(sql)
        result = run_query(db_path, sql, QUERY_TIMEOUT_S, ROW_CAP)
    except UnsafeSQL as exc:
        return AskResult(sql=sql, error=f"Refused to run this query: {exc}")
    except QueryTimeout as exc:
        return AskResult(sql=sql, error=f"Query timed out: {exc}")
    except sqlite3.Error as exc:
        return AskResult(sql=sql, error=f"Query failed: {exc}")
    return AskResult(sql=sql, rows=result.rows, columns=result.columns,
                     truncated=result.truncated)
