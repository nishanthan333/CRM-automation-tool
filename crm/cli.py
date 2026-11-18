"""The `watchdog` command: argument parsing and output, wired to existing code.

Exit codes: 0 done; 1 ran but needs attention (errors, blocked or skipped
types, HubSpot refused); 2 could not start (usage, missing or bad config);
130 interrupted.
"""
import argparse
import dataclasses
import logging
import os
import sqlite3
import sys
from pathlib import Path

import httpx
import uvicorn

from crm.config import Config, home_dir, load_config, signal_settings
from crm.db import connect, migrate
from crm.hubspot import HubSpotClient, RateLimited
from crm.sync import (
    SEARCH_RESULT_CAP,
    archive_eligible,
    backfill,
    backfill_state,
    estimate_backfill,
    reconcile,
    sync_archived,
    sync_incremental,
)
from crm.timer import SyncTimer

log = logging.getLogger("watchdog")
DEFAULTS = Path(__file__).parent / "defaults"
HOST = "127.0.0.1"          # create_app has no auth: never bind anything wider
PLACEHOLDER = "REPLACE-ME"


class CliError(Exception):
    """A problem the user can fix; printed without a traceback, exit 2."""


def make_client(cfg: Config) -> HubSpotClient:
    return HubSpotClient(cfg.hubspot_token)


# --- configuration ------------------------------------------------------------

def _load(home: Path) -> Config:
    """Config for every command but init. Fails loudly: running on defaults
    would silently ignore the user's timezone and week start."""
    config_path = home / "config.toml"
    if not config_path.exists():
        raise CliError(f"no config.toml in {home}. Run `watchdog init` there "
                       "first (or pass --home DIR / set WATCHDOG_HOME).")
    if not (home / "signals.toml").exists():
        raise CliError(f"no signals.toml in {home}. Run `watchdog init` to "
                       "create the default thresholds.")
    try:
        cfg = load_config(config_path)
        signal_settings(config_path)       # validates timezone and week start
    except (ValueError, OSError) as exc:   # TOMLDecodeError is a ValueError
        raise CliError(f"{config_path}: {exc}") from exc
    if PLACEHOLDER in cfg.hubspot_token:
        raise CliError(f"{config_path}: [hubspot] token is still the "
                       "placeholder. Paste your private app's access token.")
    if cfg.sync_interval_minutes < 1:
        raise CliError(f"{config_path}: [sync] interval_minutes must be >= 1")
    if not cfg.db_path.is_absolute():
        cfg = dataclasses.replace(cfg, db_path=home / cfg.db_path)
    return cfg


def _open(cfg: Config) -> sqlite3.Connection:
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(cfg.db_path)
    migrate(conn)
    return conn


def _never_backfilled(conn: sqlite3.Connection) -> list[str]:
    return [t for t, (phase, _) in backfill_state(conn).items() if phase is None]


def _blocked(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    return [(t, status or "blocked") for t, (phase, status)
            in backfill_state(conn).items() if phase == "blocked"]


def _say_blocked(blocked: list[tuple[str, str]]) -> None:
    for api_type, message in blocked:
        print(f"BLOCKED {api_type}: {message}", file=sys.stderr)
    print("A blocked type keeps its checkpoint and fails the same way on every "
          "re-run until the cause is fixed (see README: Blocked types need attention).",
          file=sys.stderr)


def _say_errors(errors: list[str]) -> None:
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if any("401" in e or "403" in e for e in errors):
        print(_TOKEN_HINT, file=sys.stderr)


_TOKEN_HINT = ("HubSpot rejected the token or its scopes. Check [hubspot] token "
               "in config.toml and the private app's scopes (README: Create "
               "the private app).")


# --- commands -----------------------------------------------------------------

def cmd_init(args, home: Path) -> int:
    home.mkdir(parents=True, exist_ok=True)
    for name, source in (("config.toml", "config.toml.example"),
                         ("signals.toml", "signals.toml")):
        target = home / name
        if target.exists():
            print(f"{target} already exists; left alone.")
        else:
            target.write_text((DEFAULTS / source).read_text())
            print(f"wrote {target}")
    print(f"\nNext: put your HubSpot private app token in {home / 'config.toml'},"
          "\nthen run `watchdog estimate`, and only then `watchdog backfill`.")
    return 0


def cmd_estimate(args, home: Path) -> int:
    cfg = _load(home)
    # The estimate only asks HubSpot for counts. It opens no database file, so
    # it cannot write one: a scratch in-memory connection satisfies the API.
    scratch = sqlite3.connect(":memory:")
    try:
        estimates = estimate_backfill(scratch, make_client(cfg))
    finally:
        scratch.close()
    print(f"{'type':<13}{'records':>14}{'list requests':>16}")
    total = 0
    capped = False
    for e in estimates:
        if e.records is None:
            records = "n/a (small)"
        elif e.records >= SEARCH_RESULT_CAP:
            records, capped = f"{e.records:,}+", True
        else:
            records = f"~{e.records:,}"
        total += e.requests
        print(f"{e.object_type:<13}{records:>14}{e.requests:>16,}")
    print(f"{'total':<13}{'':>14}{total:>16,}")
    print("\nCounts are approximate: HubSpot's search total may be capped at "
          f"{SEARCH_RESULT_CAP:,}, so a figure shown as N+ means at least N, "
          "not a count.")
    if capped:
        print("At least one type hit that ceiling; the real job is larger "
              "than shown.")
    print("Associations cost extra requests on top. Compare the total with "
          "your HubSpot daily API limit, and run `watchdog backfill` only if "
          "it fits. It takes hours and is resumable.")
    return 0


def cmd_backfill(args, home: Path) -> int:
    cfg = _load(home)
    conn = _open(cfg)
    last: dict[str, int] = {}

    def progress(api_type: str, count: int) -> None:
        if count - last.get(api_type, 0) >= 1000:
            last[api_type] = count
            print(f"  {api_type}: {count:,} records", flush=True)

    try:
        print("Backfilling. Safe to interrupt (Ctrl-C); re-run to resume.")
        report = backfill(conn, make_client(cfg), progress)
    except KeyboardInterrupt:
        print("\nInterrupted. Progress is saved; run `watchdog backfill` again "
              "to resume where it stopped.", file=sys.stderr)
        return 130
    except RateLimited as exc:
        print(f"HubSpot is rate limiting or unreachable: {exc}\nProgress is "
              "saved; wait, then re-run `watchdog backfill` to resume.",
              file=sys.stderr)
        return 1
    try:
        for object_type, count in report.per_type.items():
            print(f"{object_type}: {count:,} records this run")
        blocked = _blocked(conn)
        states = backfill_state(conn)
        incomplete = [t for t, (phase, _) in states.items() if phase != "complete"]
        _say_errors(report.errors)
        if blocked:
            _say_blocked(blocked)
            print("Backfill is NOT complete: blocked types are listed above.",
                  file=sys.stderr)
            return 1
        if incomplete or report.errors:
            print("Backfill is NOT complete (" + ", ".join(incomplete or ["errors above"])
                  + "). Fix the cause and re-run `watchdog backfill`.",
                  file=sys.stderr)
            return 1
        print("Backfill complete. Run `watchdog serve` to open the dashboard.")
        return 0
    finally:
        conn.close()


def scheduled_sync(cfg: Config) -> None:
    """One timer-driven incremental sync. Failures propagate to SyncTimer,
    which logs them and re-arms."""
    conn = _open(cfg)
    try:
        missing = _never_backfilled(conn)
        if missing:
            log.warning("skipping sync: never backfilled (%s); run `watchdog "
                        "backfill`", ", ".join(missing))
            return
        for api_type, message in _blocked(conn):
            log.warning("%s is blocked: %s", api_type, message)
        report = sync_incremental(conn, make_client(cfg))
        for error in report.errors:
            log.warning("sync: %s", error)
        log.info("sync finished: %s", report.per_type)
    finally:
        conn.close()


def cmd_sync(args, home: Path) -> int:
    cfg = _load(home)
    conn = _open(cfg)
    try:
        missing = _never_backfilled(conn)
        if missing:
            print("Not backfilled yet: " + ", ".join(missing) + ".\nRun "
                  "`watchdog estimate`, then `watchdog backfill`; an "
                  "incremental sync is not a substitute.", file=sys.stderr)
            return 2
        report = sync_incremental(conn, make_client(cfg))
        for object_type, count in report.per_type.items():
            print(f"{object_type}: {count:,} records changed")
        blocked = _blocked(conn)
        _say_errors(report.errors)
        if blocked:
            _say_blocked(blocked)
        if report.errors or blocked:
            return 1
        print("Sync complete; signals re-evaluated.")
        return 0
    finally:
        conn.close()


def cmd_reconcile(args, home: Path) -> int:
    """The weekly pass: archived records, then hard deletes."""
    cfg = _load(home)
    conn = _open(cfg)
    try:
        client = make_client(cfg)
        eligible = archive_eligible(conn)
        errors: list[str] = []
        if eligible:   # an empty list would make sync_archived do every type
            errors = sync_archived(conn, client, eligible).errors
        result = reconcile(conn, client)
        for object_type, count in result.items():       # not dict(result):
            print(f"{object_type}: {count:,} newly archived")  # keeps .skipped
        for object_type, reason in result.skipped.items():
            print(f"SKIPPED {object_type}: {reason}")
        _say_errors(errors)
        if result.skipped or errors:
            print("Some types were skipped or failed, so the weekly pass is "
                  "incomplete. A skip is not a clean zero.", file=sys.stderr)
            return 1
        print("Weekly pass complete. Newly archived records leave the "
              "briefing at the next sync.")
        return 0
    finally:
        conn.close()


def cmd_serve(args, home: Path) -> int:
    from crm.web import create_app
    cfg = _load(home)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    conn = _open(cfg)
    try:
        missing = _never_backfilled(conn)
        blocked = _blocked(conn)
    finally:
        conn.close()
    if missing:
        print("warning: not backfilled yet (" + ", ".join(missing) + "). The "
              "dashboard will be empty and scheduled syncs will skip until "
              "`watchdog backfill` has run.", file=sys.stderr)
    if blocked:
        _say_blocked(blocked)
    timer = SyncTimer(cfg.sync_interval_minutes * 60,
                      lambda: scheduled_sync(cfg))
    timer.start()
    print(f"Dashboard at http://{HOST}:{args.port}  (syncing every "
          f"{cfg.sync_interval_minutes} min; Ctrl-C to stop)")
    try:
        uvicorn.run(create_app(cfg), host=HOST, port=args.port)
    finally:
        timer.stop()
    return 0


# --- entry point --------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="watchdog",
        description="Read-only HubSpot watchdog: sync, signals, morning briefing.")
    parser.add_argument("--home", metavar="DIR",
                        help="directory holding config.toml and signals.toml "
                             "(default: $WATCHDOG_HOME, else the current directory)")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.add_parser("init", help="write config.toml and signals.toml here")
    sub.add_parser("estimate", help="size the backfill; writes nothing")
    sub.add_parser("backfill", help="initial load: hours, resumable")
    sub.add_parser("sync", help="incremental sync, then re-evaluate signals")
    sub.add_parser("reconcile", help="weekly: archived records and deletes")
    serve = sub.add_parser("serve", help="dashboard plus background sync")
    serve.add_argument("--port", type=int, default=8000)
    return parser


COMMANDS = {"init": cmd_init, "estimate": cmd_estimate, "backfill": cmd_backfill,
            "sync": cmd_sync, "reconcile": cmd_reconcile, "serve": cmd_serve}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_usage(sys.stderr)
        print("watchdog: choose a command (nothing runs by default).",
              file=sys.stderr)
        return 2
    # Set explicitly: home_dir() falls back to the cwd, and a process started
    # elsewhere would silently read no config.
    home = Path(args.home).expanduser().resolve() if args.home else home_dir().resolve()
    os.environ["WATCHDOG_HOME"] = str(home)
    try:
        return COMMANDS[args.command](args, home)
    except CliError as exc:
        print(f"watchdog: {exc}", file=sys.stderr)
        return 2
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        print(f"watchdog: HubSpot answered HTTP {code}. "
              + (_TOKEN_HINT if code in (401, 403) else
                 "Check the request in the message above and your HubSpot "
                 "account."), file=sys.stderr)
        return 1
    except (RateLimited, httpx.TransportError) as exc:
        print(f"watchdog: could not reach HubSpot: {exc}", file=sys.stderr)
        return 1
    except sqlite3.OperationalError as exc:
        print(f"watchdog: database error: {exc} (is another watchdog "
              "process using it?)", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
