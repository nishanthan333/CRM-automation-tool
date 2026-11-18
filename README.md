# HubSpot Watchdog

A local, read-only watchdog for one HubSpot account. It copies your CRM into a
SQLite file on this machine, runs seven SQL "signals" over it (stalled deals,
quiet big deals, untouched accounts, and so on) and serves a per-owner morning
briefing in your browser, plus a question box backed by a local Ollama model.

- **It never writes to HubSpot.** Every request is a read.
- **No CRM data leaves the machine.** The only outbound calls are to HubSpot and
  to Ollama on `localhost`; the Ollama prompt carries table and column names,
  never row data.

Requires Python 3.12+.

```
pip install -e .          # installs the `watchdog` command
```

(`python -m crm ...` works too.)

## Setup, in order

Do these in order. Skipping ahead is how you end up waiting an hour for nothing.

### 1. Create a HubSpot private app

In HubSpot: Settings, Integrations, Private Apps, Create a private app. Give it
**read-only** scopes and nothing else:

| Scope | Why |
| --- | --- |
| `crm.objects.contacts.read` | contacts, and calls/meetings/notes/tasks |
| `crm.objects.companies.read` | companies |
| `crm.objects.deals.read` | deals, deal pipelines and stages |
| `crm.objects.owners.read` | owner names for the briefing |
| `sales-email-read` | emails (the `emails` engagement type) |

Copy the access token (`pat-...`). **Verify this list against your HubSpot UI**:
scope names change from time to time, and it was not checked against a live
portal. In particular, the account-info call that learns your portal id (used
for record links) may need a scope this table omits. If a command reports
**HTTP 403** for an object type or for account info, the private app is missing
that read scope: add it and re-run.

### 2. `watchdog init`

```
mkdir ~/watchdog && cd ~/watchdog
watchdog init
```

This writes `config.toml` and `signals.toml` into the current directory and
never overwrites an existing file. Everything the tool needs (config, thresholds,
database under `data/`) lives in that directory. Commands find it through
`$WATCHDOG_HOME`, else the current directory, or pass `--home DIR` to any
command. The CLI sets `WATCHDOG_HOME` itself, so the background sync always reads
the same `config.toml` as the command that started it.

Edit `config.toml`:

- `[hubspot] token`: paste the token. Commands refuse to run on the placeholder.
- `[local] timezone` and `week_starts_on`: dates in the briefing are bucketed in
  this timezone, and weeks start on this day (default `monday`). `"local"` means
  this machine's timezone.
- `[sync] interval_minutes`: how often `serve` syncs (default 15).

A missing `config.toml` is an error, not a fallback to defaults: running on
defaults would silently ignore your timezone and week start.

### 3. `watchdog estimate`, before anything else

```
watchdog estimate
```

Asks HubSpot how many records of each type exist and how many requests the
backfill will take. It **writes nothing**: no database is opened or created.

Read the numbers before you commit to the backfill:

- Counts are **approximate**. HubSpot's search total may be capped at 10,000, so
  a figure shown as `10,000+` is a floor ("at least"), not a count, and the real
  job is bigger.
- The request total covers the list calls only. Association lookups cost extra.
- Compare it with your account's daily API limit (it depends on your HubSpot
  tier and can be as low as 250,000 a day).

### 4. `watchdog backfill`

```
watchdog backfill
```

The initial load. **It takes hours** on a real account; engagements (calls,
emails, meetings, notes, tasks) are the bulk and are loaded last, after the
companies, deals and contacts the briefing needs.

- **It is resumable and safe to interrupt.** Press Ctrl-C, close the lid, lose
  the network: progress is checkpointed page by page. Run `watchdog backfill`
  again and it continues from the last completed page. Finished types cost
  nothing on a re-run.
- Rate limiting (HTTP 429) is retried with backoff. If HubSpot stays
  unavailable the command stops with its checkpoint intact; re-run it.
- Nothing else syncs your data while it runs; `sync` and `serve` skip types that
  are still backfilling.

It prints `Backfill complete` only when every type finished. **Do not treat any
other ending as done.**

#### Blocked types need attention

If HubSpot rejects a type with a client error (a bad request, a permission
problem, an unreadable record), that type is **blocked**: its checkpoint is kept
and the command prints

```
BLOCKED deals: blocked: <the error>
Backfill is NOT complete
```

and exits non-zero. Re-running does not fix this: it resumes into the same
failure every time, because discarding the checkpoint would mean re-reading
everything already loaded. Read the message, fix the cause (usually a missing
scope on the private app, or an expired token), and re-run. `sync` and `serve`
also report blocked types, in the terminal or the log, on every run. A blocked
type keeps the briefing's numbers for that object stale, so do not ignore it.

### 5. `watchdog serve`

```
watchdog serve            # http://127.0.0.1:8000  (--port N to change)
```

Starts the dashboard and a background timer that runs an incremental sync at
startup and then every `interval_minutes`. A sync that fails (HubSpot down, rate limit, a locked
database) is logged and the next one still runs; one bad sync does not stop the
rest. Until the backfill has run, every sync (the timer, the dashboard's Sync
now button, `watchdog sync`) refuses to sync a never-backfilled type, rather
than quietly walking the whole account through the search endpoint; `serve`
also warns at startup.

Things to know:

- **It binds to 127.0.0.1 only and has no authentication.** Do not put it behind
  a tunnel or proxy that exposes it.
- **"Sync now" and the question box block their request.** Sync now waits for the
  whole sync; a question waits for the model, up to 120 seconds. The page looks
  hung while it waits; it is not.
- **Record links appear after the first sync.** Links back to HubSpot need your
  portal id and UI domain (EU accounts are not `app.hubspot.com`), which a sync
  learns and stores. Until then a record page says so instead of linking.
- **What is safe to run beside `serve`.** `watchdog estimate` always (it opens no
  database). `watchdog sync` and `watchdog reconcile` also work, but they and
  the server's own syncs write the same database, so one waits for the other
  (up to 60 seconds before reporting "database is locked", after which just
  retry). The timer and Sync now never overlap each other. `watchdog backfill`
  beside `serve` is allowed and writes page by page, but while it runs the
  server skips the types it is still loading. Never run two `serve` or two
  `backfill` processes against one database directory.
- The question box needs Ollama running locally with the model set under
  `[ollama]` in `config.toml`. Only read-only `SELECT`s run, and only against
  the local database.

### 6. `watchdog reconcile`, weekly

```
watchdog reconcile
```

`serve` runs this pass itself when it falls due (more than seven days since the
last one), right after a scheduled sync, so a long-running server keeps its own
promise. Run the command by hand if you do not leave `serve` running, or to
force a pass now. The 15-minute sync cannot see records that were
archived or deleted in HubSpot, and the two passes that find them are too
expensive to run every 15 minutes (the archived listing has no "changed since"
filter, so it is re-read in full). This command runs both: it marks archived
records, then finds hard-deleted ones by comparing every id with HubSpot's.

Until it runs, an archived or deleted record can stay in the briefing for up to a
week. Records leave the briefing at the next sync after it runs. Every page's
header says how long ago archives were last checked, and calls it overdue past
ten days, so a clean briefing can be read for what it is.

The id walk is also read the other way: a record HubSpot lists as live again is
un-archived locally. Without that, an archive — whether a real one later undone
in HubSpot, or one this pass got wrong — could never be cleared, because the
incremental sync only sees records modified since its watermark.

It only touches types whose backfill is complete, and it refuses to archive
anything on an id walk that looks truncated (a wrong archive would silently drop
a live deal). "Truncated" is measured against HubSpot's own count *and* the
number of live rows held locally, whichever is larger, so neither a capped
search total nor a mass archive in HubSpot can quietly empty the briefing. Such types are listed as `SKIPPED <type>: <reason>`, which is
different from `0 newly archived`, and the command exits non-zero. A skip means
the weekly pass did not happen for that type.

## Your first briefing will flag a lot. That is expected.

The thresholds in `signals.toml` (days stalled, deal value counted as "big",
and so on) are starting guesses, not facts about your pipeline. **Expect the
first briefing to flag a great many records**, most of which are old, quiet
data rather than real problems. This is the design's own warning, not a bug.

Tune `signals.toml` against your own data, then run `watchdog sync` (or wait for
the next scheduled sync) to re-evaluate. The dashboard shows each signal's rule
and the thresholds it used, and you can snooze a record. A briefing you have
tuned to flag a handful of things per owner is useful; the untuned one is
noise and will be ignored.

## Command reference

| Command | What it does | Exit codes |
| --- | --- | --- |
| `watchdog init` | write `config.toml` and `signals.toml` if absent | 0 |
| `watchdog estimate` | size the backfill; writes nothing | 0, 1 HubSpot error |
| `watchdog backfill` | initial load; resumable | 0 complete; 1 blocked/incomplete; 130 interrupted |
| `watchdog sync` | incremental sync, then re-evaluate signals | 0; 1 errors or blocked; 2 never backfilled |
| `watchdog reconcile` | weekly archived and delete pass | 0; 1 skipped or errors |
| `watchdog serve` | dashboard and background sync | 0 |

Exit code 2 from any command means it could not start: no `config.toml`, a
placeholder token, an unknown timezone. The message says what to do.
Running `watchdog` with no command prints usage and does nothing.

## Tests

```
pip install -e '.[dev]'
python -m pytest -W error
```

No test calls HubSpot or Ollama.
