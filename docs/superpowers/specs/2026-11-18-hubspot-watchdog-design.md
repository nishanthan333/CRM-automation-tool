# Local HubSpot Watchdog — Design

**Date:** 2026-11-18
**Status:** approved 2026-11-18

## Intent and why

A CRM is a passive database. It will happily tell you "deals by stage" but it
will never tap you on the shoulder and say *this deal has gone quiet and it's
worth $80k*. You have to go looking, which means you mostly don't.

This tool syncs our organisation's HubSpot into a local SQLite database, runs a
set of rules over it, and shows a short ranked briefing of things that need
attention — plus a box where you can ask questions in plain English.

It runs entirely on one machine. CRM data never leaves it, including the AI
part: question answering goes through a local model via Ollama rather than a
hosted API. That constraint is deliberate. Customer data is sensitive, and a
tool that ships it to someone else's cloud is a tool that dies in legal review.

## Goals

- A **morning briefing**: a short, ranked list of things in the CRM that need
  attention today, worth reading every day.
- A **correct, resumable incremental sync** of HubSpot CRM objects. If the sync
  is not trustworthy, nothing above it is.
- **Drill-down**: every flagged item leads to the actual records behind it, and
  shows the rule that flagged it.
- A **question box** answering plain-language questions about the local data,
  via a local model that writes SQL.
- Starts with **one command** and no cloud dependencies.

## Non-goals

- Multi-tenancy, login, user accounts. Single user, single HubSpot account.
- **Writing back to HubSpot.** v1 is strictly read-only.
- Data hygiene — dedupe, normalisation, filling gaps. A strong later phase,
  explicitly out of scope now because it requires write access and a much
  higher correctness bar.
- **Correcting, cleaning or reinterpreting HubSpot's data.** It is taken at face
  value: if HubSpot says a deal is worth £40k and was last touched on Tuesday,
  so do we. No inferred fields, no "we think this is really a duplicate", no
  normalisation. Consequently **record views present fields with HubSpot's own
  labels and layout**, so that anything on screen here can be checked against
  the same record in HubSpot without translation. When the two disagree, the
  tool is wrong.
- Other data sources (email, calendar, support tickets). Later.
- Hosting, deployment, packaging for other people.

## Anti-goals

Things that would make this a failure even if every feature worked:

- **A wall of vanity charts.** If the briefing isn't worth reading daily, the
  project failed regardless of how much it displays.
- **Unexplainable flags.** If you can't see why something was flagged, you will
  stop trusting it within a week. Every signal shows its rule and its records.
- **A sync you can't trust.** Silent data drift is worse than no tool, because
  you'd act on it.
- **CRM data leaving the machine.**

## Constraints

- **Auth:** a HubSpot *private app* bearer token, one account. No OAuth flow,
  no callback server, no app review.
- **Rate limits:** roughly 100–190 requests per 10 seconds plus a daily account
  cap, returning `429` with `errorType: RATE_LIMIT` when exceeded. These numbers
  come from third-party write-ups and must be verified against HubSpot's own
  usage guidelines during the build. The sync must back off and resume rather
  than assume a number.
- **Search caps at ~10,000 results**, so the search endpoint cannot do the
  initial backfill. Backfill goes through the list endpoints; only deltas go
  through search.
- **Association changes are invisible to modified-date search.** When a deal's
  linked contacts change, the deal's modified date may not move. Associations
  must be re-fetched for records we already know changed, and a full
  association refresh must be available.
- **Property naming differs per object:** contacts use `lastmodifieddate`,
  everything else uses `hs_lastmodifieddate`. Timestamps are epoch milliseconds.
- **The local model is weak at SQL.** Assume a 7B-class model on a laptop. Its
  job must be narrowed as far as possible and its output must always be visible.
- Python 3.12+. SQLite via the standard library.

Two constraints come from the owner and shape everything below:

- **The account is large** — hundreds of thousands to millions of records in the
  biggest object types. This is the single most consequential constraint in the
  document. The initial backfill is a long-running, resumable, checkpointed job
  measured in hours, not a step you run and watch. Sync state must survive a
  crash *mid-object-type*. Signal results must be precomputed rather than
  queried on page load. Every signal column needs an index.
- **The briefing covers the whole account, grouped by owner.** The reader is
  looking at the team, not their own pipeline. Owner is therefore a first-class
  dimension: `hubspot_owner_id` is indexed, every signal aggregates by it, and
  the briefing rolls up per owner with an explicit "unowned" bucket — which is
  itself signal 5, so unowned records surface rather than hide in a residual.

## Approaches

Three were considered.

| | Approach | Trade-off |
|---|---|---|
| **A** | **One Python process. SQLite on disk, signals as SQL, FastAPI serving a server-rendered page.** | Plain-looking dashboard; complex interactive charts get awkward. ~400 lines. |
| B | Next.js front end, SQLite behind it, sync as a background job. | A genuinely nice UI and an easier path to a hosted product — at roughly three times the code for the same insight, with two runtimes and a build step for one person's localhost tab. |
| C | No dashboard. A CLI printing a morning briefing. | The honest minimum, and probably the part you'd actually read — but no browsing and no drill-down. |

**Chosen: A.** The value of this tool lives entirely in the sync being correct
and the signals being well-chosen, and neither improves with a nicer front end.
B's extra surface area would eat exactly the time that should go into the
signals, and A can grow a Next.js front end later against the same SQLite file
if the plain page is outgrown — nothing is wasted.

C was dropped as the product but it won an argument: **the dashboard leads with
the briefing** rather than burying it under charts.

## Design

### Storage

A single SQLite file, `data/crm.db` (gitignored).

One table per HubSpot object: `contacts`, `companies`, `deals`, `engagements`,
`owners`. Each stores the properties the rules need as **real columns**, plus a
`raw` JSON column holding the complete API payload, and an `archived` flag.

The split matters: signals are SQL, so any field a signal touches has to be a
column — but keeping `raw` means adding a new field later is a local migration
rather than a full re-backfill of the HubSpot account.

**Deal value uses `amount_in_home_currency`, not `amount`** *(my call)*. If the
portal ever records deals in more than one currency, summing `amount` is simply
wrong, and every value threshold in `signals.toml` becomes incomparable across
records. Using the home-currency field is correct in the multi-currency case and
harmless in the single-currency one.

Supporting tables:

- `associations(from_type, from_id, to_type, to_id)` — one flat table, rewritten
  per record whose associations we re-fetch.
- `sync_state(object_type, last_modified_ms, cursor, phase, last_run_at,
  status)` — the high-water mark per object type **plus the in-flight `after`
  cursor and phase**. At this account size a crash cannot be allowed to restart
  an object type from the beginning, so the cursor is checkpointed every page.
- `signal_results(signal_id, record_id, record_type, owner_id, value,
  computed_at)` — signals are **evaluated during sync and materialised here**,
  not run against the object tables on page load. At millions of rows, a
  dashboard that evaluates seven aggregate queries per request is a dashboard
  nobody opens twice.
- `pipelines(id, label)` and `stages(id, pipeline_id, label, display_order,
  is_closed, probability)` — synced from HubSpot's `crm/v3/pipelines` endpoint,
  **not hardcoded** *(my call)*. Five signals depend on "open deal", the
  dashboard renders stages in order with their labels, and signal 6 needs stage
  ordering. Hardcoding a pipeline's stage ids would break silently the first
  time someone edits the pipeline in HubSpot, and a silently wrong pipeline
  chart is precisely the trust failure the anti-goals name.
- `snoozes(signal_id, record_id, until, created_at)` — see Signals.

**Indexes are not an optimisation here, they're a requirement.** At minimum:
`hubspot_owner_id`, `hs_last_activity_date`, `dealstage`, `closedate`,
`archived`, and `amount_in_home_currency` on deals; `hubspot_owner_id`,
`hs_last_activity_date` and `archived` on companies; the modified-date column on
every table; and both directions of `associations`.

### What counts as "activity"

Three signals turn on "no activity in N days", and the definition drives the
whole sync budget, so it is settled here.

**Activity means HubSpot's own rollup properties on the record** —
`hs_last_activity_date` on deals and companies, with `notes_last_contacted`
available as a secondary field — **not a join against engagement records**
*(my call)*.

The alternative would be to treat an associated engagement (note, call, email,
meeting, task) as the source of truth. That makes `engagements` — typically the
highest-volume object in a portal by an order of magnitude — correctness-critical:
it would have to be backfilled across all history *and* fully associated to every
deal and company, which would dominate sync time, the rate-limit budget, and the
association refresh design. For a tool whose entire value is a daily briefing,
that is a large amount of machinery to arrive at a date HubSpot already
maintains for us.

So engagements are still synced, but only to populate drill-downs — "what was
the last thing that happened here" — and a gap in them degrades detail, never
correctness. *(Ponytail: one column instead of a high-volume join. Ceiling — we
inherit HubSpot's definition of activity, including whatever it counts that we
might not. Upgrade path is a derived `last_activity_at` computed from
engagements, swapped in behind the same column name if the rollup turns out to
behave wrongly for us.)*

### Time and calendar

All HubSpot timestamps are epoch milliseconds in UTC. Signals reason about
"days", "this week" and "last quarter", so the boundaries are pinned *(my
call)*: bucketing happens in the **machine's local timezone**, weeks start
**Monday**, quarters are **calendar** quarters. All three sit in `config.toml`
so a fiscal calendar is a config change, not a code change.

This is pedantic for a reason. Near a boundary, the wrong convention makes
signal 7's "sharp drop" fire on a week that didn't happen — a plausible-looking
wrong number, which is the worst failure this tool can produce.

### Sync

**`sync --backfill`** is a distinct, long-running job, not a flag on the normal
path. It walks the list endpoints per object type, paging on the `after` cursor,
and **writes the cursor to `sync_state` after every page**. Interrupt it, reboot,
run it again tomorrow — it picks up from the last page it finished. Expect hours
at this account size. It prints progress per object type so it's possible to
tell whether it's working or wedged.

Order matters: `owners` and `pipelines` first (small, and everything references
them), then `companies` and `deals` (what the signals need), then `contacts`,
then `engagements` last. If the backfill is interrupted permanently halfway, the
tool is still useful, because the objects the briefing depends on are already
down.

**`sync`** (the default, incremental) does, per object type: a search filtered
on modified date greater than the stored watermark, paged to exhaustion; then
re-fetches associations for exactly the record ids that came back; then
re-evaluates signals into `signal_results`.

Rules that keep it honest:

- The watermark advances **only when an object type completes**, and the
  **page cursor is checkpointed as it goes**. A crash resumes from the last
  completed page, not from the start of the type and not past the gap.
- The watermark is rewound by a small margin (~60s) on each read, to tolerate
  clock skew and same-millisecond writes. Re-processing a few records is free;
  missing one is not.
- **Associations are fetched through the batch read endpoint** (up to 100 source
  ids per request), and only for the object pairs the signals actually use:
  deal↔company, deal↔contact, company↔contact. Engagement associations are not
  synced in bulk; a record's engagements are fetched on demand when someone
  opens its drill-down. At this scale a complete association graph would cost
  more than everything else in the tool combined and the briefing would not be
  one row different for it.
- Writes are upserts keyed on HubSpot id, so overlap is harmless.
- A `429` triggers exponential backoff and resumes from the stored cursor.
- **Pipelines and stages are refreshed on every run**, full or incremental.
  They're two small requests and everything downstream depends on them.

**Deletions and archives** *(my call)*. Modified-date search never returns
deleted or archived records, so without handling them a deal deleted in HubSpot
would sit in SQLite being flagged as stalled forever — the exact silent drift
the anti-goals call a failure. Three layers:

1. Each incremental run also pages the list endpoints with `archived=true` and
   sets the local `archived` flag. Cheap, and catches the common case, since
   most "deletions" in HubSpot are archives.
2. A **reconciliation pass** (`sync --reconcile`) pages the list endpoints for
   ids only — no properties, so it's far cheaper than a backfill — and marks
   anything local and not seen as archived. Weekly, not per-run, because at this
   account size it is not free.
3. Every signal query filters `archived = 0`.

Hard deletes that skip the archive state are only caught by layer 2, so a
hard-deleted record can linger until the next reconcile. That's accepted: it's a
rare path, and the failure mode is one stale row rather than a wrong aggregate.

Cadence: on startup, then every 15 minutes on a background timer, plus a
"Sync now" button. *(Ponytail: a `threading.Timer` in-process, not a scheduler
or a job queue. Ceiling — it dies with the process and won't sync while the
tool is closed. Upgrade path is a launchd plist or cron calling `sync` if
background syncing turns out to matter.)*

### Signals

A signal is a declared row: `id`, `title`, `severity`, a SQL query, and
threshold parameters. Thresholds live in `signals.toml` so they can be tuned
without touching code — which matters, because the right number depends on our
sales cycle and nobody knows it yet.

**v1 signal set — these are my picks, and the main thing to check while you
read:**

1. **Stalled deal** — open deal with no activity in N days (default 14), ranked
   by deal value.
2. **Past its close date** — open deal whose expected close date has passed.
3. **Big and quiet** — open deal above a value threshold with no activity in 7
   days. Deliberately overlaps #1: a $200k deal silent for a week deserves a
   louder flag than a $2k deal silent for a month.
4. **Untouched account** — company with an open deal and no activity in 30 days.
5. **Unowned record** — open deal or company with no owner assigned.
6. **Stage regression** — deal that moved backwards in the pipeline. *Carries a
   risk: this needs stage history, and whether we get it cheaply from HubSpot's
   property-history API or have to derive it from changes we observe across syncs
   is unresolved. If it's expensive, this signal ships in a later phase.*
7. **Quiet pipeline** — new deals created this week against the trailing
   four-week average, flagged on a sharp drop.

Each renders as a card: title, count, headline number ("7 stalled deals ·
$142k at risk"), and on expansion the affected records plus the rule in plain
English and the SQL that produced it.

**Snooze** *(my call — include it)*. A list you read every morning will show
you the same items tomorrow, and the fastest way to make a daily briefing
unreadable is to make it un-actionable. Each flagged record gets "I've seen
this, hide it for a week", stored in the local `snoozes` table. It's local
state only; nothing is written to HubSpot. A snooze is dropped early if the
record's data changes materially (value, stage, or owner), because that's new
information rather than the same nag.

**De-duplicated totals** *(my call)*. Signals 1 and 3 overlap deliberately, so
a $200k deal can appear in both. Per-signal numbers stay per-signal — the card
says what the card found. But any day-level total is computed over distinct deal
ids, so that deal is counted once. Double-counted money in a headline destroys
trust faster than almost anything else here.

### Dashboard

FastAPI with Jinja2 templates. Server-rendered, one page plus drill-downs. No
front-end framework; a little vanilla JS for the question box.

```
┌───────────────────────────────────────────────────────────────┐
│  HubSpot Watchdog               last sync 4m ago  [Sync now]  │
├───────────────────────────────────────────────────────────────┤
│  NEEDS ATTENTION          · 1,284 flags · $4.1M at risk       │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ ▾ Stalled deals            412 · $1.8M at risk   HIGH   │  │
│  │     R. Mehta        88 · $410k                          │  │
│  │     J. Osei         71 · $386k                          │  │
│  │     A. Lindqvist    64 · $295k                          │  │
│  │     … 9 more owners                                     │  │
│  │ ▸ Past close date          190 · $740k           HIGH   │  │
│  │ ▸ Big and quiet             31 · $1.2M           HIGH   │  │
│  │ ▸ Untouched accounts       588                   MED    │  │
│  │ ▸ No owner assigned         63 · $212k           LOW    │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                               │
│  PIPELINE                        [ all owners ▾ ]             │
│   Qualify  ████████░░  2,140 · $18.3M                         │
│   Demo     ██████░░░░    860 · $22.1M                         │
│   Propose  ███░░░░░░░    310 · $14.8M                         │
│   Closing  ██░░░░░░░░     95 · $6.2M                          │
│                                                               │
│  ASK ──────────────────────────────────────────────────────    │
│  [ which deals died after the demo stage last quarter?     ]  │
└───────────────────────────────────────────────────────────────┘
```

Every signal card expands to its owner breakdown first, and an owner expands to
the records. That ordering is the point of the per-owner choice: at this volume
"412 stalled deals" is not actionable, but "88 of them are R. Mehta's" is a
conversation. Record lists are paginated and capped; the briefing never tries to
render four hundred rows.

Routes: `GET /`, `GET /signal/{id}`, `GET /record/{type}/{id}`, `POST /ask`,
`POST /sync`, `POST /snooze`.

### Question box

The local model's job is narrowed as hard as possible:

- The prompt carries **the schema only** — table and column names, no row data —
  plus a handful of example question→SQL pairs.
- The model returns SQL and nothing else.
- **Guard before execution:** reject anything that isn't a single `SELECT`. No
  multiple statements, no `ATTACH`, no `PRAGMA`, no writes. Execute on a
  read-only SQLite connection with a row cap. Belt and braces, because the
  model will produce garbage sometimes and garbage shouldn't be able to drop a
  table.
- **A query timeout** (via SQLite's progress handler), because at this row count
  a model-written query missing a join condition will otherwise hang the page.
  On timeout, show the SQL and say it was too expensive rather than failing
  silently.
- **The generated SQL is always displayed above the results.** This is the
  feature, not a debug affordance: a weak model's wrong answer is obvious when
  you can see it queried the wrong column.
- Model name is config, defaulting to a coder-tuned 7B, swappable without code
  changes.
- If Ollama isn't running, the box says so plainly and the rest of the dashboard
  works untouched.

### Layout

```
crm/
  config.py      token, thresholds, model name
  hubspot.py     API client: auth, paging, backoff
  db.py          schema, migrations, upserts
  sync.py        full + incremental, watermarks, associations
  signals.py     signal definitions, evaluation
  ask.py         prompt construction, SQL guard, execution
  web.py         FastAPI app and routes
  templates/
signals.toml     thresholds
config.toml      gitignored; config.toml.example committed
data/crm.db      gitignored
```

### Testing

- The HubSpot client is tested against **recorded JSON fixtures**. No test makes
  a live API call.
- Sync gets tests for the cases that actually bite: resume after a mid-type
  crash, watermark advance, association re-fetch, `429` backoff.
- Every signal gets a test against a seeded SQLite database.
- The SQL guard gets **adversarial** tests — writes, stacked statements,
  `PRAGMA`, `ATTACH`.
- No test touches Ollama. SQL generation is tested with a stubbed model.

### Left to the builder

- The exact HubSpot property list per object type.
- Template structure and CSS.
- Backoff constants and page sizes.
- Whether deal stage history comes from HubSpot's property-history API or is
  derived locally from observed changes.
- Ollama prompt wording and the example question→SQL pairs.

### Risks

| Risk | Mitigation |
|---|---|
| **The backfill is the riskiest part of the build.** Hours long, against an API with a daily cap, on an account large enough that a design mistake costs a day to discover | Checkpoint every page; object types ordered so the briefing works before the backfill finishes; dry-run mode that counts records per type and estimates requests and wall-clock time *before* committing to the full run |
| Default thresholds will flag thousands of records and the briefing will be useless on day one | Expected, not a surprise. Thresholds live in `signals.toml` and the first real task after the backfill is tuning them against actual data. Severity ordering and per-owner rollup exist precisely so a big list stays navigable while being tuned |
| Signal 6 depends on stage history being cheaply available — at this volume a property-history backfill may be prohibitive | Ships in a later phase if so. Deriving it from changes observed across syncs is the fallback and costs nothing extra |
| The local model may be too weak to be genuinely useful | SQL is always visible, there's a query timeout, and the watchdog stands alone without it |
| Rate limit numbers are unverified | Verify against HubSpot's usage guidelines during the build; back off adaptively on `429` rather than hardcoding a ceiling |
| The in-process 15-minute timer won't sync while the tool is closed | Accepted for v1; a launchd plist calling `sync` is the upgrade path |
