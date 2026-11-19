#!/usr/bin/env python3
"""Fill a local database with a fictional mortgage brokerage, so the dashboard
can be looked at without a HubSpot account.

Harbour & Vale Finance Group, an Australian broker: seven brokers, two pipelines
(purchase and refinance/investment), referral partners (agencies, builders,
conveyancers, accountants) as companies, borrowers as contacts, and one loan
file per deal.

The data is shaped to make every enabled signal fire, including the awkward
cases a real portal will have: deals with no owner, loans with no amount, a
company whose activity was never recorded, and a slow week.

    python scripts/seed_demo.py [--home DIR]

Writes config.toml, signals.toml and the database into DIR (default: ./demo),
replacing any database already there. Then: watchdog --home DIR serve
"""
import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crm import config, db, signals  # noqa: E402

DAY = 86_400_000
NOW = int(datetime.now(timezone.utc).timestamp() * 1000)
RNG = random.Random(20261118)   # reproducible: the same portal every run

# Loan amounts in AUD, not commission: it is the deal amount a brokerage puts in
# HubSpot, and it makes "at risk" read as the pipeline a manager worries about.
# A Sydney median purchase is around a million, so "big" has to start above it
# or two thirds of the book is a big deal.
BIG_LOAN = 1_500_000

OWNERS = [
    ("51", "priya.raghunathan", "Priya", "Raghunathan"),
    ("52", "d.nguyen", "Dieu", "Nguyen"),
    ("53", "s.clarke", "Siobhan", "Clarke"),
    ("54", "m.webb", "Marcus", "Webb"),
    ("55", "y.tanaka", "Yuki", "Tanaka"),
    ("56", "e.macleod", "Ewan", "MacLeod"),
    ("57", "f.alrashid", "Fatima", "Al-Rashid"),
]
OWNER_IDS = [o[0] for o in OWNERS]

PIPELINES = [("purchase", "Residential Purchase"), ("refi", "Refinance & Investment")]

# (id, pipeline, label, order, is_closed, probability)
STAGES = [
    ("p_lead",         "purchase", "New Enquiry",            1, 0, 0.10),
    ("p_preapproval",  "purchase", "Pre-Approval",           2, 0, 0.25),
    ("p_application",  "purchase", "Application Submitted",  3, 0, 0.45),
    ("p_processing",   "purchase", "Valuation Ordered",      4, 0, 0.60),
    ("p_underwrite",   "purchase", "Conditional Approval",   5, 0, 0.75),
    ("p_cleartoclose", "purchase", "Unconditional Approval", 6, 0, 0.92),
    ("p_funded",       "purchase", "Settled",                7, 1, 1.00),
    ("p_dead",         "purchase", "Declined / Withdrawn",   8, 1, 0.00),
    ("r_enquiry",      "refi",     "Rate Review",            1, 0, 0.10),
    ("r_application",  "refi",     "Application Submitted",  2, 0, 0.40),
    ("r_appraisal",    "refi",     "Valuation Ordered",      3, 0, 0.65),
    ("r_underwrite",   "refi",     "Credit Assessment",      4, 0, 0.80),
    ("r_funded",       "refi",     "Settled",                5, 1, 1.00),
    ("r_dead",         "refi",     "Withdrawn",              6, 1, 0.00),
]
OPEN_PURCHASE = [s[0] for s in STAGES if s[1] == "purchase" and not s[4]]
OPEN_REFI = [s[0] for s in STAGES if s[1] == "refi" and not s[4]]

PARTNERS = [
    ("Cascade & Reed Realty", "cascadereed.com.au"),
    ("Thornbury Property Group", "thornburyproperty.com.au"),
    ("Willowmere Homes", "willowmerehomes.com.au"),
    ("Keystone Conveyancing", "keystoneconveyancing.com.au"),
    ("Aldergrove Constructions", "aldergrove.com.au"),
    ("Marchetti & Osei Accountants", "marchettiosei.com.au"),
    ("Blue Harbour Relocations", "blueharbourreloc.com.au"),
    ("Sandalwood Realty Partners", "sandalwoodrealty.com.au"),
    ("Pinecrest Developments", "pinecrestdev.com.au"),
    ("Varela Building Inspections", "varelainspect.com.au"),
    ("Northgate Settlements", "northgatesettlements.com.au"),
    ("Hollis & Day Property", "hollisday.com.au"),
    ("Brackenridge Capital", "brackenridgecap.com.au"),
    ("Sunfield Strata Management", "sunfieldstrata.com.au"),
    ("Orchard Lane Estate Agents", "orchardlane.com.au"),
    ("Tamarind Residential", "tamarindres.com.au"),
    ("Quarrystone Buyers Agents", "quarrystone.com.au"),
    ("Lindholm Realty", "lindholmrealty.com.au"),
    ("Everly Court Developments", "everlycourt.com.au"),
    ("Castellan Financial Advisers", "castellanfa.com.au"),
    ("Redbud Homes", "redbudhomes.com.au"),
    ("Ferncliff Estate Agency", "ferncliff.com.au"),
    ("Mayweather Conveyancers", "mayweatherconvey.com.au"),
    ("Silverbrook Builders", "silverbrookbuild.com.au"),
    ("Opaline Realty", "opalinerealty.com.au"),
]

STREETS = [
    "Boronia Rd", "Wattle St", "Jacaranda Ave", "Bottlebrush Cres",
    "Kookaburra Pde", "Banksia St", "Lillypilly Ct", "Melaleuca Dr",
    "Currawong Rd", "Grevillea Gr", "Casuarina Way", "Kurrajong St",
    "Coolibah Cres", "Waratah Ave", "Tallowwood Rise", "Paperbark Pl",
    "Bellbird Ln", "Myrtle St", "Hakea Cl", "Ironbark Rd",
    "Sturt Pde", "Lachlan St", "Macquarie Rd", "Barossa Way",
    "Nullarbor Cres", "Yarrabee Rd", "Kialla Ct", "Mirrabooka Ave",
]
# Suburb and state, the way an Australian listing is addressed.
CITIES = ["Ryde NSW", "Coburg VIC", "Toowong QLD", "Glenelg SA",
          "Subiaco WA", "Newtown NSW", "Geelong VIC", "Chermside QLD",
          "Fremantle WA", "Kingston TAS"]

FIRST = ["Alina", "Jonah", "Mei", "Tobias", "Rosa", "Dmitri", "Keisha", "Arun",
         "Freya", "Mateo", "Noor", "Callum", "Ingrid", "Hassan", "Petra",
         "Oyelaran", "Sunniva", "Rafael", "Thandiwe", "Lukas", "Amara",
         "Soren", "Giulia", "Nnamdi", "Esme", "Kiran", "Lars", "Beatriz",
         "Omar", "Saoirse", "Tariq", "Linnea", "Desmond", "Yara", "Piotr",
         "Adaeze", "Halvard", "Juno", "Rashid", "Clementine",
         "Darcy", "Shazia", "Thanh", "Bridie", "Ngaire", "Kostas"]
LAST = ["Vaszary", "Okafor", "Lindqvist", "Belmonte", "Nakamura", "Ferreira",
        "Abernathy", "Sandoval", "Krishnan", "Haverford", "Dlamini", "Rosetti",
        "Thorsen", "Castellano", "Mwangi", "Petrov", "Alvarado", "Fitzwilliam",
        "Osei", "Bergstrom", "Kowalczyk", "Nakashima", "Varela", "Hollings",
        "Adeyemi", "Marchetti", "Solberg", "Quintero", "Bhattacharya",
        "Winterbourne", "Fontaine", "Zielinski", "Marsden", "Achebe", "Lund",
        "Papadopoulos", "Tran", "Mackellar", "Hoang", "Whitlam", "Chidiac"]


def ts(days_ago: float) -> int:
    return NOW - int(days_ago * DAY)


def midnight(days_from_now: int) -> int:
    """A close date, the way HubSpot stores one: midnight UTC on that day."""
    day = datetime.now(timezone.utc).date() + timedelta(days=days_from_now)
    return int(datetime(day.year, day.month, day.day,
                        tzinfo=timezone.utc).timestamp() * 1000)


def raw(**fields) -> str:
    return json.dumps({"properties": {k: str(v) for k, v in fields.items()
                                      if v is not None}})


def build_companies() -> list[dict]:
    rows = []
    for i, (name, domain) in enumerate(PARTNERS):
        # Four partners have gone quiet for over a month and still have live
        # loans with us; two were never logged against at all.
        if i < 4:
            activity = ts(RNG.uniform(34, 95))
        elif i in (4, 5):
            activity = None
        else:
            activity = ts(RNG.uniform(0.2, 26))
        # Two referral partners nobody owns.
        owner = None if i in (6, 7) else RNG.choice(OWNER_IDS)
        rows.append({
            "id": f"c{2000 + i}", "name": name, "domain": domain,
            "hubspot_owner_id": owner, "hs_last_activity_date": activity,
            "hs_lastmodifieddate": ts(RNG.uniform(0, 12)), "archived": 0,
            "raw": raw(name=name, domain=domain),
        })
    # One partner that closed its doors: archived in HubSpot.
    rows.append({
        "id": "c2900", "name": "Harlow Crest Realty (closed)",
        "domain": "harlowcrest.com.au", "hubspot_owner_id": "53",
        "hs_last_activity_date": ts(210), "hs_lastmodifieddate": ts(180),
        "archived": 1, "raw": raw(name="Harlow Crest Realty"),
    })
    return rows


def build_contacts(n: int) -> list[dict]:
    rows = []
    for i in range(n):
        first, last = RNG.choice(FIRST), RNG.choice(LAST)
        rows.append({
            "id": f"ct{5000 + i}", "firstname": first, "lastname": last,
            "email": f"{first.lower()}.{last.lower()}@example.com",
            "hubspot_owner_id": RNG.choice(OWNER_IDS + [None]),
            "hs_last_activity_date": None if i % 23 == 0 else ts(RNG.uniform(0.1, 120)),
            "lastmodifieddate": ts(RNG.uniform(0, 30)), "archived": 0,
            "raw": raw(firstname=first, lastname=last),
        })
    return rows


def _deal(n, stage, pipeline, owner, amount, activity_days, close_days,
          created_days, archived=0):
    # A file cannot have been worked before it was opened; where the ranges
    # would say otherwise, the activity sits just after the create date.
    if activity_days is not None and activity_days > created_days:
        activity_days = max(created_days - RNG.uniform(0.1, 1.5), 0.05)
    addr = f"{RNG.randrange(12, 980)} {RNG.choice(STREETS)}"
    kind = "Purchase" if pipeline == "purchase" else RNG.choice(
        ["Refinance", "Equity Release", "Investment Purchase"])
    name = f"{addr}, {RNG.choice(CITIES)} - {kind}"
    return {
        "id": f"d{9000 + n}", "dealname": name, "dealstage": stage,
        "pipeline": pipeline, "amount_in_home_currency": amount,
        "closedate": None if close_days is None else midnight(close_days),
        "hubspot_owner_id": owner,
        "hs_last_activity_date": None if activity_days is None else ts(activity_days),
        "hs_lastmodifieddate": ts(RNG.uniform(0, 8)),
        "createdate": ts(created_days), "archived": archived,
        "raw": raw(dealname=name, dealstage=stage, pipeline=pipeline),
    }


def build_deals() -> list[dict]:
    """Loan files, laid out so each signal has something to find.

    The weekly intake is deliberately uneven: four healthy prior weeks and a
    thin current one, which is what `quiet_pipeline` compares.
    """
    rows, n = [], 0

    def add(**kw):
        nonlocal n
        rows.append(_deal(n, **kw))
        n += 1

    def loan():
        return round(RNG.triangular(320_000, 2_200_000, 680_000), -3)

    # Healthy, recently-touched work in both pipelines.
    for _ in range(96):
        pipe = "purchase" if RNG.random() < 0.68 else "refi"
        add(stage=RNG.choice(OPEN_PURCHASE if pipe == "purchase" else OPEN_REFI),
            pipeline=pipe, owner=RNG.choice(OWNER_IDS), amount=loan(),
            activity_days=RNG.uniform(0.1, 9), close_days=RNG.randrange(3, 75),
            created_days=RNG.uniform(10, 150))

    # Stalled: open past the 14-day activity threshold. Enough to page.
    for _ in range(74):
        pipe = "purchase" if RNG.random() < 0.6 else "refi"
        add(stage=RNG.choice(OPEN_PURCHASE if pipe == "purchase" else OPEN_REFI),
            pipeline=pipe, owner=RNG.choice(OWNER_IDS), amount=loan(),
            activity_days=RNG.uniform(16, 140), close_days=RNG.randrange(5, 60),
            created_days=RNG.uniform(40, 220))

    # Never touched since the file was opened - stale by creation date.
    for _ in range(9):
        add(stage="p_lead", pipeline="purchase", owner=RNG.choice(OWNER_IDS),
            amount=loan(), activity_days=None, close_days=RNG.randrange(10, 50),
            created_days=RNG.uniform(22, 70))

    # Past its close date: a rate lock or appraisal window that has expired.
    for _ in range(21):
        pipe = "purchase" if RNG.random() < 0.7 else "refi"
        add(stage=RNG.choice(OPEN_PURCHASE if pipe == "purchase" else OPEN_REFI),
            pipeline=pipe, owner=RNG.choice(OWNER_IDS), amount=loan(),
            activity_days=RNG.uniform(0.5, 30), close_days=-RNG.randrange(1, 45),
            created_days=RNG.uniform(60, 240))

    # Closing today: must NOT read as overdue, in any timezone.
    add(stage="p_cleartoclose", pipeline="purchase", owner="51", amount=905_000,
        activity_days=0.3, close_days=0, created_days=48)

    # Big and quiet: prestige and investment files over a week cold.
    for _ in range(11):
        add(stage=RNG.choice(OPEN_PURCHASE), pipeline="purchase",
            owner=RNG.choice(OWNER_IDS),
            amount=round(RNG.uniform(BIG_LOAN + 50_000, 3_200_000), -3),
            activity_days=RNG.uniform(8, 40), close_days=RNG.randrange(8, 70),
            created_days=RNG.uniform(30, 160))

    # No owner: web leads that never got assigned to an officer.
    for _ in range(7):
        add(stage="p_lead", pipeline="purchase", owner=None, amount=loan(),
            activity_days=RNG.uniform(1, 25), close_days=RNG.randrange(14, 60),
            created_days=RNG.uniform(3, 30))

    # No amount recorded yet: flagged on staleness, worth nothing in totals.
    for _ in range(5):
        add(stage="r_enquiry", pipeline="refi", owner=RNG.choice(OWNER_IDS),
            amount=None, activity_days=RNG.uniform(18, 60),
            close_days=None, created_days=RNG.uniform(25, 90))

    # Closed files, so the pipeline chart has funded and dead stages and the
    # signals have something they must ignore.
    for _ in range(64):
        pipe = "purchase" if RNG.random() < 0.65 else "refi"
        funded = RNG.random() < 0.72
        stage = (("p_funded" if funded else "p_dead") if pipe == "purchase"
                 else ("r_funded" if funded else "r_dead"))
        add(stage=stage, pipeline=pipe, owner=RNG.choice(OWNER_IDS),
            amount=loan(), activity_days=RNG.uniform(1, 90),
            close_days=-RNG.randrange(1, 120), created_days=RNG.uniform(40, 280))

    # Weekly intake: four solid prior weeks, then a thin one - a quiet pipeline.
    for week, count in ((1, 17), (2, 21), (3, 15), (4, 19)):
        for _ in range(count):
            add(stage=RNG.choice(OPEN_PURCHASE), pipeline="purchase",
                owner=RNG.choice(OWNER_IDS), amount=loan(),
                activity_days=RNG.uniform(0.2, 6),
                close_days=RNG.randrange(20, 90),
                created_days=7 * week + RNG.uniform(0.2, 6.5))
    for _ in range(3):
        add(stage="p_lead", pipeline="purchase", owner=RNG.choice(OWNER_IDS),
            amount=loan(), activity_days=RNG.uniform(0.1, 2),
            close_days=RNG.randrange(30, 90), created_days=RNG.uniform(0.2, 4))

    # An archived loan file: nothing may flag it.
    add(stage="p_underwrite", pipeline="purchase", owner="54", amount=1_140_000,
        activity_days=120, close_days=-60, created_days=200, archived=1)
    return rows


def build_engagements(deals, contacts):
    """Calls, emails and the rest, hung off the deals and contacts they touched."""
    rows, assoc, n = [], [], 0
    kinds = ["calls", "emails", "meetings", "notes", "tasks"]
    weights = [26, 38, 9, 18, 9]
    for d in deals:
        if d["archived"]:
            continue
        for _ in range(RNG.randrange(0, 7)):
            when = d["hs_last_activity_date"] or d["createdate"]
            when -= int(RNG.uniform(0, 40) * DAY)
            eid = f"e{40000 + n}"
            kind = RNG.choices(kinds, weights)[0]
            rows.append({"id": eid, "type": kind, "timestamp": when,
                         "hs_lastmodifieddate": when, "archived": 0,
                         "raw": raw(hs_timestamp=when)})
            assoc.append(("engagements", eid, "deals", d["id"]))
            n += 1
    for c in RNG.sample(contacts, k=len(contacts) // 2):
        for _ in range(RNG.randrange(1, 4)):
            when = c["hs_last_activity_date"] or ts(RNG.uniform(5, 100))
            when -= int(RNG.uniform(0, 30) * DAY)
            eid = f"e{40000 + n}"
            kind = RNG.choices(kinds, weights)[0]
            rows.append({"id": eid, "type": kind, "timestamp": when,
                         "hs_lastmodifieddate": when, "archived": 0,
                         "raw": raw(hs_timestamp=when)})
            assoc.append(("engagements", eid, "contacts", c["id"]))
            n += 1
    return rows, assoc


def build_associations(deals, contacts, companies):
    """Every live loan file gets a borrower and, usually, a referral partner."""
    rows = []
    live_co = [c for c in companies if not c["archived"]]
    for i, d in enumerate(deals):
        rows.append(("deals", d["id"], "contacts", contacts[i % len(contacts)]["id"]))
        if RNG.random() < 0.85:
            # The first six partners are the quiet ones; keep open loans on them
            # so `untouched_account` has something to find.
            co = (live_co[i % 6] if i % 7 == 0 else RNG.choice(live_co))
            rows.append(("deals", d["id"], "companies", co["id"]))
            rows.append(("contacts", contacts[i % len(contacts)]["id"],
                         "companies", co["id"]))
    return rows


def write(home: Path) -> Path:
    home.mkdir(parents=True, exist_ok=True)
    defaults = Path(__file__).resolve().parent.parent / "crm" / "defaults"
    cfg = home / "config.toml"
    if not cfg.exists():
        cfg.write_text((defaults / "config.toml.example").read_text()
                       .replace("pat-na1-REPLACE-ME", "pat-ap1-demo-no-live-portal")
                       .replace('timezone = "local"', 'timezone = "Australia/Sydney"'))
    # Thresholds the brokerage would actually use: the shipped big_deal_value of
    # 50,000 would call every mortgage in the book a big deal.
    (home / "signals.toml").write_text(f"""[thresholds]
stalled_days = 14
big_deal_quiet_days = 7
big_deal_value = {BIG_LOAN}
untouched_account_days = 30
quiet_pipeline_drop_pct = 40
""")
    (home / "data").mkdir(exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        Path(str(home / "data" / "crm.db") + suffix).unlink(missing_ok=True)
    return home / "data" / "crm.db"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--home", default="demo", type=Path)
    ap.add_argument("--contacts", type=int, default=240)
    args = ap.parse_args()

    path = write(args.home)
    conn = db.connect(path)
    db.migrate(conn)

    companies = build_companies()
    contacts = build_contacts(args.contacts)
    deals = build_deals()
    engagements, eng_assoc = build_engagements(deals, contacts)
    assoc = build_associations(deals, contacts, companies) + eng_assoc

    db.upsert(conn, "owners", [
        {"id": i, "email": f"{e}@harbourvale.example", "first_name": f,
         "last_name": l, "archived": 0,
         "raw": json.dumps({"id": i, "email": f"{e}@harbourvale.example",
                            "firstName": f, "lastName": l})}
        for i, e, f, l in OWNERS])
    db.upsert(conn, "pipelines", [
        {"id": i, "label": l, "archived": 0,
         "raw": json.dumps({"id": i, "label": l})} for i, l in PIPELINES])
    db.upsert(conn, "stages", [
        {"id": i, "pipeline_id": p, "label": l, "display_order": o,
         "is_closed": c, "probability": pr, "archived": 0,
         "raw": json.dumps({"id": i, "label": l})}
        for i, p, l, o, c, pr in STAGES])
    db.upsert(conn, "companies", companies)
    db.upsert(conn, "contacts", contacts)
    db.upsert(conn, "deals", deals)
    db.upsert(conn, "engagements", engagements)

    with conn:
        conn.executemany(
            "INSERT OR IGNORE INTO associations (from_type, from_id, to_type, to_id)"
            " VALUES (?, ?, ?, ?)", assoc)
        # A portal id and UI domain, so the "Open in HubSpot" links render.
        # An Australian portal is served from the ap1 region, not app.hubspot.com.
        conn.executemany("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                         [("portal_id", "48213907"),
                          ("ui_domain", "app-ap1.hubspot.com")])
        # Present the account as fully backfilled and freshly synced, so the
        # dashboard shows its normal state rather than its empty one.
        conn.executemany(
            "INSERT OR REPLACE INTO sync_state"
            " (object_type, last_modified_ms, cursor, phase, last_run_at, status)"
            " VALUES (?, ?, NULL, 'complete', ?, 'ok')",
            [(t, NOW - 4 * 60_000, NOW - 4 * 60_000)
             for t in ("deals", "companies", "contacts", "owners",
                       "calls", "emails", "meetings", "notes", "tasks")])
        conn.execute(
            "INSERT OR REPLACE INTO sync_state"
            " (object_type, last_modified_ms, cursor, phase, last_run_at, status)"
            " VALUES ('weekly', NULL, NULL, 'complete', ?, 'ok')",
            (NOW - 2 * DAY,))
    # Evaluate the signals here too, so the demo needs no network at all:
    # `watchdog sync` would do this, but only after talking to HubSpot.
    hits = signals.evaluate_all(
        conn, config.load_thresholds(args.home / "signals.toml"), NOW)
    conn.close()

    print(f"Seeded {path}")
    print(f"  {len(deals):>5} deals   {len(contacts):>5} contacts  "
          f"{len(companies):>3} companies  {len(engagements):>5} engagements")
    print(f"  {hits} records flagged by the signals")
    print(f"\nNext:  ./.venv/bin/python -m crm --home {args.home} serve")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
