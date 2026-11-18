import re
from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from crm.signals import (
    SIGNALS, OwnerRollup, briefing, evaluate_all, explain, total_at_risk,
)
from tests.conftest import (
    DAY, DEFAULT_THRESHOLDS, NOW, UTC, add_company, add_deal, add_owner,
    add_stage, days_ago, flagged, flagged_in_briefing, summary,
)


@pytest.fixture(autouse=True)
def stages(conn):
    add_stage(conn, "s_open", is_closed=0)
    add_stage(conn, "s_won", is_closed=1)


def with_thresholds(**overrides):
    t = deepcopy(DEFAULT_THRESHOLDS)
    t["thresholds"].update(overrides)
    return t


def ms(*args, tz=UTC) -> int:
    return int(datetime(*args, tzinfo=tz).timestamp() * 1000)


def evaluate(conn, now=NOW, **kwargs):
    kwargs.setdefault("tz", UTC)
    return evaluate_all(conn, DEFAULT_THRESHOLDS, now, **kwargs)


# --- the declarations themselves -----------------------------------------------

def test_seven_signals_declared_and_only_stage_regression_disabled():
    assert [s.id for s in SIGNALS] == [
        "stalled_deal", "past_close_date", "big_and_quiet", "untouched_account",
        "unowned_record", "stage_regression", "quiet_pipeline"]
    assert [s.id for s in SIGNALS if not s.enabled] == ["stage_regression"]


def test_every_signal_sql_filters_archived():
    for s in SIGNALS:
        assert "archived = 0" in s.sql


def test_no_signal_sql_touches_engagements():
    for s in SIGNALS:
        assert "engagements" not in s.sql.lower()


def test_no_signal_sql_uses_bare_amount_column():
    for s in SIGNALS:
        assert re.search(r"amount(?!_in_home_currency)", s.sql) is None


def test_every_threshold_a_signal_names_exists_in_signals_toml_and_sql():
    for s in SIGNALS:
        for name in s.thresholds:
            assert name in DEFAULT_THRESHOLDS["thresholds"]
            assert f":{name}" in s.sql


def test_every_deal_signal_defines_open_by_the_stages_table():
    for s in SIGNALS:
        if s.enabled and s.id != "quiet_pipeline":
            assert "is_closed = 0" in s.sql, s.id


def test_explanation_states_the_rule_with_the_live_threshold():
    stalled = next(s for s in SIGNALS if s.id == "stalled_deal")
    text = explain(stalled, with_thresholds(stalled_days=21))
    assert "21 days" in text and "{" not in text


def test_stage_regression_does_not_run(conn):
    add_deal(conn, "d1")
    assert flagged(conn, "stage_regression") == set()


# --- stalled_deal ------------------------------------------------------------------

def test_stalled_deal_flagged_past_threshold_not_before(conn):
    add_deal(conn, "d_stale", activity=days_ago(20))
    add_deal(conn, "d_fresh", activity=days_ago(3))
    assert flagged(conn, "stalled_deal") == {"d_stale"}


def test_stalled_threshold_comes_from_the_thresholds_argument(conn):
    add_deal(conn, "d_mid", activity=days_ago(10))
    assert flagged(conn, "stalled_deal") == set()
    assert flagged(conn, "stalled_deal", with_thresholds(stalled_days=7)) == {"d_mid"}


def test_deal_with_no_activity_falls_back_to_its_creation_date(conn):
    add_deal(conn, "d_old", activity=None, createdate=days_ago(60))
    add_deal(conn, "d_new", activity=None, createdate=days_ago(2))
    assert flagged(conn, "stalled_deal") == {"d_old"}


def test_closed_deals_never_flagged(conn):
    add_deal(conn, "d_won", stage="s_won", activity=days_ago(90),
             closedate=days_ago(30), owner=None)
    for s in SIGNALS:
        assert flagged(conn, s.id) == set(), s.id


def test_open_means_the_stage_row_not_its_label(conn):
    add_stage(conn, "s_open", is_closed=0, label="Closed Won")
    add_stage(conn, "s_won", is_closed=1, label="Qualified")
    add_deal(conn, "d1", activity=days_ago(20))
    add_deal(conn, "d2", stage="s_won", activity=days_ago(20))
    assert flagged(conn, "stalled_deal") == {"d1"}


def test_archived_records_never_flagged(conn):
    add_deal(conn, "d_arch", amount=90000, activity=days_ago(30),
             closedate=days_ago(5), owner=None, archived=1)
    add_company(conn, "c_arch", owner=None, activity=days_ago(90), archived=1,
                open_deals=["d_arch"])
    # A live company whose only open deal is archived has no open deal.
    add_company(conn, "c_only_archived_deal", activity=days_ago(90),
                open_deals=["d_arch"])
    for s in SIGNALS:
        assert flagged(conn, s.id) == set(), s.id


# --- past_close_date -----------------------------------------------------------------

def test_past_close_date_uses_local_timezone_boundary(conn):
    # 2026-11-19 01:30 in Kolkata (UTC+5:30); local "today" began 18:30 UTC on the 18th.
    kolkata = ZoneInfo("Asia/Kolkata")
    now = ms(2026, 11, 19, 1, 30, tz=kolkata)
    add_deal(conn, "d_yesterday_local", closedate=ms(2026, 11, 18, 10, 0))  # 15:30 IST, 18th
    add_deal(conn, "d_today_local", closedate=ms(2026, 11, 18, 19, 0))      # 00:30 IST, 19th
    add_deal(conn, "d_future", closedate=ms(2026, 11, 25))
    evaluate(conn, now, tz=kolkata)
    got = {r[0] for r in conn.execute(
        "SELECT record_id FROM signal_results WHERE signal_id = 'past_close_date'")}
    assert got == {"d_yesterday_local"}


def test_null_closedate_deal_is_not_flagged_as_past_close(conn):
    add_deal(conn, "d_none", closedate=None)
    add_deal(conn, "d_past", closedate=days_ago(5))
    assert flagged(conn, "past_close_date") == {"d_past"}


# --- big_and_quiet -------------------------------------------------------------------

def test_big_and_quiet_respects_value_threshold(conn):
    add_deal(conn, "big_quiet", amount=60000, activity=days_ago(10))
    add_deal(conn, "small_quiet", amount=40000, activity=days_ago(10))
    add_deal(conn, "big_active", amount=60000, activity=days_ago(3))
    add_deal(conn, "no_amount_quiet", amount=None, activity=days_ago(10))
    assert flagged(conn, "big_and_quiet") == {"big_quiet"}


# --- untouched_account ---------------------------------------------------------------

def test_untouched_account_requires_an_open_deal(conn):
    add_deal(conn, "d_open")
    add_deal(conn, "d_closed", stage="s_won")
    add_company(conn, "c_with_open", activity=days_ago(45), open_deals=["d_open"])
    add_company(conn, "c_only_closed", activity=days_ago(45), open_deals=["d_closed"])
    add_company(conn, "c_no_deal", activity=days_ago(45))
    add_company(conn, "c_recent", activity=days_ago(5), open_deals=["d_open"])
    assert flagged(conn, "untouched_account") == {"c_with_open"}


def test_company_with_no_recorded_activity_is_untouched(conn):
    add_deal(conn, "d_open")
    add_company(conn, "c1", activity=None, open_deals=["d_open"])
    assert flagged(conn, "untouched_account") == {"c1"}


# --- unowned_record --------------------------------------------------------------------

def test_unowned_record_flags_null_owner(conn):
    add_deal(conn, "d_unowned", owner=None)
    add_deal(conn, "d_blank", owner="")
    add_deal(conn, "d_owned", owner="o1")
    add_deal(conn, "d_closed_unowned", stage="s_won", owner=None)
    add_company(conn, "c_unowned", owner=None)
    add_company(conn, "c_owned", owner="o1")
    assert flagged(conn, "unowned_record") == {"d_unowned", "d_blank", "c_unowned"}


# --- quiet_pipeline --------------------------------------------------------------------
# NOW is Wednesday 12:00 UTC, so the week so far is 2.5 days old.

def seed_created(conn, prefix, when_ms, n):
    for i in range(n):
        add_deal(conn, f"{prefix}{i}", createdate=when_ms)


def tuesday_of(weeks_back):
    return ms(2026, 11, 17, 12, 0) - weeks_back * 7 * DAY


def test_quiet_pipeline_flags_sharp_drop_against_4_week_average(conn):
    for k in range(1, 5):
        seed_created(conn, f"w{k}_", tuesday_of(k), 10)
    seed_created(conn, "now_", tuesday_of(0), 4)
    assert flagged(conn, "quiet_pipeline") == {"2026-11-16"}


def test_quiet_pipeline_silent_on_normal_week(conn):
    for k in range(1, 5):
        seed_created(conn, f"w{k}_", tuesday_of(k), 10)
    seed_created(conn, "now_", tuesday_of(0), 9)
    assert flagged(conn, "quiet_pipeline") == set()


def test_quiet_pipeline_compares_like_for_like_so_monday_morning_is_silent(conn):
    # Prior weeks' deals all landed on Fridays, after the elapsed window of a
    # Monday-morning run; comparing against full weeks would flag a drop that
    # has not happened.
    monday_morning = ms(2026, 11, 16, 9, 0)
    for k in range(1, 5):
        seed_created(conn, f"w{k}_", ms(2026, 11, 13, 12, 0) - k * 7 * DAY, 10)
    assert flagged(conn, "quiet_pipeline", now_ms=monday_morning) == set()


def test_quiet_pipeline_ignores_archived_deals(conn):
    for k in range(1, 5):
        for i in range(10):
            add_deal(conn, f"w{k}_{i}", createdate=tuesday_of(k), archived=1)
    assert flagged(conn, "quiet_pipeline") == set()


def test_week_start_day_is_validated(conn):
    with pytest.raises(ValueError, match="week_starts_on"):
        evaluate(conn, week_starts_on="funday")


def test_week_start_day_changes_the_week_being_measured(conn):
    # Ten deals on each of the last four Tuesdays, none yet this week. With
    # Monday weeks that is a collapse against the Monday-to-now slice; with
    # Wednesday weeks the slice is Wednesday 00:00-12:00, which held nothing.
    for k in range(1, 5):
        seed_created(conn, f"w{k}_", tuesday_of(k), 10)
    assert flagged(conn, "quiet_pipeline") == {"2026-11-16"}
    evaluate(conn, week_starts_on="Wednesday")
    assert conn.execute("SELECT count(*) FROM signal_results WHERE signal_id"
                        " = 'quiet_pipeline'").fetchone()[0] == 0


# --- nulls -------------------------------------------------------------------------------

def test_null_amount_deal_is_flagged_but_contributes_zero_to_total(conn):
    add_deal(conn, "d_no_amount", amount=None, activity=days_ago(20))
    add_deal(conn, "d_amount", amount=50000.0, activity=days_ago(20))
    s = summary(conn, "stalled_deal")
    assert "d_no_amount" in s.record_ids
    assert s.count == 2
    assert s.total_value == 50000.0


def test_null_owner_rolls_up_under_unassigned(conn):
    add_deal(conn, "d1", owner=None, activity=days_ago(20))
    s = summary(conn, "stalled_deal")
    assert any(o.owner_id is None and o.label == "Unassigned" for o in s.by_owner)


def test_owner_label_prefers_name_then_email_then_id(conn):
    add_owner(conn, "o1", "Rhea", "Mehta")
    add_owner(conn, "o2", None, None, email="sam@example.com")
    for i, owner in enumerate(["o1", "o2", "o_gone"]):
        add_deal(conn, f"d{i}", owner=owner, activity=days_ago(20))
    labels = {o.owner_id: o.label for o in summary(conn, "stalled_deal").by_owner}
    assert labels == {"o1": "Rhea Mehta", "o2": "sam@example.com", "o_gone": "o_gone"}


# --- briefing ----------------------------------------------------------------------------

def test_by_owner_sorted_by_value_descending(conn):
    add_deal(conn, "a", owner="o1", amount=10000, activity=days_ago(20))
    add_deal(conn, "b", owner="o2", amount=90000, activity=days_ago(20))
    add_deal(conn, "c", owner=None, amount=30000, activity=days_ago(20))
    by_owner = summary(conn, "stalled_deal").by_owner
    assert all(isinstance(o, OwnerRollup) for o in by_owner)
    assert [o.owner_id for o in by_owner] == ["o2", None, "o1"]
    assert [o.total_value for o in by_owner] == [90000.0, 30000.0, 10000.0]
    assert [o.count for o in by_owner] == [1, 1, 1]


def test_briefing_sorted_by_severity_then_value(conn):
    add_deal(conn, "stalled_small", amount=1000, activity=days_ago(20))
    add_deal(conn, "unowned_big", amount=900000, owner=None)          # medium
    add_deal(conn, "past_big", amount=500000, closedate=days_ago(3))  # high
    evaluate(conn)
    out = briefing(conn, NOW)
    rank = {"high": 0, "medium": 1, "low": 2}
    keys = [(rank[s.severity], -s.total_value) for s in out]
    assert keys == sorted(keys)
    ids = [s.signal_id for s in out]
    assert ids[:2] == ["past_close_date", "stalled_deal"]
    assert "unowned_record" in ids


def test_briefing_omits_signals_with_nothing_flagged(conn):
    add_deal(conn, "d1", activity=days_ago(20))
    evaluate(conn)
    assert [s.signal_id for s in briefing(conn, NOW)] == ["stalled_deal"]


def test_briefing_reads_only_signal_results(conn):
    add_deal(conn, "d1", activity=days_ago(20))
    evaluate(conn)
    with conn:
        conn.execute("DELETE FROM deals")
    assert [s.signal_id for s in briefing(conn, NOW)] == ["stalled_deal"]


def snooze(conn, signal_id, record_id, until):
    with conn:
        conn.execute("INSERT INTO snoozes (signal_id, record_id, until, created_at)"
                     " VALUES (?, ?, ?, ?)", (signal_id, record_id, until, NOW))


def test_unexpired_snooze_hides_a_record_and_expired_one_does_not(conn):
    add_deal(conn, "d_snoozed", activity=days_ago(20))
    add_deal(conn, "d_expired", activity=days_ago(20))
    add_deal(conn, "d_shown", activity=days_ago(20))
    evaluate(conn)
    snooze(conn, "stalled_deal", "d_snoozed", NOW + DAY)
    snooze(conn, "stalled_deal", "d_expired", NOW - 1)
    assert flagged_in_briefing(conn, NOW, "stalled_deal") == {"d_expired", "d_shown"}
    assert summary(conn, "stalled_deal").count == 2


def test_snooze_is_per_signal(conn):
    add_deal(conn, "d1", amount=60000, activity=days_ago(20))
    evaluate(conn)
    snooze(conn, "stalled_deal", "d1", NOW + DAY)
    assert flagged_in_briefing(conn, NOW, "stalled_deal") == set()
    assert flagged_in_briefing(conn, NOW, "big_and_quiet") == {"d1"}


# --- total_at_risk -----------------------------------------------------------------------

def test_total_at_risk_counts_a_deal_once_across_overlapping_signals(conn):
    add_deal(conn, "d_big", amount=200000, activity=days_ago(20))
    assert flagged(conn, "stalled_deal") == {"d_big"}
    assert flagged(conn, "big_and_quiet") == {"d_big"}
    assert total_at_risk(conn) == 200000.0


def test_total_at_risk_sums_deals_only_and_treats_null_as_zero(conn):
    add_deal(conn, "d1", amount=1000, activity=days_ago(20))
    add_deal(conn, "d2", amount=None, activity=days_ago(20))
    add_company(conn, "c1", activity=days_ago(90), open_deals=["d1"])
    assert flagged(conn, "untouched_account") == {"c1"}
    assert total_at_risk(conn) == 1000.0


def test_total_at_risk_excludes_snoozed_when_given_now(conn):
    add_deal(conn, "d1", amount=1000, activity=days_ago(20))
    add_deal(conn, "d2", amount=2000, activity=days_ago(20))
    evaluate(conn)
    snooze(conn, "stalled_deal", "d2", NOW + DAY)
    assert total_at_risk(conn) == 3000.0
    assert total_at_risk(conn, NOW) == 1000.0


def test_total_at_risk_is_zero_with_nothing_flagged(conn):
    assert total_at_risk(conn) == 0.0


# --- evaluate_all --------------------------------------------------------------------------

def test_evaluate_all_replaces_previous_results(conn):
    add_deal(conn, "d1", activity=days_ago(20))
    evaluate(conn)
    n = evaluate(conn)
    assert n > 0
    assert conn.execute("select count(*) from signal_results").fetchone()[0] == n


def test_evaluate_all_drops_records_that_stopped_matching(conn):
    add_deal(conn, "d1", activity=days_ago(20))
    evaluate(conn)
    add_deal(conn, "d1", activity=days_ago(1))
    evaluate(conn)
    assert conn.execute("select count(*) from signal_results").fetchone()[0] == 0


def test_evaluate_all_stamps_computed_at_with_the_given_now(conn):
    add_deal(conn, "d1", activity=days_ago(20))
    evaluate(conn)
    assert {r[0] for r in conn.execute(
        "select computed_at from signal_results")} == {NOW}


def test_failed_evaluation_leaves_previous_results_intact(conn):
    add_deal(conn, "d1", activity=days_ago(20))
    evaluate(conn)
    before = conn.execute("select count(*) from signal_results").fetchone()[0]
    broken = deepcopy(DEFAULT_THRESHOLDS)
    del broken["thresholds"]["untouched_account_days"]   # a later signal needs it
    with pytest.raises(KeyError):
        evaluate_all(conn, broken, NOW, tz=UTC)
    assert conn.execute("select count(*) from signal_results").fetchone()[0] == before


# --- sync wiring ---------------------------------------------------------------------------

def test_incremental_sync_evaluates_signals(conn, fake_client):
    from crm.sync import sync_incremental
    add_deal(conn, "d_stale", activity=days_ago(400))
    sync_incremental(conn, fake_client, ["deals"])
    assert conn.execute(
        "select count(*) from signal_results where record_id = 'd_stale'"
    ).fetchone()[0] > 0


def test_backfill_evaluates_signals(conn, fake_client):
    from crm.sync import backfill
    add_deal(conn, "d_stale", activity=days_ago(400))
    backfill(conn, fake_client)
    assert conn.execute(
        "select count(*) from signal_results where record_id = 'd_stale'"
    ).fetchone()[0] > 0
