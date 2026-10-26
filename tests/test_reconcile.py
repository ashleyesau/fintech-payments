"""
test_reconcile.py: the Phase 8 reconciliation and failure-mode tests.

Everything here runs validate.sql against one 13-month, 100-merchant backfill (the
`validated` fixture in conftest.py), so the module is slow. Application propensity is raised
tenfold in that run so the lending checks have loans to look at.

Three kinds of test:
  * validate.sql runs, and its loan book agrees with the generator's own loan state;
  * the seven failure modes from the Simulation Validation Targets hold on the run;
  * each failure-mode query fires when its fault is planted (negative controls), so a pass
    above means the query looked and found nothing, not that it cannot see.

The band checks (PAR30 against 5 to 10% and the rest) are measured by `python -m
simulator.validation` on larger runs and recorded in DECISION_LOG; they are not asserted
here because a 100-merchant run is too noisy to hold a band.
"""
from datetime import date

import pytest

from simulator import validation

pytestmark = pytest.mark.slow


def check(validated, query: str, name: str) -> validation.Check:
    conn, _ = validated
    rows = {c.name: c for c in validation.run_check(conn, query)}
    return rows[name]


# ---------------------------------------------------------------------------
# validate.sql runs, and agrees with the generator
# ---------------------------------------------------------------------------

def test_every_query_in_validate_sql_runs(validated):
    conn, _ = validated
    for name in validation.load_queries():
        if name == "setup":
            continue
        if name.startswith("series_"):
            columns, rows = validation.run_series(conn, name)
            assert columns and rows, name
        else:
            assert validation.run_check(conn, name), name


def test_the_sql_loan_book_equals_the_generators_loans(validated):
    """At the last month end the instalment-ageing book holds the same loans, with the same
    outstanding balances, as the generator's live Loan objects."""
    conn, merchants = validated
    last = conn.execute("SELECT data_end FROM t_bounds").fetchone()[0]
    assert last == date(2026, 1, 31), "the fixture run must end on a month end"
    sql_book = dict(conn.execute("SELECT loan_id, balance FROM t_book WHERE d = %s", (last,)).fetchall())
    live = {
        m.active_loan.loan_id: m.active_loan.outstanding_balance_minor
        for m in merchants if m.active_loan is not None and not m.active_loan.is_written_off
    }
    assert live, "the run must leave loans on the book"
    assert sql_book == live


def test_row_derived_dpd_flags_the_loans_the_generator_has_past_30_days(validated):
    """The DPD recovered from loan_repayments rows and the generator's own days_past_due
    agree on which live loans are more than 30 days late. They differ for loans in arrears
    after their term ended, which the generator steps without writing a row, so a small
    disagreement is allowed."""
    conn, merchants = validated
    last = conn.execute("SELECT data_end FROM t_bounds").fetchone()[0]
    sql_late = {r[0] for r in conn.execute(
        "SELECT loan_id FROM t_book WHERE d = %s AND dpd > 30", (last,))}
    live = [m.active_loan for m in merchants if m.active_loan is not None and not m.active_loan.is_written_off]
    gen_late = {loan.loan_id for loan in live if loan.days_past_due > 30}
    assert gen_late, "the run must have loans past 30 days at the end for this to mean anything"
    disagreement = len(sql_late ^ gen_late) / len(live)
    assert disagreement <= 0.10, (sql_late ^ gen_late, len(live))


def test_vintage_par30_never_falls_as_loans_stay_on_book_longer(validated):
    """The measure is cumulative ("ever late by month k"), so for the same loans the share at
    month 6 cannot be below the share at month 3."""
    conn, _ = validated
    row = conn.execute("""
        WITH obs AS (
            SELECT l.loan_id, l.disbursed_on FROM t_loan l, t_bounds b
            WHERE l.initial_balance IS NOT NULL AND (l.disbursed_on + interval '6 months')::date <= b.data_end),
        f AS (
            SELECT k, avg((EXISTS (SELECT 1 FROM t_rrow r WHERE r.loan_id = o.loan_id
                   AND r.event_on <= (o.disbursed_on + make_interval(months => k))::date AND r.days_past_due > 30))::int) AS par, count(*) AS n
            FROM obs o CROSS JOIN (VALUES (3), (6)) m(k) GROUP BY k)
        SELECT (SELECT par FROM f WHERE k = 3), (SELECT par FROM f WHERE k = 6), (SELECT max(n) FROM f)""").fetchone()
    assert row[2] and row[2] >= 10, "the run must have loans six months on book"
    assert row[0] <= row[1]


# ---------------------------------------------------------------------------
# The seven failure modes
# ---------------------------------------------------------------------------

def test_no_modifier_stacking_spike(validated):
    """The cap bounds the intensity function, so the assertion is on payment counts, plus
    platform GPV. The archetype GPV ratio is reported but not asserted: Black Friday lifts
    tickets after the cap, by design."""
    conn, _ = validated
    checks = {c.name: c for c in validation.run_check(conn, "fm_modifier_stacking")}
    assert checks["max_day_tx_over_platform_month_mean_ratio"].status == "ok", checks
    assert checks["max_day_gpv_over_platform_month_mean_ratio"].status == "ok", checks
    assert checks["max_day_tx_over_archetype_month_mean_ratio"].status in ("ok", "no data"), checks


def test_merchants_do_not_snap_states_together(validated):
    c = check(validated, "fm_state_snapping", "max_weekly_share_of_merchants_changing_state")
    assert c.value is not None and c.status == "ok", c


def test_par30_does_not_explode_in_the_first_three_months_of_lending(validated):
    c = check(validated, "fm_early_par30", "max_par30_first_three_months_of_lending")
    assert c.value is not None and c.status == "ok", c


def test_hourly_distribution_is_not_flat(validated):
    c = check(validated, "hourly_chi2", "hourly_distribution_chi2_vs_uniform")
    assert c.status == "ok", c


def test_january_gpv_is_below_december(validated):
    c = check(validated, "january_gpv_vs_december", "january_gpv_vs_december")
    assert c.value is not None and c.value < 0, c


def test_wholesale_does_not_reach_par90(validated):
    c = check(validated, "fm_wholesale_par90", "wholesale_par90")
    assert c.status in ("ok", "no data"), c


def test_risk_scores_do_not_cluster_above_60(validated):
    c = check(validated, "fm_risk_clustering", "active_merchants_with_risk_score_above_60")
    assert c.value is not None and c.status == "ok", c


# ---------------------------------------------------------------------------
# Negative controls: plant each fault in the temp tables and check the query fires.
# The temp tables are rolled back afterwards, so the other tests see the real data.
# ---------------------------------------------------------------------------

@pytest.fixture
def planted(validated):
    conn, _ = validated
    yield conn
    conn.rollback()


def fires(conn, query: str, name: str) -> validation.Check:
    return {c.name: c for c in validation.run_check(conn, query)}[name]


def test_stacking_query_fires_on_a_day_with_triple_the_payments(planted):
    planted.execute("""
        INSERT INTO t_pay (transaction_id, merchant_id, archetype, channel, status, amount_minor, fee_minor, markup_minor, d, h, dow, otp_dropout)
        SELECT 'planted-' || i || '-' || p.transaction_id, p.merchant_id, p.archetype, p.channel, p.status, p.amount_minor,
               p.fee_minor, p.markup_minor, p.d, p.h, p.dow, p.otp_dropout
        FROM t_pay p, generate_series(1, 40) i
        WHERE p.d = (SELECT max(d) - 40 FROM t_pay)""")
    assert fires(planted, "fm_modifier_stacking", "max_day_tx_over_platform_month_mean_ratio").status == "HIGH"


def test_snapping_query_fires_when_everyone_changes_state_in_one_week(planted):
    planted.execute("""
        UPDATE t_day SET merchant_state = CASE WHEN snapshot_date >= date '2025-09-15' THEN 'stressed' ELSE 'stable' END
        WHERE snapshot_date BETWEEN date '2025-09-08' AND date '2025-09-21'""")
    assert fires(planted, "fm_state_snapping", "max_weekly_share_of_merchants_changing_state").status == "HIGH"


def test_early_par30_query_fires_when_the_first_loans_are_all_late(planted):
    planted.execute("UPDATE t_book SET dpd = 45, oldest_unpaid_days = 45")
    assert fires(planted, "fm_early_par30", "max_par30_first_three_months_of_lending").status == "HIGH"


def test_hourly_query_fires_on_a_flat_distribution(planted):
    planted.execute("UPDATE t_pay SET h = abs(hashtext(transaction_id)) % 24")
    assert fires(planted, "hourly_chi2", "hourly_distribution_chi2_vs_uniform").status == "LOW"


def test_january_query_fires_when_january_beats_december(planted):
    planted.execute("""
        UPDATE t_pay SET amount_minor = amount_minor * 3
        WHERE d >= date '2026-01-01' AND status = 'completed'""")
    assert fires(planted, "january_gpv_vs_december", "january_gpv_vs_december").value > 0


def test_wholesale_par90_query_fires_on_a_deep_wholesale_loan(planted):
    planted.execute("""
        INSERT INTO t_book (loan_id, merchant_id, archetype, d, balance, oldest_unpaid_days, dpd)
        SELECT 'planted', 'planted', 'wholesale', data_end, 100000000, 120, 120 FROM t_bounds""")
    assert fires(planted, "fm_wholesale_par90", "wholesale_par90").status == "HIGH"


def test_vintage_query_fires_when_every_loan_is_late_early(planted):
    planted.execute("UPDATE t_rrow SET days_past_due = 45")
    checks = {c.name: c for c in validation.run_check(planted, "vintage_par30_by_mob")}
    assert checks["vintage_par30_mob_3"].value == 1.0
    assert checks["vintage_par30_mob_3"].status == "HIGH"


def test_risk_clustering_query_fires_when_scores_sit_high(planted):
    planted.execute("UPDATE t_active_last SET risk_score = 80")
    assert fires(planted, "fm_risk_clustering", "active_merchants_with_risk_score_above_60").status == "HIGH"
