"""
scripts/portfolio_check.py: run merchants, lending and repayments end to end and print
portfolio quality against the Validation Targets.

Not a test: a live run takes minutes. Used to calibrate the arrears parameters, and a
starting point for the Phase 8 reconciliation queries.

    PYTHONPATH=. python scripts/portfolio_check.py --merchants 120 --days 540 --boost 5

--boost multiplies the monthly application propensity so a small population originates
enough loans to measure. It does not change repayment behaviour.
"""
from __future__ import annotations

import argparse
import time
from collections import Counter, defaultdict
from datetime import date, timedelta

import numpy as np

from simulator import lending
from simulator.generator import simulate_day
from simulator.lending import simulate_lending_day
from simulator.risk import evaluate_risk
from simulator.state import Merchant

ARCHETYPES = ["wholesale", "tutor", "salon", "e_commerce", "restaurant", "salon_low_cost"]
PROVINCES = ["gauteng", "western_cape", "kwazulu_natal", "eastern_cape"]


def run(merchants: int, days: int, boost: float, seed: int) -> dict:
    np.random.seed(seed)
    for state in ("growing", "stable"):
        lending._PROPENSITY[state] *= boost
    start = date(2025, 1, 1)
    pool = [
        Merchant(
            merchant_id=f"m-{i}", archetype=ARCHETYPES[i % len(ARCHETYPES)],
            quality_percentile=float(np.random.beta(1.5, 4.0)), onboarded_at=start,
            province=PROVINCES[i % 4], is_metro=bool(i % 2),
        )
        for i in range(merchants)
    ]
    rows: list[dict] = []
    snapshots: list[tuple] = []
    states = Counter()
    risk_events: Counter = Counter()
    book_by_state: Counter = Counter()
    par30_by_state: Counter = Counter()
    loans_by_state: Counter = Counter()
    for offset in range(days):
        day = start + timedelta(days=offset)
        for m in pool:
            events = simulate_day(m, day)
            for r in evaluate_risk(m, events, day):
                risk_events[r["rule_type"]] += 1
            for r in simulate_lending_day(m, day)["loan_repayments"]:
                r["_day"] = day
                rows.append(r)
            m.evaluate_state(day)
            m.decay_risk(day)
        if (day + timedelta(days=1)).day == 1:  # month end
            loans = [m.active_loan for m in pool if m.active_loan]
            total = sum(l.outstanding_balance_minor for l in loans)
            if total:
                def par(d):
                    return sum(l.outstanding_balance_minor for l in loans if l.days_past_due > d) / total
                snapshots.append((day, par(30), par(60), par(90), total, len(loans)))
            states.update(m.merchant_state for m in pool)
            if day >= start + timedelta(days=days - 180):
                for m in pool:
                    if m.active_loan:
                        book_by_state[m.merchant_state] += m.active_loan.outstanding_balance_minor
                        loans_by_state[m.merchant_state] += 1
                        if m.active_loan.days_past_due > 30:
                            par30_by_state[m.merchant_state] += m.active_loan.outstanding_balance_minor
    return {
        "rows": rows, "snapshots": snapshots, "pool": pool, "states": states,
        "risk_events": risk_events, "book_by_state": book_by_state, "par30_by_state": par30_by_state, "loans_by_state": loans_by_state,
    }


def report(result: dict, days: int) -> None:
    rows, snaps, pool = result["rows"], result["snapshots"], result["pool"]
    mature = [s for s in snaps if s[0] >= date(2025, 1, 1) + timedelta(days=days - 180)]
    print(f"loans originated: {sum(m.loan_count for m in pool)}  repayment rows: {len(rows)}")
    if mature:
        print("PAR30 %.1f%%  PAR60 %.1f%%  PAR90 %.1f%%  (mean of last %d month ends; band 5-10 / 3-7 / 2-5)" % (
            100 * np.mean([s[1] for s in mature]), 100 * np.mean([s[2] for s in mature]),
            100 * np.mean([s[3] for s in mature]), len(mature)))
        avg_book = np.mean([s[4] for s in mature])
        print("mean loans on book: %.0f" % np.mean([s[5] for s in mature]))
    else:
        avg_book = 0
    first = [r for r in rows if r["retry_attempt"] == 0 and r["days_past_due"] == 0 and not r["is_cured"]]
    print("first strike %.1f%%  (band 72-82)" % (100 * np.mean([r["collection_status"] == "success" for r in first])))
    wo = [r for r in rows if r["write_off_flag"]]
    window_start = date(2025, 1, 1) + timedelta(days=days - 180)
    wo_recent = [r for r in wo if r["_day"] >= window_start]
    if avg_book:
        annual = sum(r["outstanding_before"] for r in wo_recent) * 2 / avg_book
        print("write-offs: %d loans (%d in the last 6 months); annualised write-off %.1f%% of average book  (band 3-7)" % (
            len(wo), len(wo_recent), 100 * annual))
    wo_dormant = sum(1 for m in pool if m.is_written_off and m.dormant_since is not None)
    print("written-off merchants that had gone dormant: %d of %d" % (wo_dormant, sum(m.is_written_off for m in pool)))
    print("merchants with a written-off loan: %d  with a repaid loan: %d" % (
        sum(m.is_written_off for m in pool), sum(m.prior_loan_repaid for m in pool)))
    unpaid_first = set()
    paid_first = set()
    for r in rows:
        if r["installment_number"] == 1:
            (paid_first if r["collection_status"] == "success" else unpaid_first).add(r["loan_id"])
    if paid_first | unpaid_first:
        print("first payment default (instalment 1 never collected): %.1f%%  (band 3-6)" % (
            100 * len(unpaid_first - paid_first) / len(paid_first | unpaid_first)))
    book, par30, loans = result["book_by_state"], result["par30_by_state"], result["loans_by_state"]
    total_book = sum(book.values()) or 1
    print("book by merchant state (last 6 month ends): " + ", ".join(
        f"{k} {100 * v / total_book:.0f}% (PAR30 within {100 * par30[k] / v:.0f}%)" for k, v in sorted(book.items())))
    buckets = Counter(
        "0-30" if m.risk_score <= 30 else "31-55" if m.risk_score <= 55 else "56-75" if m.risk_score <= 75 else "76-100"
        for m in pool
    )
    print("risk score buckets (end of run): " + ", ".join(
        f"{k} {100 * buckets[k] / len(pool):.0f}%" for k in ("0-30", "31-55", "56-75", "76-100")
    ) + "  (target 55-70 / 15-25 / 8-15 / 3-8)")
    years = days / 365
    print("risk events per merchant-year: " + ", ".join(
        f"{k} {v / len(pool) / years:.2f}" for k, v in sorted(result["risk_events"].items())
    ) + f"  (total {sum(result['risk_events'].values()) / len(pool) / years:.2f})")
    share = result["states"]
    total = sum(share.values())
    print("merchant states (month-end average): " + ", ".join(f"{k} {100*v/total:.0f}%" for k, v in sorted(share.items())))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--merchants", type=int, default=120)
    parser.add_argument("--days", type=int, default=540)
    parser.add_argument("--boost", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=3)
    args = parser.parse_args()
    t0 = time.time()
    report(run(args.merchants, args.days, args.boost, args.seed), args.days)
    print("(%.0fs)" % (time.time() - t0))
