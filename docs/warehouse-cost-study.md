# Warehouse cost and performance study: fintech-payments

Measurements taken 9 November 2026 | GCP project payments-512910 (us-east1) | dbt-core 1.12.6, dbt-bigquery 1.9.2 | Status: measurements complete; recommendations implemented and checked 10 November 2026 (section 15)

## 1. Summary

This study measured what the fintech-payments pipeline costs to run on BigQuery, where that cost sits, and what it would take to cut it. All work stayed inside Google Cloud free tiers. The study's own queries billed roughly 21 GB, about 2% of the free 1 TiB monthly allowance (excluding some earlier native-graph builds whose output was not captured).

**Main findings**

1. A full rebuild of the 25-model graph bills 5.115 GB (4.942 GB processed, 1,689.5 slot-seconds, 38.2 s wall clock).
2. Four models account for 94% of those bytes: int\_payments\_\_transaction\_enriched (49.5%), fct\_settlement\_daily (19.3%), int\_payments\_\_merchant\_daily\_gpv (16.4%) and fct\_revenue\_daily (9.0%). Nearly every other model bills BigQuery's 10 MiB per-table minimum.
3. No staging or intermediate model restricts its Bronze read by date, so every run reads the full transactions table. Date filters do reduce bytes processed (one day: 1.7 MB against 66.6 MB for the whole column) but nothing uses them.
4. Incremental marts do not reduce billed bytes today. The 11 marts bill 1.955 GB per incremental run against a 1.618 GB full-refresh baseline (assembled from two executions). Bytes processed are the same (1.529 GB against 1.519 GB). Slot time falls 88% (167 s against 1,340 s), which matters under capacity pricing but not on-demand.
5. Partitioning alone does nothing. The marts' cutoff is a MAX(...) subquery, which did not prune partitions. A partitioned source plus a literal event\_date bound cut fct\_settlement\_daily from 990.9 MB to 61.9 MB (-93.8%) and fct\_revenue\_daily from 459.3 MB to 31.5 MB (-93.1%), with identical output.
6. The same bound cuts the dry-run read of the two big intermediates by 94.5% (enriched 2,532.9 MB to 139.3 MB; GPV 839.1 MB to 45.9 MB). Estimated whole-graph incremental cost is about 0.84 GB per run (-84%). This is an estimate: no modified build was run.
7. External Iceberg against native BigQuery is a tie on billed bytes at this size (626.5 MiB of Parquet, 3.13 GB logical).
8. The Bronze write scales linearly at about 3,000 to 3,300 events per second: 55.8 minutes for 11M events, roughly 9 to 10 hours if extrapolated to 110M. Memory in the write path stayed under 1.2 GB.
9. Concurrency at 1x showed no degradation up to 20 worker threads, but the test was weak (section 9).
10. Not measurable on this dataset: late-arriving data. The ingested\_at column records generation time, 311 to 673 days after event\_date.
11. After implementation (section 15): the four large models were made incremental and partitioned and checked against baseline copies with no differences. Steady-state billed bytes fell about 82% for the enriched model and about 91% for the GPV model. The enriched model cost about three times its dry-run estimate, so the whole-graph estimate of 0.84 GB is optimistic; a revised model-only estimate is about 1.2 GB, while a measured Airflow run of the dbt layers, tests included, billed 4.379 GB (section 15.6).

**Recommendations (status after implementation on 10 November 2026; details in section 15)**

1. **Implemented.** Make int\_payments\_\_transaction\_enriched and int\_payments\_\_merchant\_daily\_gpv incremental, partitioned by event\_date, with a literal lookback of 9 days (6-day maximum settlement lag plus the 3-day mart lookback).
2. **Implemented**, with a per-stream cutoff for revenue (section 15.1). Give fct\_settlement\_daily and fct\_revenue\_daily the same literal event\_date bound, reading from the partitioned table. Do not rely on the MAX(...) subquery.
3. **Implemented** (a full\_refresh DAG parameter replaces it). Stop running dbt run --full-refresh on every DAG run. Reserve it for controlled rebuilds.
4. **No change needed.** Keep Bronze external at the current size. Revisit if the transactions table grows by an order of magnitude.
5. **Partly implemented.** The dbt tests exist; validation against a real arrival-lag distribution was not done. Validate the 9-day bound against a real arrival-lag distribution and add a dbt test asserting it before trusting the incremental design. The SCD2 merchant join (backdated attribute changes) also needs a test.
6. **Implemented** (the drift check as a BigQuery-side schema check; section 15.1). As a separate engineering change: layered dbt build with tests gating each layer, task retries, dbt parse in CI, and a schema drift check at the CDC-to-Bronze boundary.

## 2. Method and environment

| Item | Value |
| --- | --- |
| GCP project / region | payments-512910 / us-east1 |
| Bronze storage | gs://fintech\_bronze: 626.53 MiB, 137 Parquet files (external Iceberg) |
| Datasets used | bronze (external), bronze\_native, staging and marts (production), native\_staging and native\_marts (study copies) |
| dbt | 4 threads, interactive priority, maximum\_bytes\_billed set to 50,000,000,000 as a safety cap (not a free-tier guarantee) |
| Free-tier context | BigQuery: 1 TiB of queries and 10 GiB of storage free per month. GCS (us-east1): 5 GB-months, 5,000 Class A and 50,000 Class B operations per month. On-demand overage was quoted at $6.25 per TiB on Google's pricing page; not re-verified for us-east1. |

**Measurement sources.** dbt run\_results.json (adapter\_response: bytes billed, bytes processed, slot ms); BigQuery job statistics for ad hoc queries (cache disabled; run time is start to end, queue time is creation to start); dry runs for read-side estimates; scripts/storage\_request\_counts.py for GCS request counts; scripts/measure\_bronze\_scale.py for the Bronze write curve.

**Principles.** Bytes billed is the cost metric. Bytes processed is reported alongside it because BigQuery's 10 MiB minimum per table hides differences between small queries. Measured results, dry-run estimates and extrapolations are labelled as such throughout. Every result is a single run.

**Temporary changes.** The generate\_schema\_name macro was patched to accept an out\_prefix variable and \_sources.yml was changed to read the Bronze schema from a variable, so study runs wrote to separate datasets. Both are reverted with git checkout (section 13). A scratch partitioned table was created for the partitioning experiment and deleted afterwards.

## 3. Data

| Bronze table | Rows | Logical size | Native copy billed |
| --- | --: | --: | --: |
| merchant\_transactions | 8,321,600 | 3.131 GB | 3,132.1 MB |
| merchants | 213,923 | 0.043 GB | 43.0 MB |
| loan\_applications | 451 | \~0 | 10.5 MB |
| loan\_repayments | 523 | \~0 | 10.5 MB |
| risk\_events | 593 | \~0 | 10.5 MB |

Native copies are partitioned by event\_date and clustered by merchant\_id. Transactions cover 2025-01-03 to 2025-12-31 and are 99% of Bronze by size. The three loan and risk tables are tiny, so at 1x almost every byte billed traces to one table.

int\_payments\_\_transaction\_enriched is a 3.43 GB table (8,321,600 rows), unpartitioned and unclustered. event\_date is populated for every row; settlement\_date is null for 664,172 rows (92% populated). Two marts read it directly (fct\_revenue\_daily and fct\_settlement\_daily); a comment in the model says four.

## 4. Baseline: external full refresh (25 models)

One run of dbt run --full-refresh against the external tables: 25 of 25 passed (11 incremental, 9 table, 5 view), 38.2 s wall clock. Totals: 5.115 GB billed, 4.942 GB processed, 1,689.5 slot-seconds.

| Model | Type | Wall (s) | Billed (GB) | Processed (GB) | Slot (s) |
| --- | --- | --: | --: | --: | --: |
| int\_payments\_\_transaction\_enriched | table | 6.1 | 2.533 | 2.533 | 230.4 |
| fct\_settlement\_daily | incremental | 7.1 | 0.988 | 0.987 | 209.0 |
| int\_payments\_\_merchant\_daily\_gpv | table | 4.6 | 0.840 | 0.839 | 21.1 |
| fct\_revenue\_daily | incremental | 6.5 | 0.458 | 0.458 | 187.6 |
| fct\_loan\_unit\_economics | incremental | 2.9 | 0.031 | 0.003 | 2.5 |
| fct\_merchant\_monthly\_summary | incremental | 3.9 | 0.031 | 0.021 | 2.4 |
| int\_risk\_\_merchant\_risk\_daily | table | 4.7 | 0.031 | 0.011 | 7.7 |
| merchants\_scd | table | 4.1 | 0.025 | 0.024 | 8.1 |
| fct\_payments\_daily | incremental | 7.1 | 0.025 | 0.024 | 168.8 |
| int\_lending\_\_loan\_daily\_snapshot | table | 4.0 | 0.021 | 0.000 | 9.3 |
| fct\_risk\_events\_daily | incremental | 6.6 | 0.021 | 0.000 | 67.6 |
| fct\_merchant\_health\_daily | incremental | 7.8 | 0.021 | 0.020 | 203.1 |
| int\_payments\_\_merchant\_gpv\_rolling | table | 3.2 | 0.015 | 0.014 | 1.7 |
| dim\_date | table | 5.1 | 0.010 | 0.000 | 8.5 |
| fct\_collections\_performance\_daily | incremental | 5.1 | 0.010 | 0.000 | 17.1 |
| fct\_loan\_applications\_daily | incremental | 5.7 | 0.010 | 0.000 | 55.0 |
| int\_lending\_\_loan\_fee\_schedule | table | 3.4 | 0.010 | 0.000 | 3.0 |
| fct\_vintage\_curves | incremental | 2.7 | 0.010 | 0.004 | 0.8 |
| dim\_merchants | table | 2.5 | 0.010 | 0.001 | 0.6 |
| fct\_loan\_portfolio\_daily | incremental | 9.7 | 0.010 | 0.003 | 485.4 |
| 5 staging models | view | 1.7 to 2.1 | 0.000 | 0.000 | 0.0 |

**Reading it.** The top four models bill 4.819 GB of 5.115 GB (94.2%). Values of 0.010 GB are the 10 MiB billing minimum, not real scans. Ranking by slot time gives a different order: fct\_loan\_portfolio\_daily (485.4 s), enriched (230.4), settlement (209.0), merchant health (203.1), revenue (187.6), payments daily (168.8). Window-heavy models cost slots without costing bytes, so the report shows both. The two big marts bill the same bytes in the external and native runs, which shows they read the same intermediate table.

The dbt wall time (38.2 s) differs from the 65 s `dbt_marts` task time in the earlier 1,000-merchant Airflow run recorded in [Cost and performance](cost-and-performance.md). The gap was not investigated.

## 5. Bronze date filters: external against native

The query was SELECT SUM(amount\_minor) FROM merchant\_transactions with different event\_date filters, cache off.

| Filter | External processed (MB) | Native processed (MB) | Billed, both (MB) |
| --- | --: | --: | --: |
| 1 day | 1.669 | 0.576 | 10.49 |
| 7 days | 4.949 | 4.604 | 10.49 |
| 1 month | 18.674 | 18.674 | 18.87 |
| whole table | 66.573 | 66.573 | 67.11 |

- Date filters reduce bytes processed on both layouts, but the 10 MiB minimum hides the difference on the bill.
- Billed bytes are identical in all four cases, so native and external are a tie on query cost at this size. Native scans less for a one-day filter (2.9 times less) because it is partitioned by day. Why a daily filter helps on a monthly-partitioned external layout was not investigated.
- A keyword search of staging and intermediate SQL found only an SCD2 join condition (event\_date >= DATE(m.dbt\_valid\_from)) and lending snapshot comparisons. None restricts the Bronze read, so no model uses the pruning.
- Selecting one column (amount\_minor, 66.6 MB) is 47 times cheaper than the whole table (3.13 GB). Column width matters more than date filters.
- One-second timings were inconclusive (native about 1.0 to 1.3 s, external about 0.5 to 0.6 s) and are not used as evidence.
- Extrapolation, not measured: at 10 times the volume a one-day native filter would be about 6 MB, below the 10 MiB floor, while the external layout would not.

## 6. Full refresh against incremental (native marts)

The full-refresh baseline is the native marts-only run: ten models in one invocation, plus fct\_loan\_unit\_economics retried separately after a missing provision\_rates seed in native\_staging. The incremental run was dbt run --select path:models/marts with no full-refresh flag.

| Model | Full billed (MB) | Incr. billed (MB) | Full slot (s) | Incr. slot (s) |
| --- | --: | --: | --: | --: |
| fct\_settlement\_daily | 987.8 | 1,022 | 164.3 | 19.1 |
| fct\_revenue\_daily | 458.2 | 491 | 137.4 | 16.3 |
| fct\_merchant\_monthly\_summary | 31.5 | 73 | 3.8 | 4.5 |
| fct\_payments\_daily | 25.2 | 63 | 147.7 | 14.2 |
| fct\_risk\_events\_daily | 21.0 | 63 | 74.7 | 19.8 |
| fct\_merchant\_health\_daily | 21.0 | 53 | 253.2 | 14.6 |
| fct\_collections\_performance\_daily | 10.5 | 52 | 17.1 | 9.0 |
| fct\_loan\_applications\_daily | 10.5 | 52 | 56.6 | 15.5 |
| fct\_loan\_unit\_economics | 31.5 (retry) | 42 | 3.6 | 2.2 |
| fct\_loan\_portfolio\_daily | 10.5 | 21 | 480.3 | 50.3 |
| fct\_vintage\_curves | 10.5 | 21 | 1.0 | 1.3 |
| **Total** | **1,618** | **1,955** | **1,339.7** | **166.8** |

- Billed bytes rose 21% and processed bytes were flat (1.519 GB against 1.529 GB). Settlement processed 0.990 GB incrementally against 0.987 GB in a full refresh, because a 3-day filter on an unpartitioned table cannot prune anything.
- Slot time fell 88%. On-demand billing ignores slots, so there is no saving there, but it would reduce compute under capacity pricing.
- Hypothesis, unverified: the higher billed bytes on the small marts come from each incremental model issuing several statements, each paying the 10 MiB minimum.
- Caveats: single run; the full baseline is stitched from two executions. Intermediate and dimension tables are rebuilt in full on every run regardless of the marts' lookbacks.

## 7. Partitioning and date bounds

### 7.1 Settlement lag

Over the 7,657,428 transactions with a settlement date, days between event\_date and settlement\_date: minimum 0, median 1, 99th percentile 4, maximum 6. A 3-day lookback on settlement\_date therefore maps to at most a 9-day event\_date window (about 2.5% of the year). This comes from synthetic data.

### 7.2 Mart experiment (measured)

The marts' compiled incremental SQL was run four ways against the original enriched table and a scratch copy partitioned by event\_date (3,426.7 MB billed to create). Variant 2 used a literal bound of event\_date >= 2025-12-20. Every variant returned identical output (860 rows for settlement, 4 for revenue, matching fingerprints).

| Variant | Settlement billed (MB) | Revenue billed (MB) |
| --- | --: | --: |
| 0: as written, original table | 990.9 | 459.3 |
| 1: as written, partitioned copy | 990.9 | 459.3 |
| 2: literal bound, partitioned copy | 61.9 (-93.8%) | 31.5 (-93.1%) |
| 3: literal bound, original table (control) | 1,057.0 | 459.3 |

Partitioning alone changed nothing, because the marts' cutoff is a MAX(...) subquery that did not prune. The control shows a bound without partitioning does not help (settlement got 66 MB worse, the cost of reading event\_date). Both are required. A literal date computed with a dbt run\_query was later built and does prune (section 15).

### 7.3 Intermediate reads (dry run)

| Model | Full read (MB) | Bound 2025-12-20 (MB) | Bound 2025-12-31 (MB) |
| --- | --: | --: | --: |
| int\_payments\_\_transaction\_enriched | 2,532.9 | 139.3 (-94.5%) | 11.6 (-99.5%) |
| int\_payments\_\_merchant\_daily\_gpv | 839.1 | 45.9 (-94.5%) | 3.6 (-99.6%) |

These are read-side estimates only. They exclude the write and the cost of handling late changes.

### 7.4 Whole-graph estimate (not measured; superseded by section 15.4)

| Part | Today (GB) | Estimated incremental (GB) |
| --- | --: | --: |
| int\_payments\_\_transaction\_enriched | 2.533 | 0.139 |
| int\_payments\_\_merchant\_daily\_gpv | 0.840 | 0.046 |
| fct\_settlement\_daily | 0.988 | 0.062 |
| fct\_revenue\_daily | 0.458 | 0.032 |
| 9 other marts (incremental figures measured) | 0.169 | 0.440 |
| other intermediates and dimensions | 0.122 | 0.122 |
| **Total** | **5.115** | **about 0.84 (-84%)** |

The nine other marts cost more incrementally than under a full refresh (section 6), so the saving comes entirely from the four large models. Section 15.4 shows the enriched model costing about three times its estimate here, so this total is optimistic.

### 7.5 Conditions on the recommendation

- **Late arrivals.** An event\_date window silently misses events that arrive after their window closes. ingested\_at in this dataset is 311 to 673 days after event\_date (median 437), so it records when the generator ran, not a simulated delay. Late-arrival behaviour cannot be measured here. The alternative design, selecting new rows by ingested\_at and overwriting only the partitions they touch, handles late arrivals correctly but costs more and needs a simulator that emits late events.
- **SCD2.** A backdated merchant attribute change alters old enriched rows that a short window never rebuilds.
- **Bound derivation.** The 6-day maximum lag is a synthetic-data figure. Any production bound needs a safety margin and a dbt test asserting the lag. Status 10 November: the lag is asserted by `assert_settlement_lag_within_lookback` (last 30 days of event_date), backdated merchant changes by the periodic `assert_enriched_scd_not_stale`, and events landing outside the window by the periodic `assert_enriched_complete_vs_staging`. All three pass on this data; none has been exercised with a failing case.

## 8. Bronze write scaling (local runs)

| Events | Write, both passes | Per million events | Second run, no new data | Replay check | Peak memory in write | Peak memory overall | Script-reported Class A |
| --- | --: | --: | --: | --: | --: | --: | --: |
| 1.1M | 371.5 s | 338 s | 188.6 s | 327.4 s | 931 MB | 1,384 MB | 74 |
| 3.3M | 1,109.8 s | 336 s | 525.6 s | 1,006.8 s | 965 MB | 2,249 MB | 193 |
| 11.0M | 3,346.2 s | 304 s | 1,757.1 s | 3,183.3 s | 1,174 MB | 2,858 MB | 621 |

All three runs passed 12 of 12 hard checks, including idempotency of the second run. Two runs raised the memory-growth flag.

- The write is linear at about 3,000 to 3,300 events per second. Extending the line to 110M events gives about 9.3 to 10.3 hours. That is an extrapolation from three single runs.
- The script's own estimate line (sandbox seconds times 3.1) does not apply because these timings were taken on the Air. The measured figures are 6.2, 18.5 and 55.8 minutes. The 124-minute Bronze write in [Cost and performance](cost-and-performance.md) was an earlier Airflow run on a fresh bucket (8.5M events) and was not reconciled with these local timings.
- A rerun that appends nothing takes 47 to 53% of the original write time. The task is idempotent but a retry is not cheap, which matters for the DAG retry policy.
- Memory in the write path grew from 931 MB to 1,174 MB across ten times the data. The larger peaks come from the replay check. Growth per million rows was 62.8 MB at 3.3M and 15.4 MB at 11M, both above the script's flag threshold of 15, but the rate fell as the table grew (three points only).
- Commit time for the last tenth against the first was x0.57, x0.68 and x1.27, so the largest run was the only one that slowed. One run at one size.
- At the script-reported rate of 56 to 67 Class A operations per million events, a 10x write would need roughly 6,000, above the 5,000 monthly free allowance.

## 9. Concurrency at 1x

Four marts (fct\_payments\_daily, fct\_settlement\_daily, fct\_revenue\_daily, fct\_merchant\_health\_daily) were queried with 30-day date filters, three queries per worker, cache off.

| Workers | Queries | p50 | p95 | Max queue | Avg billed |
| --: | --: | --: | --: | --: | --: |
| 1 | 3 | 0.2 s | 0.2 s | 0.1 s | 10.5 MB |
| 5 | 15 | 0.1 s | 0.2 s | 0.3 s | 10.5 MB |
| 10 | 30 | 0.1 s | 0.2 s | 0.1 s | 10.5 MB |
| 20 | 60 | 0.1 s | 0.2 s | 0.1 s | 10.5 MB |

- Latency stayed flat, and every query billed the 10 MiB minimum. At that floor, one free TiB covers roughly 104,857 queries per month.
- Weak test: wall time grew from 21 s to 171 s because each worker starts the bq command-line tool. The true overlap inside BigQuery was probably well below the worker count. A test with the Python client library would be stronger.
- It ran at 1x on tiny marts. A 10x run was not done.

## 10. GCS requests and storage

- Free-tier limits confirmed (5 GB-months, 5,000 Class A, 50,000 Class B per month in us-east1). Class A covers writes and listings; Class B covers reads and metadata gets.
- Request counts for the window ending 9 November 2026: roughly 3,535 to 3,564 Class A and 32,319 to 32,851 Class B, about 71% and 65% of the monthly allowances. cdc\_landing is the largest source (23,000 Class B, almost all object reads). These are request-count estimates, not invoice amounts.
- The counts are approximate: Class B totals fell between two runs of the script (32,851 to 32,319), which a cumulative count should not do.
- fintech\_bronze Class B stayed at 6,750 after a full external dbt run, a CTAS and repeated external scans. BigQuery's reads of the external tables therefore do not appear in this metric, and the GCS read cost of the external approach is unmeasured, not zero. Class A did rise slightly, matching the listing commands run during the study.
- Overage rates (about $0.0004 per 1,000 Class B and $0.005 per 1,000 Class A) were found only on a secondary page and should be confirmed against Google's pricing before use. At those rates the remaining Class B headroom is worth well under a cent.
- Storage: Bronze occupies 626.53 MiB of Parquet in GCS against 3.13 GB logical in BigQuery. Native storage of the five copied tables is about 3.2 GB, under the free 10 GiB.

## 11. Incremental correctness (limited)

All 11 marts are incremental. Until 10 November the DAG's --full-refresh flag meant none of this logic ran incrementally. The DAG no longer passes it by default (section 15), so these lookbacks are now live in the pipeline. Only fct\_settlement\_daily and fct\_revenue\_daily have been checked against a full refresh since.

| Model | Strategy | Unique key | Lookback |
| --- | --- | --- | --- |
| fct\_merchant\_monthly\_summary | insert\_overwrite | merchant\_id, month | 3 months |
| fct\_merchant\_health\_daily | insert\_overwrite | none | 3 days |
| fct\_vintage\_curves | merge | vintage\_month, month\_on\_book | 3 months |
| fct\_payments\_daily | insert\_overwrite | none | 3 days |
| fct\_loan\_unit\_economics | merge | loan\_id, fee\_month | 3 months |
| fct\_revenue\_daily | insert\_overwrite | none | 3 days |
| fct\_settlement\_daily | insert\_overwrite | none | 3 days |
| fct\_risk\_events\_daily | insert\_overwrite | none | 3 days |
| fct\_collections\_performance\_daily | insert\_overwrite | none | 3 days |
| fct\_loan\_applications\_daily | insert\_overwrite | none | 3 days |
| fct\_loan\_portfolio\_daily | merge | snapshot\_date | 30 days |

Two models were checked against source-derived values. fct\_vintage\_curves had no missing vintage and month-on-book combinations (111 rows, vintages 2025-04 to 2025-12, maximum month on book 13). fct\_loan\_unit\_economics matched exactly (0 missing, 0 extra, 0 differing rows). The other four lookback risks (merchant health, loan portfolio, monthly summary, settlement) are design risks that were not tested: a historical correction outside a short lookback would leave stale rows. No defect was found in the data that was checked.

## 12. Limitations

- Every figure is a single run on synthetic data at 1x. There are no repeats and no variance.
- The native full-refresh baseline was built from two executions, so the billed comparison with the incremental run is approximate.
- Four modified incremental models were built and measured on 10 November (section 15), and a whole-graph run of the dbt layers was measured in Airflow (section 15.6). Sections 7.2 to 7.4 remain SELECT bytes and dry-run estimates.
- GCS read cost for external tables, late-arrival behaviour and SCD2 restatement behaviour were not measurable.
- The 10x BigQuery build was not run, to stay inside free tiers. Scale evidence is the local Bronze write curve plus extrapolation.
- The concurrency test was weak (section 9).
- Dollar figures were deliberately not computed: at 5.1 GB per full run, spend is far below the free tier.

## 13. Next steps and cleanup

**Cleanup, completed 10 November 2026**

- Reverted the temporary patches with git checkout: dbt/macros/generate\_schema\_name.sql dbt/models/staging/\_sources.yml
- Dropped datasets bronze\_native, native\_staging and native\_marts; scratch\_enriched\_part was confirmed gone.
- Removed /tmp/p6\_550, /tmp/p6\_1650 and /tmp/p6\_5500.
- The untracked infra/\*.tfplan files were left alone. A new untracked logs/ directory holds only dbt.log.

**Implementation order**

1. **Done.** Layered build with tests gating each layer, DAG retries, dbt parse in CI and the schema drift check, as a separate change.
2. **Done.** Incremental, event\_date-partitioned enriched and GPV models with literal lookback bounds, plus the same bounds on the two big marts.
3. **Partly done** (four models measured and one whole-graph dbt run; section 15). Rerun the measurements in sections 4 and 6 and replace the estimates with measured builds.
4. **Partly done.** The dbt tests for the bound and the SCD2 join exist. Validating the lookback against real arrival lags was deliberately not done (section 15.5).

## 14. How to remeasure after the changes

**Status (10 November 2026).** Partly executed, not by this procedure: correctness was checked with baseline copies and EXCEPT DISTINCT diffs instead of fingerprints, and bytes were read from INFORMATION\_SCHEMA.JOBS\_BY\_USER instead of the stats helper (section 15). A whole-graph run of the dbt layers was measured in Airflow (section 15.6), but not by the procedure below. The lag test sketched in step 6 now exists as `assert_settlement_lag_within_lookback`.

Rerun the same measurements on the same data and compare them with the baselines below. Every figure in this report is a single run, so run each measurement twice and keep the second.

**1. Capture fingerprints before changing anything**

A fingerprint (row count plus a hash of every row) proves a mart's output is unchanged after the changes. If a mart has a build-timestamp column, its hash will differ on every run; exclude that column with EXCEPT. This costs about 0.1 GB.

```bash
mart_fp() {
  for m in fct_settlement_daily fct_revenue_daily fct_payments_daily fct_merchant_health_daily fct_risk_events_daily fct_collections_performance_daily fct_loan_applications_daily fct_loan_unit_economics fct_loan_portfolio_daily fct_vintage_curves fct_merchant_monthly_summary; do
    bq query --project_id=payments-512910 --dataset_id=payments-512910:marts --use_legacy_sql=false --location=us-east1 --format=csv --quiet "SELECT '$m' AS model, COUNT(*) AS n, BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS h FROM $m AS t" | tail -1
  done
}
mart_fp | tee ~/marts_fingerprints_before.txt
```

**2. Save the stats helper in the repo** as scripts/dbt\_run\_stats.py. It reads target/run\_results.json and prints bytes billed, bytes processed and slot time per model, including tests.

```python
import json, sys
r = json.load(open(sys.argv[1] if len(sys.argv) > 1 else 'target/run_results.json'))
rows = []
for x in r['results']:
    a = x.get('adapter_response') or {}
    rows.append((x['unique_id'].split('.')[-1], x['status'], x['execution_time'], a.get('bytes_billed') or 0, a.get('bytes_processed') or 0, a.get('slot_ms') or 0))
rows.sort(key=lambda t: -t[3])
print('%-44s%-9s%7s%11s%9s%9s' % ('node', 'status', 'sec', 'GB_billed', 'GB_proc', 'slot_s'))
for n, s, t, b, p, sl in rows:
    print('%-44s%-9s%7.1f%11.3f%9.3f%9.1f' % (n, s, t, b / 1e9, p / 1e9, sl / 1000))
print('TOTAL billed=%.3fGB processed=%.3fGB slot=%.1fs wall=%.1fs' % (sum(t[3] for t in rows) / 1e9, sum(t[4] for t in rows) / 1e9, sum(t[5] for t in rows) / 1000, r['elapsed_time']))
```

**3. Baselines to compare against**

| Measurement | Baseline | Source |
| --- | --- | --- |
| Whole graph, full refresh | 5.115 GB billed, 4.942 GB processed, 1,689.5 slot-s, 38.2 s | Section 4 |
| 11 marts, incremental run | 1.955 GB billed, 1.529 GB processed, 166.8 slot-s | Section 6 |
| fct\_settlement\_daily, incremental | 1,022 MB billed | Section 6 |
| fct\_revenue\_daily, incremental | 491 MB billed | Section 6 |
| Whole graph, incremental | none measured; estimate about 0.84 GB | Section 7.4 |
| Test queries | none measured | not covered by this study |

**4. Runs**

Before starting, check free-tier headroom for the month (the free allowance is 1 TiB):

```bash
bq query --project_id=payments-512910 --use_legacy_sql=false --location=us-east1 --format=pretty 'SELECT ROUND(SUM(total_bytes_billed)/POW(1024,4),4) AS tib_billed_this_month FROM `region-us-east1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE creation_time >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH) AND job_type = "QUERY"'
```

The first run after the model changes must be a full refresh, so BigQuery creates the partitioned tables. Then run the normal incremental run twice and record the second. The whole sequence should cost roughly 10 GB billed plus the test queries, about 1% of the free TiB.

```bash
cd dbt
dbt run --full-refresh 2>&1 | tail -6
python3 ../scripts/dbt_run_stats.py | tee ~/remeasure_full.txt
dbt run 2>&1 | tail -4
dbt run 2>&1 | tail -4
python3 ../scripts/dbt_run_stats.py | tee ~/remeasure_incremental.txt
dbt run --select path:models/marts 2>&1 | tail -4
python3 ../scripts/dbt_run_stats.py | tee ~/remeasure_marts.txt
dbt build 2>&1 | tail -6
python3 ../scripts/dbt_run_stats.py | tee ~/remeasure_build.txt
cd ..
```

The study did not measure test cost. The project has 165 data tests, each a query, and a layered build runs them on every run, so their bytes may be a visible share of the total. The last run above captures them; compare it with the incremental run to see the difference.

Then rerun the fingerprints and compare:

```bash
mart_fp > ~/marts_fingerprints_after.txt
diff ~/marts_fingerprints_before.txt ~/marts_fingerprints_after.txt && echo identical
```

**5. Pass criteria (suggested, not derived from the study)**

- Fingerprints identical for all 11 marts. This is required.
- Whole-graph incremental run close to the 0.84 GB estimate. Investigate if it is above about 1.1 GB.
- fct\_settlement\_daily and fct\_revenue\_daily incremental runs near the experiment values (about 62 MB and 32 MB), against baselines of 1,022 MB and 491 MB.
- Full refresh no more expensive than the 5.115 GB baseline.
- Test cost recorded separately, with no pass mark until it has been measured once.

**6. Tests for the new conditions**

The 9-day bound assumes the settlement lag never exceeds 6 days. A singular test asserts it (sketch, untested):

```sql
SELECT *
FROM {{ ref('int_payments__transaction_enriched') }}
WHERE settlement_date IS NOT NULL
  AND DATE_DIFF(settlement_date, event_date, DAY) > {{ var('max_settlement_lag_days', 6) }}
```

Two conditions have no ready test. Backdated merchant attribute changes: change a merchant attribute with a date before the window, rebuild, and check whether old enriched rows update. Late-arriving events: needs a simulator that emits late events, because ingested\_at in the current data records generation time.

**7. Recording the result**

Add an After column to the tables in sections 4, 6 and 7, mark each estimate as measured where a run replaced it, and keep the before and after figures side by side. Rerun anything surprising before reporting it.

## 15. Implementation and results (10 November 2026)

All figures are single runs. Bytes billed come from INFORMATION\_SCHEMA.JOBS\_BY\_USER (top-level jobs). Each dbt model is a multi-statement script, and the mapping of billed jobs to models is inferred by matching dbt's reported bytes processed. Treat per-model billed figures as approximate.

### 15.1 What changed

| Recommendation | Change | Status |
| --- | --- | --- |
| 1 | int\_payments\_\_transaction\_enriched and int\_payments\_\_merchant\_daily\_gpv are incremental (insert\_overwrite), partitioned by event\_date (txn\_date for GPV), with a 9-day lookback. Cutoffs are literal dates computed by a macro (`incremental_cutoff`) using run\_query on the target table, not CURRENT\_DATE, because the synthetic event dates are 311 to 673 days old. | Done, diff-verified |
| 2 | fct\_settlement\_daily reads with both an event\_date bound (9 days) and a settlement\_date bound (3 days). fct\_revenue\_daily bounds its card branch. | Done, diff-verified |
| 2 (defect found) | The revenue mart's cutoff was the maximum revenue\_date across both streams. Loan fees run to 2026-11-01 and card revenue stops at 2025-12-31, so the card branch would have read nothing and never refreshed. This was already true before the changes. The cutoff is now the earlier of the two streams' latest dates, minus 3 days. | Fixed, diff-verified |
| 3 | The DAG no longer passes --full-refresh. A full\_refresh parameter (default false) enables it for controlled rebuilds. | Done |
| 4 | None. | No change |
| 5 | Singular tests: `assert_settlement_lag_within_lookback`, `assert_merchants_scd_no_overlap`, `assert_merchants_scd_one_current_row` (run on every build); `assert_enriched_scd_not_stale` and `assert_enriched_complete_vs_staging` (tagged periodic, excluded from the DAG build). | Done, all pass |
| 6 | Layered dbt build (staging, dimensions, intermediate, marts) with tests gating each layer; retries of 2 (none on bronze\_write, which appends); `dbt parse` in CI; a BigQuery external-table schema check (`scripts/check_bq_schema.py`) between register\_bq and the first dbt layer. CDC and Bronze drift were already covered in Python (SchemaDriftError, TableDriftError, bronze.check), so the schema check covers only the last hop. | Done; run in Airflow under pipeline-runner (section 15.6), except bronze\_verify |
| Arrival-lag validation | Not done (section 15.5). | Skipped |

### 15.2 Correctness check

The four tables were copied, the last 30 days were deleted from each, and the incremental build was run without --full-refresh. A two-way EXCEPT DISTINCT against the copies returned 0 rows in each direction for all four tables. The revenue mart was re-checked after its cutoff fix, also 0 and 0. This shows the incremental logic reproduces the full-refresh output on this dataset. It does not test late arrivals or a failing case for any new test.

### 15.3 Bytes billed

| Model | Full refresh (before) | Steady-state incremental | Recovery of 30 deleted days |
| --- | --: | --: | --: |
| int\_payments\_\_transaction\_enriched | 2,533 MB | 453 MB (-82%) | 1,253 MB (-51%) |
| int\_payments\_\_merchant\_daily\_gpv | 840 MB | 73 MB (-91%) | 188 MB (-78%) |
| fct\_settlement\_daily | 991 MB | not attributed (24.1 MiB processed) | 223 MB (-77%) |
| fct\_revenue\_daily | 459 MB | not attributed (7.9 MiB processed after the cutoff fix) | 105 MB (-77%) |

Each incremental model also issues a small cutoff query (inferred about 67 MB for enriched and 10 MB for GPV in one run), which is not included above. The four large models together cost roughly 0.6 to 0.7 GB per steady-state run against about 4.8 GB, about -87%, partly inferred.

### 15.4 Against the estimates

- Enriched: the dry-run estimate was 0.139 GB; the measured cost was 0.453 GB billed (424 MiB processed), about three times higher. The cause was not investigated. Unverified possibilities: the scripted insert\_overwrite reads or writes more than the dry-run SELECT, or the SCD2 join and column set add bytes.
- GPV: estimate 0.046 GB, measured 0.073 GB, partly the 10 MiB minimum.
- Revised whole-graph estimate: about 0.65 GB (four large models) + 0.44 GB (nine other marts, section 7.4) + 0.12 GB (others) = about 1.2 GB, about -77% against 5.115 GB. This was extrapolated. A measured Airflow run of the four dbt layers (models, tests and cutoff queries together) billed 4.379 GB, about 3.2 GB above the model-only estimate. The split between models and tests has not been measured (15.6).

### 15.5 Open items

- A whole-graph run of the dbt layers was measured in Airflow (15.6), but the section 14 procedure (steps 3 and 4, with its fingerprints) was not followed.
- The enriched model costs about three times its estimate (15.4).
- Test cost is probably the largest part of the steady-state bill. About 168 data tests each bill at least 10 MiB (about 1.8 GB at the minimum), and three jobs of about 317 MB each appeared while the enriched tests ran (inferred to be tests on transaction\_id). The split between models and tests has not been measured. Options to try: restricting tests to recent partitions with a `where` config, and combining not-null checks into fewer queries.
- The periodic tests are not scheduled and run only by hand (`dbt test -s tag:periodic`).
- bronze\_verify has not been run in Airflow (unchanged by this work), and no scheduled DAG run has been done. The tasks were run one at a time with `airflow tasks test`.
- Late-arrival validation against a real arrival-lag distribution was not done. A simulator that emits late events would mean choosing the distribution it then measures, so it cannot validate the bound. `assert_enriched_complete_vs_staging` covers the failure mode (late events outside the window), and a small fault-injection demo (one old-dated row inserted into a scratch copy, test goes red, a full refresh turns it green) would show it working.
- The nine other marts still cost more incrementally than under a full refresh (section 6) and were not changed.

### 15.6 Run in Airflow (10 November 2026)

Each task was run on its own with `airflow tasks test`, as the pipeline-runner account.

| Task | Result |
| --- | --- |
| check\_bq\_schema | OK, 5 external tables match the contract |
| dbt\_build\_staging | 47 of 47 passed (5 views, 42 tests) |
| dbt\_build\_dimensions | 32 of 32 passed (3 tables, 29 tests) |
| dbt\_build\_intermediate | 49 of 49 passed (2 incremental models, 4 tables, 43 tests) |
| dbt\_build\_marts | 65 of 65 passed (11 incremental models, 54 tests) |
| register\_bq | All five tables registered; row counts 213,923, 8,321,600, 451, 523 and 593 match the Bronze marker |
| bronze\_write | Passed on the second attempt (below), 40 minutes, nothing to append |
| bronze\_verify | Not run |

**BigQuery cost.** The dbt tasks above billed 4.379 GB across 200 top-level jobs for the pipeline-runner account (INFORMATION\_SCHEMA.JOBS\_BY\_PROJECT, each script counted once). That figure includes the models, about 168 data tests and the cutoff queries. The model-only estimate in 15.4 was about 1.2 GB, so roughly 3.2 GB is unaccounted for by models. Tests are the likely cause (15.5), but this has not been split. It is not comparable with the 5.115 GB full-refresh baseline, which did not include tests. At this size one run is about 0.4% of the monthly free query allowance.

**GCS cost.** Class B operations rose from about 32,100 to about 35,700 across the failed and the passing `bronze_write` attempts, roughly 3,600. Class A was 3,349 at the end (the script's earlier figure was 3,553; the drop is unexplained). That puts the month at about 67% of Class A and 72% of Class B.

**A failure that did not reproduce.** The first `bronze_write` attempt failed. Its catalog rebuild crashed with "table iceberg\_tables already exists" immediately after the catalog file was deleted, the `|| true` in the command hid that, and the write then refused to run ("Nothing was written"). A manual rebuild worked, and the next attempt passed through the same code path. The cause was not found. Stale SQLite sidecar files (none present), a bind-mounted filesystem (it is an overlay filesystem) and a stray background process (none found) were checked and ruled out.

**Duration.** `bronze_write` took about 40 minutes (11:04:42 to 11:45:07) to find nothing to append. It reads every landed object, and the Docker setup has 2 cores. Whether the machine slept during the run is unknown.

---

*See also [Cost and performance](cost-and-performance.md) for GCS operation costs, timings and free-tier headroom.*
