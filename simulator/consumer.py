"""
consumer.py — V2 SCOPE. Not built in V1.

In V2, this module will implement a Redpanda/Kafka consumer that:
  - Reads from merchant_transactions, loan_applications, loan_repayments topics
  - Flushes raw events to GCS as NDJSON (bronze layer input)
  - Maintains rolling 5-minute windowed aggregations (GPV, auth rate, funnel
    drop-off) and flushes them to a BigQuery stream_aggregates table

The Python consumer is the portfolio-scale equivalent of Flink — the rolling
aggregates table is something the nightly batch path cannot produce.

V1 writes all events directly to Postgres via psycopg3 (see generator.py).
"""

raise NotImplementedError(
    "consumer.py is V2 scope. "
    "V1 writes events directly to Postgres — see simulator/generator.py."
)
