FROM apache/airflow:2.9.3-python3.11

# Airflow 2.9.3 uses SQLAlchemy 1.4.x; pyiceberg 0.12.0 requires SQLAlchemy 2.x.
# A shared Python environment cannot satisfy both. Solution: build a separate virtualenv
# for the pipeline at /opt/pipeline-venv so the two dependency sets never conflict.
# BashOperator tasks invoke /opt/pipeline-venv/bin/python and /opt/pipeline-venv/bin/dbt.
USER root
RUN python3 -m venv /opt/pipeline-venv && \
    /opt/pipeline-venv/bin/pip install --no-cache-dir --default-timeout 100 \
        pyiceberg==0.12.0 \
        pyiceberg-core==0.10.1 \
        pyarrow==26.0.0 \
        "dbt-bigquery~=1.9.2"
USER airflow
