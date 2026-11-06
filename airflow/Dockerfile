FROM apache/airflow:2.9.3-python3.11

# Install pipeline packages on top of the Airflow base image.
# Using the official Airflow constraints file keeps all packages compatible with 2.9.3.
# Build context is the repo root (docker-compose sets context: ..).
COPY airflow/requirements-pipeline.txt /requirements-pipeline.txt
RUN pip install --no-cache-dir --default-timeout 100 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    -r /requirements-pipeline.txt
