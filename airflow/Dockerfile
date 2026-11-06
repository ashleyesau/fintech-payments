FROM apache/airflow:2.9.3-python3.11

# Install pipeline packages on top of the Airflow base image.
# No constraints file: the Airflow 2.9.3 constraints pin pyiceberg==0.6.1 which conflicts
# with the pipeline requirement of pyiceberg==0.12.0. Pipeline tasks run as BashOperator
# subprocesses so their package versions are independent of Airflow's own imports.
# Build context is the repo root (docker-compose sets context: ..).
COPY airflow/requirements-pipeline.txt /requirements-pipeline.txt
RUN pip install --no-cache-dir --default-timeout 100 -r /requirements-pipeline.txt
