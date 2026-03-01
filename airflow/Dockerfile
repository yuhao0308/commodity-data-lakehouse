FROM apache/airflow:2.10.5-python3.11

USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# DAGs, plugins, and shared modules are mounted via docker-compose volumes
# This image only bakes in Python dependencies
