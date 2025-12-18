FROM apache/airflow:2.9.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
  "click==8.1.7" \
  "dbt-core==1.8.4" \
  "dbt-snowflake==1.8.4" \
  "snowflake-connector-python[pandas]==3.12.0" \
  "pandas==2.2.2"
