"""
mfg_pipeline.py

Airflow DAG to orchestrate:
1) Python ingestion of CSVs into Snowflake RAW schema
2) dbt run (staging, core, marts)
3) dbt test

This DAG runs entirely inside the Airflow container, which has:
- The whole project mounted at /opt/airflow
- Python + dbt + Snowflake connector installed
- Snowflake credentials from environment variables
"""

import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator


# ---------------------- Default DAG configuration ---------------------- #

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


# ---------------------- Define the DAG itself -------------------------- #

with DAG(
    dag_id="mfg_accelerator_pipeline",
    description="End-to-end manufacturing data pipeline: ingestion + dbt.",
    default_args=default_args,
    start_date=dt.datetime(2025, 1, 1),
    schedule_interval=None,  # manual trigger only for now
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1) Ingest RAW data into Snowflake using your Python script
    ingest_raw = BashOperator(
        task_id="ingest_raw_to_snowflake",
        bash_command="python /opt/airflow/ingestion/python/load_raw_to_snowflake.py",

    )

    # 2) Run dbt models (staging, core, marts)
    dbt_run = BashOperator(
        task_id="dbt_run_all",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --profiles-dir /opt/airflow/dbt_profiles"
        ),
    )


    # 3) Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --profiles-dir /opt/airflow/dbt_profiles"
        ),
    )  


    # Set task dependencies:
    # First ingest, then dbt run, then dbt test
    ingest_raw >> dbt_run >> dbt_test
