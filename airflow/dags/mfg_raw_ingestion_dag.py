from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow"
INGEST_SCRIPT = f"{PROJECT_ROOT}/ingestion/python/load_raw_to_snowflake.py"
DBT_DIR = f"{PROJECT_ROOT}/dbt_project"
DBT_PROFILES_DIR = f"{PROJECT_ROOT}/dbt_profiles"


def airflow_context_env() -> dict:
    return {
        "AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
        "AIRFLOW_RUN_ID": "{{ run_id }}",
        "AIRFLOW_TASK_ID": "{{ task.task_id }}",
        "AIRFLOW_LOGICAL_DATE": "{{ logical_date.isoformat() if logical_date else '' }}",
    }


with DAG(
    dag_id="mfg_raw_ingestion_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["mfg", "raw", "dbt", "run-aware"],
) as dag:

    ingest_raw_to_snowflake = BashOperator(
        task_id="ingest_raw_to_snowflake",
        bash_command=f"python {INGEST_SCRIPT}",
        env=airflow_context_env(),
        append_env=True,
    )

    dbt_run_all = BashOperator(
        task_id="dbt_run_all",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
        append_env=True,
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
        append_env=True,
    )

    ingest_raw_to_snowflake >> dbt_run_all >> dbt_test_all
