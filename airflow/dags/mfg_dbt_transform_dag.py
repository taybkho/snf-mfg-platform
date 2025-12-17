from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT_ROOT = "/opt/airflow"
DBT_DIR = f"{PROJECT_ROOT}/dbt_project"
DBT_PROFILES_DIR = f"{PROJECT_ROOT}/dbt_profiles"

with DAG(
    dag_id="mfg_dbt_transform_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="15 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["mfg", "dbt", "transforms"],
) as dag:

    wait_for_raw = EmptyOperator(task_id="wait_for_raw")

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}",
        append_env=True,
    )

    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=f"cd {DBT_DIR} && dbt run --select core --profiles-dir {DBT_PROFILES_DIR}",
        append_env=True,
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}",
        append_env=True,
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
        append_env=True,
    )

    wait_for_raw >> dbt_run_staging >> dbt_run_core >> dbt_run_marts >> dbt_test_all
