"""
mfg_pipeline.py (shared helpers)

This module contains shared constants/helpers used by Airflow DAGs in this repo.
"""

from __future__ import annotations

from typing import Dict

PROJECT_ROOT = "/opt/airflow"
INGEST_SCRIPT = f"{PROJECT_ROOT}/ingestion/python/load_raw_to_snowflake.py"
DBT_DIR = f"{PROJECT_ROOT}/dbt_project"
DBT_PROFILES_DIR = f"{PROJECT_ROOT}/dbt_profiles"


def airflow_context_env() -> Dict[str, str]:
    return {
        "AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
        "AIRFLOW_RUN_ID": "{{ run_id }}",
        "AIRFLOW_TASK_ID": "{{ task.task_id }}",
        "AIRFLOW_LOGICAL_DATE": "{{ logical_date.isoformat() if logical_date else '' }}",
    }
