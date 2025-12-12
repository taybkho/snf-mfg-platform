from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="demo_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # only run manually
    catchup=False,
    tags=["demo"],
) as dag:
    start = EmptyOperator(task_id="start")
