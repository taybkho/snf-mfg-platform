"""
mfg_data_quality_monitoring_dag.py (production-grade, XCom-safe)

Fixes:
- Do NOT push custom classes to XCom (Airflow blocks deserialization unless allowlisted)
- All tasks return JSON-serializable payloads only: list[dict]

Checks:
- RAW vs TRANSFORM rowcount reconciliation
- Duplicate detection on CORE batches
- Freshness check on MARTS KPI table

Writes results to: <DB>.RAW.DATA_QUALITY_AUDIT

Schema routing via env vars:
- MFG_SCHEMA_RAW (default RAW)
- MFG_SCHEMA_TRANSFORM (default CORE)
- MFG_SCHEMA_MARTS (default MARTS)
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pendulum
import snowflake.connector
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


SEVERITY_ERROR = "ERROR"
SEVERITY_WARN = "WARN"


def _get_required_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _get_schema_env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _fqtn(db: str, schema: str, table: str) -> str:
    return f"{db}.{schema}.{table}"


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=_get_required_env("SNOWFLAKE_ACCOUNT"),
        user=_get_required_env("SNOWFLAKE_USER"),
        password=_get_required_env("SNOWFLAKE_PASSWORD"),
        role=_get_required_env("SNOWFLAKE_ROLE"),
        warehouse=_get_required_env("SNOWFLAKE_WAREHOUSE"),
        database=_get_required_env("SNOWFLAKE_DATABASE"),
        autocommit=True,
    )


def ensure_dq_audit_table(cur) -> None:
    db = _get_required_env("SNOWFLAKE_DATABASE")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.RAW")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db}.RAW.DATA_QUALITY_AUDIT (
            CHECK_NAME STRING,
            CHECK_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            SEVERITY STRING,
            STATUS STRING,
            DETAILS STRING
        )
        """
    )


def _fetch_one(cur, sql: str, params: Optional[tuple] = None) -> Any:
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def _result(check_name: str, severity: str, status: str, details: Dict[str, Any]) -> Dict[str, Any]:
    """
    XCom-safe payload: dict only.
    """
    return {
        "check_name": check_name,
        "severity": severity,
        "status": status,  # PASS/FAIL
        "details": details,
    }


def dq_check_raw_vs_transform_rowcounts(**context) -> List[Dict[str, Any]]:
    db = _get_required_env("SNOWFLAKE_DATABASE")
    raw_schema = _get_schema_env("MFG_SCHEMA_RAW", "RAW")
    transform_schema = _get_schema_env("MFG_SCHEMA_TRANSFORM", "CORE")

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        results: List[Dict[str, Any]] = []

        pairs = [
            ("materials", _fqtn(db, raw_schema, "MATERIALS"), _fqtn(db, transform_schema, "CORE_MATERIALS")),
            ("batches", _fqtn(db, raw_schema, "BATCHES"), _fqtn(db, transform_schema, "CORE_BATCHES")),
            ("production_orders", _fqtn(db, raw_schema, "PRODUCTION_ORDERS"), _fqtn(db, transform_schema, "FCT_PRODUCTION_ORDERS")),
        ]

        for name, raw_tbl, tr_tbl in pairs:
            raw_cnt = int(_fetch_one(cur, f"SELECT COUNT(*) FROM {raw_tbl}") or 0)
            tr_cnt = int(_fetch_one(cur, f"SELECT COUNT(*) FROM {tr_tbl}") or 0)
            status = "PASS" if raw_cnt == tr_cnt else "FAIL"

            results.append(
                _result(
                    check_name=f"rowcount_reconcile_{name}",
                    severity=SEVERITY_WARN,
                    status=status,
                    details={
                        "raw_table": raw_tbl,
                        "transform_table": tr_tbl,
                        "raw_count": raw_cnt,
                        "transform_count": tr_cnt,
                        "raw_schema": raw_schema,
                        "transform_schema": transform_schema,
                    },
                )
            )

        return results
    finally:
        cur.close()
        conn.close()


def dq_check_duplicate_batches(**context) -> List[Dict[str, Any]]:
    db = _get_required_env("SNOWFLAKE_DATABASE")
    transform_schema = _get_schema_env("MFG_SCHEMA_TRANSFORM", "CORE")

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        tbl = _fqtn(db, transform_schema, "CORE_BATCHES")
        dupes = int(
            _fetch_one(
                cur,
                f"""
                SELECT COUNT(*) FROM (
                  SELECT BATCH_ID
                  FROM {tbl}
                  GROUP BY BATCH_ID
                  HAVING COUNT(*) > 1
                )
                """,
            )
            or 0
        )

        status = "PASS" if dupes == 0 else "FAIL"
        return [
            _result(
                check_name="duplicate_batch_id_core_batches",
                severity=SEVERITY_ERROR,
                status=status,
                details={"table": tbl, "duplicate_keys": dupes, "transform_schema": transform_schema},
            )
        ]
    finally:
        cur.close()
        conn.close()


def dq_check_freshness_kpis(**context) -> List[Dict[str, Any]]:
    db = _get_required_env("SNOWFLAKE_DATABASE")
    marts_schema = _get_schema_env("MFG_SCHEMA_MARTS", "MARTS")

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        tbl = _fqtn(db, marts_schema, "MFG_PLANT_DAILY_KPIS")
        max_date = _fetch_one(cur, f"SELECT MAX(CALENDAR_DATE) FROM {tbl}")

        if max_date is None:
            return [
                _result(
                    check_name="freshness_mfg_plant_daily_kpis",
                    severity=SEVERITY_WARN,
                    status="FAIL",
                    details={"table": tbl, "max_calendar_date": None, "threshold_days": 2, "marts_schema": marts_schema},
                )
            ]

        today = pendulum.now("UTC").date()
        delta_days = int((today - max_date).days)
        status = "PASS" if delta_days <= 2 else "FAIL"

        return [
            _result(
                check_name="freshness_mfg_plant_daily_kpis",
                severity=SEVERITY_WARN,
                status=status,
                details={
                    "table": tbl,
                    "max_calendar_date": str(max_date),
                    "today": str(today),
                    "delta_days": delta_days,
                    "threshold_days": 2,
                    "marts_schema": marts_schema,
                },
            )
        ]
    finally:
        cur.close()
        conn.close()


def dq_log_results(**context) -> None:
    ti = context["ti"]
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    db = _get_required_env("SNOWFLAKE_DATABASE")

    all_results: List[Dict[str, Any]] = []
    for task_id in [
        "dq_check_raw_vs_transform_rowcounts",
        "dq_check_duplicate_batches",
        "dq_check_freshness_kpis",
    ]:
        payload = ti.xcom_pull(task_ids=task_id)
        if not payload:
            continue
        # payload is expected to be list[dict]
        all_results.extend(payload)

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        ensure_dq_audit_table(cur)

        failed_errors = 0
        for r in all_results:
            details = dict(r.get("details", {}))
            details.update({"dag_id": dag_id, "run_id": run_id})

            cur.execute(
                f"""
                INSERT INTO {db}.RAW.DATA_QUALITY_AUDIT
                (CHECK_NAME, SEVERITY, STATUS, DETAILS)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    r["check_name"],
                    r["severity"],
                    r["status"],
                    json.dumps(details, ensure_ascii=False),
                ),
            )

            if r["severity"] == SEVERITY_ERROR and r["status"] == "FAIL":
                failed_errors += 1

        if failed_errors:
            raise AirflowFailException(f"Data quality checks failed: {failed_errors} ERROR-level check(s) failed.")
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="mfg_data_quality_monitoring_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="30 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["mfg", "dq", "monitoring"],
) as dag:

    t_rowcounts = PythonOperator(
        task_id="dq_check_raw_vs_transform_rowcounts",
        python_callable=dq_check_raw_vs_transform_rowcounts,
    )

    t_dupes = PythonOperator(
        task_id="dq_check_duplicate_batches",
        python_callable=dq_check_duplicate_batches,
    )

    t_fresh = PythonOperator(
        task_id="dq_check_freshness_kpis",
        python_callable=dq_check_freshness_kpis,
    )

    t_log = PythonOperator(
        task_id="dq_log_results",
        python_callable=dq_log_results,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [t_rowcounts, t_dupes, t_fresh] >> t_log
