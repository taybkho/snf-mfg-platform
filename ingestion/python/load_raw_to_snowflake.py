"""
load_raw_to_snowflake.py (RUN-AWARE, production-grade)

Goal:
- Read source CSV files from: data/raw/source_system/
- Load into Snowflake as immutable, run-partitioned RAW_RUN tables (append-only by LOAD_ID)
- Maintain consumer-friendly RAW views that always point to the latest SUCCESSFUL run
- Write auditable load metadata into RAW.RAW_LOAD_AUDIT

Key production properties:
- No TRUNCATE of shared tables (safe for concurrent runs and backfills)
- Retry-safe: re-running the same Airflow run_id overwrites only that run's slice (DELETE WHERE LOAD_ID = ?)
- Deterministic LOAD_ID derived from Airflow context (or a safe local fallback)

Required env vars (Snowflake):
- SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
- SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE

Optional env vars (Airflow context):
- AIRFLOW_DAG_ID, AIRFLOW_RUN_ID, AIRFLOW_TASK_ID, AIRFLOW_LOGICAL_DATE (ISO8601)

How to run locally (dev only):
1) Ensure .env exists at project root with SNOWFLAKE_* vars
2) python ingestion/python/load_raw_to_snowflake.py
"""

from __future__ import annotations

import os
import sys
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple, Optional

import pandas as pd
from dotenv import load_dotenv

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


logger = logging.getLogger("raw_loader")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.handlers.clear()
logger.addHandler(_handler)


# -----------------------------
# Config + context
# -----------------------------

@dataclass(frozen=True)
class AirflowContext:
    dag_id: str
    run_id: str
    task_id: str
    logical_date: Optional[str]  # ISO string


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str


def load_env() -> None:
    """
    Load environment variables from project-root .env if present.
    In Docker, env vars should be injected via docker-compose env_file.
    """
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
        logger.info("Loaded environment from project-root .env")
    else:
        logger.info("No .env at project root; assuming env vars are injected (Docker/Airflow).")


def get_airflow_context() -> AirflowContext:
    """
    Fetch Airflow context passed via env vars. If not present, use stable local fallbacks.
    """
    dag_id = os.getenv("AIRFLOW_DAG_ID", "local_manual")
    run_id = os.getenv("AIRFLOW_RUN_ID", f"manual__{datetime.now(timezone.utc).isoformat()}")
    task_id = os.getenv("AIRFLOW_TASK_ID", "local_task")
    logical_date = os.getenv("AIRFLOW_LOGICAL_DATE")
    return AirflowContext(dag_id=dag_id, run_id=run_id, task_id=task_id, logical_date=logical_date)


def compute_load_id(ctx: AirflowContext) -> str:
    """
    Deterministic load identifier.
    - In Airflow: dag_id + run_id is globally unique and stable across retries.
    - Locally: run_id includes timestamp so it is unique per execution.
    """
    return f"{ctx.dag_id}::{ctx.run_id}"


def get_snowflake_config() -> SnowflakeConfig:
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    return SnowflakeConfig(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )


def get_connection(cfg: SnowflakeConfig) -> snowflake.connector.SnowflakeConnection:
    """
    Production notes:
    - Avoid logging secrets.
    - Use explicit database; schemas will be qualified in SQL.
    """
    logger.info("Connecting to Snowflake...")
    return snowflake.connector.connect(
        account=cfg.account,
        user=cfg.user,
        password=cfg.password,
        role=cfg.role,
        warehouse=cfg.warehouse,
        database=cfg.database,
        autocommit=False,  # we control commits
    )


# -----------------------------
# Paths + IO
# -----------------------------

def get_project_paths() -> Dict[str, Path]:
    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data" / "raw" / "source_system"
    return {
        "project_root": project_root,
        "data_dir": data_dir,
        "materials_csv": data_dir / "materials.csv",
        "batches_csv": data_dir / "batches.csv",
        "orders_csv": data_dir / "production_orders.csv",
    }


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    df = pd.read_csv(path)
    logger.info("Read %s rows from %s", f"{len(df):,}", path.name)
    return df


# -----------------------------
# DDL: schemas, tables, views, audits
# -----------------------------

RAW_SCHEMA = "RAW"
RAW_RUN_SCHEMA = "RAW_RUN"

RUN_TABLES = {
    "MATERIALS_RUN": """
        CREATE TABLE IF NOT EXISTS {db}.{raw_run}.MATERIALS_RUN (
            LOAD_ID STRING NOT NULL,
            INGESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            MATERIAL_ID VARCHAR,
            MATERIAL_NAME VARCHAR,
            MATERIAL_TYPE VARCHAR,
            UNIT_OF_MEASURE VARCHAR,
            STATUS VARCHAR,
            CREATED_AT DATE
        )
    """,
    "BATCHES_RUN": """
        CREATE TABLE IF NOT EXISTS {db}.{raw_run}.BATCHES_RUN (
            LOAD_ID STRING NOT NULL,
            INGESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR,
            MATERIAL_ID VARCHAR,
            MANUFACTURING_SITE VARCHAR,
            MANUFACTURE_DATE DATE,
            EXPIRY_DATE DATE,
            QUANTITY NUMBER,
            BATCH_STATUS VARCHAR
        )
    """,
    "PRODUCTION_ORDERS_RUN": """
        CREATE TABLE IF NOT EXISTS {db}.{raw_run}.PRODUCTION_ORDERS_RUN (
            LOAD_ID STRING NOT NULL,
            INGESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            ORDER_ID VARCHAR,
            BATCH_ID VARCHAR,
            ORDER_TYPE VARCHAR,
            PLANNED_START_DATE DATE,
            PLANNED_END_DATE DATE,
            ACTUAL_START_DATE DATE,
            ACTUAL_END_DATE DATE,
            ORDER_STATUS VARCHAR,
            LINE_ID VARCHAR
        )
    """,
}

AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS {db}.{raw}.RAW_LOAD_AUDIT (
    LOAD_ID STRING,
    LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    DAG_ID STRING,
    RUN_ID STRING,
    TASK_ID STRING,
    LOGICAL_DATE TIMESTAMP_LTZ,
    SOURCE_NAME STRING,
    TARGET_TABLE STRING,
    ROWS_READ NUMBER,
    ROWS_LOADED NUMBER,
    STATUS STRING,
    ERROR_MESSAGE STRING
)
"""

CURRENT_VIEWS = {
    "MATERIALS": """
        CREATE OR REPLACE VIEW {db}.{raw}.MATERIALS AS
        SELECT m.* EXCLUDE (LOAD_ID)
        FROM {db}.{raw_run}.MATERIALS_RUN m
        JOIN (
          SELECT LOAD_ID
          FROM {db}.{raw}.RAW_LOAD_AUDIT
          WHERE TARGET_TABLE = 'MATERIALS_RUN' AND STATUS = 'SUCCESS'
          QUALIFY ROW_NUMBER() OVER (ORDER BY LOAD_TIMESTAMP DESC) = 1
        ) latest
        ON m.LOAD_ID = latest.LOAD_ID
    """,
    "BATCHES": """
        CREATE OR REPLACE VIEW {db}.{raw}.BATCHES AS
        SELECT b.* EXCLUDE (LOAD_ID)
        FROM {db}.{raw_run}.BATCHES_RUN b
        JOIN (
          SELECT LOAD_ID
          FROM {db}.{raw}.RAW_LOAD_AUDIT
          WHERE TARGET_TABLE = 'BATCHES_RUN' AND STATUS = 'SUCCESS'
          QUALIFY ROW_NUMBER() OVER (ORDER BY LOAD_TIMESTAMP DESC) = 1
        ) latest
        ON b.LOAD_ID = latest.LOAD_ID
    """,
    "PRODUCTION_ORDERS": """
        CREATE OR REPLACE VIEW {db}.{raw}.PRODUCTION_ORDERS AS
        SELECT o.* EXCLUDE (LOAD_ID)
        FROM {db}.{raw_run}.PRODUCTION_ORDERS_RUN o
        JOIN (
          SELECT LOAD_ID
          FROM {db}.{raw}.RAW_LOAD_AUDIT
          WHERE TARGET_TABLE = 'PRODUCTION_ORDERS_RUN' AND STATUS = 'SUCCESS'
          QUALIFY ROW_NUMBER() OVER (ORDER BY LOAD_TIMESTAMP DESC) = 1
        ) latest
        ON o.LOAD_ID = latest.LOAD_ID
    """,
}


def ensure_schemas(cursor, database: str) -> None:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{RAW_SCHEMA}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{RAW_RUN_SCHEMA}")


def ensure_tables_and_views(cursor, database: str) -> None:
    # audit
    cursor.execute(AUDIT_DDL.format(db=database, raw=RAW_SCHEMA))
    # run tables
    for ddl in RUN_TABLES.values():
        cursor.execute(ddl.format(db=database, raw_run=RAW_RUN_SCHEMA))
    # current views
    for ddl in CURRENT_VIEWS.values():
        cursor.execute(ddl.format(db=database, raw=RAW_SCHEMA, raw_run=RAW_RUN_SCHEMA))


# -----------------------------
# Loading + auditing
# -----------------------------

def _to_utc_timestamp(logical_date: Optional[str]) -> Optional[str]:
    if not logical_date:
        return None
    return logical_date


def write_audit_row(
    cursor,
    database: str,
    load_id: str,
    ctx: AirflowContext,
    source_name: str,
    target_table: str,
    rows_read: int,
    rows_loaded: int,
    status: str,
    error_message: Optional[str],
) -> None:
    cursor.execute(
        f"""
        INSERT INTO {database}.{RAW_SCHEMA}.RAW_LOAD_AUDIT
        (LOAD_ID, DAG_ID, RUN_ID, TASK_ID, LOGICAL_DATE, SOURCE_NAME, TARGET_TABLE, ROWS_READ, ROWS_LOADED, STATUS, ERROR_MESSAGE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            load_id,
            ctx.dag_id,
            ctx.run_id,
            ctx.task_id,
            _to_utc_timestamp(ctx.logical_date),
            source_name,
            target_table,
            rows_read,
            rows_loaded,
            status,
            error_message,
        ),
    )


def load_run_table(
    conn: snowflake.connector.SnowflakeConnection,
    cursor,
    database: str,
    df: pd.DataFrame,
    run_table: str,
    source_name: str,
    load_id: str,
    ctx: AirflowContext,
) -> Tuple[bool, int]:
    """
    Retry-safe:
    - Delete the slice for this LOAD_ID
    - Insert new rows with LOAD_ID
    """
    rows_read = int(len(df))
    fq_table = f"{database}.{RAW_RUN_SCHEMA}.{run_table}"

    df2 = df.copy()
    df2.columns = [c.upper() for c in df2.columns]
    df2.insert(0, "LOAD_ID", load_id)

    try:
        logger.info("[%s] Deleting previous slice for load_id (retry-safe)...", run_table)
        cursor.execute(f"DELETE FROM {fq_table} WHERE LOAD_ID = %s", (load_id,))

        logger.info("[%s] Loading %s rows from %s ...", run_table, f"{rows_read:,}", source_name)
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df2,
            table_name=run_table,
            database=database,
            schema=RAW_RUN_SCHEMA,
            quote_identifiers=False,
        )
        nrows = int(nrows or 0)
        logger.info("[%s] write_pandas success=%s rows_loaded=%s chunks=%s", run_table, success, f"{nrows:,}", nchunks)

        write_audit_row(
            cursor=cursor,
            database=database,
            load_id=load_id,
            ctx=ctx,
            source_name=source_name,
            target_table=run_table,
            rows_read=rows_read,
            rows_loaded=nrows,
            status="SUCCESS" if success else "FAILED",
            error_message=None if success else "write_pandas returned success=false",
        )
        return bool(success), nrows

    except Exception as e:
        logger.exception("[%s] Load failed: %s", run_table, str(e))
        write_audit_row(
            cursor=cursor,
            database=database,
            load_id=load_id,
            ctx=ctx,
            source_name=source_name,
            target_table=run_table,
            rows_read=rows_read,
            rows_loaded=0,
            status="FAILED",
            error_message=str(e)[:8000],
        )
        return False, 0


def main() -> None:
    load_env()
    ctx = get_airflow_context()
    load_id = compute_load_id(ctx)

    cfg = get_snowflake_config()
    paths = get_project_paths()

    logger.info("Starting run-aware load. load_id=%s dag_id=%s run_id=%s task_id=%s", load_id, ctx.dag_id, ctx.run_id, ctx.task_id)
    logger.info("Using data directory: %s", paths["data_dir"])

    conn = get_connection(cfg)
    cursor = conn.cursor()

    try:
        ensure_schemas(cursor, cfg.database)
        ensure_tables_and_views(cursor, cfg.database)

        materials_df = read_csv(paths["materials_csv"])
        batches_df = read_csv(paths["batches_csv"])
        orders_df = read_csv(paths["orders_csv"])

        ok_m, _ = load_run_table(
            conn, cursor, cfg.database, materials_df, "MATERIALS_RUN", paths["materials_csv"].name, load_id, ctx
        )
        ok_b, _ = load_run_table(
            conn, cursor, cfg.database, batches_df, "BATCHES_RUN", paths["batches_csv"].name, load_id, ctx
        )
        ok_o, _ = load_run_table(
            conn, cursor, cfg.database, orders_df, "PRODUCTION_ORDERS_RUN", paths["orders_csv"].name, load_id, ctx
        )

        if not (ok_m and ok_b and ok_o):
            conn.rollback()
            raise RuntimeError("One or more table loads failed; rolled back transaction. Check RAW.RAW_LOAD_AUDIT for details.")

        ensure_tables_and_views(cursor, cfg.database)

        conn.commit()
        logger.info("Run-aware load committed successfully. load_id=%s", load_id)

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.error("Load failed: %s", str(e))
        raise
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
