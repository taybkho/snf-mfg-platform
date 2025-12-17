"""
load_raw_to_snowflake.py

Goal:
- Read generated CSV files from data/raw/source_system
- Connect to Snowflake using credentials from environment (.env or container env)
- Idempotently load RAW tables (truncate + reload)
- Validate basic row counts
- Record a simple load audit log

How to run locally:
1. Activate your virtual environment: (.venv)
2. Ensure .env at project root has Snowflake credentials.
3. Run from project root:
   python ingestion/python/load_raw_to_snowflake.py
"""

import os
import sys
import uuid
import logging
from typing import Tuple, Dict

from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logger = logging.getLogger("raw_loader")
logger.setLevel(logging.INFO)

# Simple console handler
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(
    logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S")
)
logger.addHandler(_handler)


# ---------------------------------------------------------------------------
# Environment + connection helpers
# ---------------------------------------------------------------------------

def load_env() -> None:
    """
    Load environment variables from the .env file at the project root if present.
    In Docker, env vars can also come from docker-compose (env_file).
    """
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"

    if env_path.exists():
        load_dotenv(env_path, override=True)
        logger.info(f"Environment variables loaded from {env_path}")
    else:
        logger.info(".env not found, assuming env vars are already set (e.g. via Docker)")


def get_snowflake_connection() -> snowflake.connector.connection.SnowflakeConnection:
    """
    Create and return a Snowflake connection using environment variables.
    """

    required_envs = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]

    missing = [name for name in required_envs if not os.getenv(name)]
    if missing:
        logger.error("Missing required Snowflake environment variables:")
        for name in missing:
            logger.error(f"  - {name}")
        sys.exit(1)

    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    logger.info("Snowflake connection established.")
    return conn


# ---------------------------------------------------------------------------
# DDL helpers (table creation + truncation + audit table)
# ---------------------------------------------------------------------------

def create_raw_tables(cursor) -> None:
    """
    Ensure RAW tables exist with the expected schema.
    """
    logger.info("Ensuring RAW tables exist...")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS MATERIALS (
            MATERIAL_ID VARCHAR,
            MATERIAL_NAME VARCHAR,
            MATERIAL_TYPE VARCHAR,
            UNIT_OF_MEASURE VARCHAR,
            STATUS VARCHAR,
            CREATED_AT DATE
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS BATCHES (
            BATCH_ID VARCHAR,
            MATERIAL_ID VARCHAR,
            MANUFACTURING_SITE VARCHAR,
            MANUFACTURE_DATE DATE,
            EXPIRY_DATE DATE,
            QUANTITY NUMBER,
            BATCH_STATUS VARCHAR
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS PRODUCTION_ORDERS (
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
        """
    )

    logger.info("RAW tables are ready.")


def create_audit_table(cursor) -> None:
    """
    Create a simple RAW_LOAD_AUDIT table to track each load.
    """
    logger.info("Ensuring RAW_LOAD_AUDIT table exists...")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS RAW_LOAD_AUDIT (
            LOAD_ID STRING,
            LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            SOURCE_NAME STRING,
            TARGET_TABLE STRING,
            ROWS_READ NUMBER,
            ROWS_LOADED NUMBER,
            STATUS STRING,
            ERROR_MESSAGE STRING
        )
        """
    )
    logger.info("RAW_LOAD_AUDIT table is ready.")


def truncate_raw_tables(cursor) -> None:
    """
    Truncate RAW tables so the load is idempotent.
    """
    logger.info("Truncating RAW tables before load (idempotent reload)...")
    cursor.execute("TRUNCATE TABLE IF EXISTS MATERIALS")
    cursor.execute("TRUNCATE TABLE IF EXISTS BATCHES")
    cursor.execute("TRUNCATE TABLE IF EXISTS PRODUCTION_ORDERS")
    logger.info("RAW tables truncated.")


# ---------------------------------------------------------------------------
# File + DataFrame helpers
# ---------------------------------------------------------------------------

def get_project_paths() -> Dict[str, Path]:
    """
    Compute important project paths relative to this file.
    """
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
    """
    Read a CSV file with basic logging and validation.
    """
    if not path.exists():
        logger.error(f"CSV file not found: {path}")
        sys.exit(1)

    logger.info(f"Reading CSV: {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded {len(df):,} rows from {path.name}")
    if df.empty:
        logger.warning(f"CSV {path.name} is empty. This may or may not be expected.")
    return df


# ---------------------------------------------------------------------------
# Load + validation + audit
# ---------------------------------------------------------------------------

def load_dataframe_to_snowflake(
    conn: snowflake.connector.connection.SnowflakeConnection,
    cursor,
    df: pd.DataFrame,
    table_name: str,
    source_name: str,
    load_id: str,
) -> Tuple[bool, int]:
    """
    Load a DataFrame into a Snowflake table using write_pandas and
    write an audit record into RAW_LOAD_AUDIT.
    """
    rows_read = len(df)
    logger.info(f"[{table_name}] Preparing to load {rows_read:,} rows from {source_name}.")

    # Ensure column names are uppercase to match Snowflake convention
    df = df.copy()
    df.columns = [col.upper() for col in df.columns]

    try:
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name=table_name)
        logger.info(
            f"[{table_name}] write_pandas success={success}, rows_loaded={nrows:,}, chunks={nchunks}"
        )

        # Basic rowcount validation: what is now in the table?
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_in_table = cursor.fetchone()[0]
        logger.info(
            f"[{table_name}] Total rows in table after load: {total_in_table:,}"
        )

        status = "SUCCESS" if success else "FAILED"
        error_message = None

        # Insert audit record
        cursor.execute(
            """
            INSERT INTO RAW_LOAD_AUDIT
            (LOAD_ID, SOURCE_NAME, TARGET_TABLE, ROWS_READ, ROWS_LOADED, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                load_id,
                source_name,
                table_name,
                rows_read,
                nrows,
                status,
                error_message,
            ),
        )

        if not success:
            logger.error(f"[{table_name}] write_pandas reported failure.")
        return success, nrows

    except Exception as e:
        logger.exception(f"[{table_name}] Error while loading data: {e}")

        # Record failed audit
        cursor.execute(
            """
            INSERT INTO RAW_LOAD_AUDIT
            (LOAD_ID, SOURCE_NAME, TARGET_TABLE, ROWS_READ, ROWS_LOADED, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                load_id,
                source_name,
                table_name,
                rows_read,
                0,
                "FAILED",
                str(e),
            ),
        )
        return False, 0


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Main flow:
    - Load env vars
    - Connect to Snowflake
    - Ensure RAW + AUDIT tables exist
    - Truncate RAW (idempotent reload)
    - Read CSVs
    - Load into Snowflake
    - Validate + audit
    """
    load_env()
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Unique ID for this load run (shared across tables)
    load_id = str(uuid.uuid4())

    paths = get_project_paths()
    data_dir = paths["data_dir"]
    logger.info(f"Using data directory: {data_dir}")

    try:
        # Ensure tables exist
        create_raw_tables(cursor)
        create_audit_table(cursor)

        # Idempotent reload
        truncate_raw_tables(cursor)

        # Read source CSVs
        materials_df = read_csv(paths["materials_csv"])
        batches_df = read_csv(paths["batches_csv"])
        orders_df = read_csv(paths["orders_csv"])

        # Load each table + audit
        ok_materials, _ = load_dataframe_to_snowflake(
            conn,
            cursor,
            materials_df,
            table_name="MATERIALS",
            source_name=paths["materials_csv"].name,
            load_id=load_id,
        )

        ok_batches, _ = load_dataframe_to_snowflake(
            conn,
            cursor,
            batches_df,
            table_name="BATCHES",
            source_name=paths["batches_csv"].name,
            load_id=load_id,
        )

        ok_orders, _ = load_dataframe_to_snowflake(
            conn,
            cursor,
            orders_df,
            table_name="PRODUCTION_ORDERS",
            source_name=paths["orders_csv"].name,
            load_id=load_id,
        )

        if not (ok_materials and ok_batches and ok_orders):
            logger.error("One or more table loads failed. See logs/audit for details.")
            sys.exit(1)

        logger.info("All RAW tables loaded successfully and audited.")
        logger.info(f"Load ID: {load_id}")

    finally:
        logger.info("Closing Snowflake cursor and connection...")
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        logger.info("Snowflake connection closed.")


if __name__ == "__main__":
    main()
