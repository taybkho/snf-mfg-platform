# Manufacturing Data Platform (Snowflake + dbt + Airflow) — Portfolio Project

Production-inspired **manufacturing data platform** simulating how a Digital Manufacturing & Supply team builds reliable pipelines for analytics and AI-ready data.

## Architecture (High level)

**Local “prod-like” environment** using Docker + Airflow, loading into **Snowflake**:

1. **Ingestion (Python) — RUN-AWARE**
   - Reads source CSVs from `data/raw/source_system`
   - Loads into Snowflake **RAW_RUN** tables partitioned by a deterministic `LOAD_ID` (Airflow `dag_id::run_id`)
   - Maintains consumer-friendly **RAW views** that always point to the **latest successful** ingest
   - Writes ingestion audit records into `RAW.RAW_LOAD_AUDIT`

2. **Transformations (dbt)**
   - **STAGING**: `stg_*` models (type casting, naming standardization)
   - **CORE**: `core_*` and fact-like models (entity integration + derived metrics)
   - **MARTS**: KPI and ML-ready tables (business/AI consumption layer)
   - dbt schema tests: `unique`, `not_null`, `relationships`

3. **Orchestration & Monitoring (Airflow)**
   - Airflow runs in Docker (LocalExecutor + Postgres metadata DB)
   - Project is mounted inside the container at `/opt/airflow`
   - `.env` is injected via Docker `env_file` to supply `SNOWFLAKE_*` credentials
   - Data quality DAG logs results into `RAW.DATA_QUALITY_AUDIT`

---

## Snowflake Layering

- **RAW_RUN** (immutable, run-partitioned)
  - `MATERIALS_RUN`, `BATCHES_RUN`, `PRODUCTION_ORDERS_RUN`
  - Each row tagged with `LOAD_ID` to support backfills, traceability, and reruns.

- **RAW** (consumer-friendly “current” views)
  - `MATERIALS`, `BATCHES`, `PRODUCTION_ORDERS` (views → latest successful `LOAD_ID`)
  - Audit tables: `RAW_LOAD_AUDIT`, `DATA_QUALITY_AUDIT`

- **STAGING / CORE / MARTS**
  - Built by dbt from RAW views.

---

## Why RUN-AWARE ingestion (no TRUNCATE)

- Safe for retries and reruns (overwrites only the run slice)
- Backfills are possible without destroying “current” data
- Full lineage / traceability: every record can be traced to a specific Airflow run

---

## How to run (local)

1. Create `.env` at repo root (do not commit). Use the provided template.
2. Start Airflow via Docker Compose (from `airflow/` folder):
   - `docker compose up -d`
3. Open Airflow UI: `http://localhost:8080`
4. Trigger:
   - `mfg_raw_ingestion_dag` (end-to-end demo)
   - `mfg_dbt_transform_dag` (transform-only)
   - `mfg_data_quality_monitoring_dag` (monitoring)
