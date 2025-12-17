
# Manufacturing Data Platform

**Snowflake · dbt · Airflow · Docker**

Production-inspired manufacturing data platform demonstrating how **reliable, testable, and observable data pipelines** are built and evolved in real environments.

This repository intentionally shows **design iteration**, starting from a simple ingestion model and evolving toward a production-grade, run-aware architecture.

---

## What this project demonstrates

* End-to-end orchestration with **Airflow** (Docker, LocalExecutor, Postgres metadata)
* Cloud data ingestion into **Snowflake**
* Layered transformations and testing with **dbt**
* Data quality monitoring and audit logging
* Production engineering principles: idempotency, traceability, configuration isolation

---

## Architecture (high level)

Local, prod-like setup using **Docker + Airflow**, loading into **Snowflake**.

### Ingestion (Python)

* Reads source CSVs from `data/raw/source_system`
* Loads into Snowflake **RAW tables**
* Uses **TRUNCATE + reload** to ensure idempotent daily loads
* Ingestion history written to `RAW.RAW_LOAD_AUDIT`

### Transformations (dbt)

* **STAGING**: type casting, cleaning, naming standardization
* **CORE**: integrated entities and fact-like models
* **MARTS**: business KPIs and ML-ready datasets
* Schema tests: `unique`, `not_null`, `relationships`

### Orchestration & Monitoring (Airflow)

* Airflow runs in Docker with Postgres metadata DB
* Project mounted at `/opt/airflow`
* Environment variables injected via `.env` and Docker `env_file`
* Data quality checks logged to `RAW.DATA_QUALITY_AUDIT`

---

## Snowflake data layers

* **RAW**
  Source tables loaded from CSV + audit tables
* **STAGING**
  Cleaned, standardized views
* **CORE**
  Integrated entities and facts
* **MARTS**
  Business KPIs and ML-ready datasets

---

## Design evolution (intentional)

This project demonstrates how ingestion strategies evolve as operational needs increase.

### v1 — TRUNCATE + reload (**main branch**)

* Simple and predictable ingestion pattern
* Works well for small to medium data volumes
* Easy to test and reason about
* Used as the **baseline implementation**

### v2 — Run-aware ingestion (**`v2_run_aware` branch**)

* Immutable, run-partitioned RAW tables
* Deterministic `LOAD_ID` (`dag_id::run_id`)
* Safe retries and backfills without data loss
* Full lineage and traceability per Airflow run
* Consumer-friendly RAW views always point to the latest successful load

> The `v2_run_aware` branch represents the **production-grade evolution**
> after identifying the limitations of TRUNCATE-based ingestion.

---

## Airflow DAGs

* **Raw ingestion + full build**
* **dbt-only transformations**
* **Data quality monitoring** (row counts, duplicates, freshness)

---

## Run locally

1. Create `.env` at repo root with `SNOWFLAKE_*` variables (do not commit)
2. Start Airflow:

   ```bash
   docker compose up -d
   ```
3. Open Airflow UI: `http://localhost:8080`
4. Trigger:

   * `mfg_raw_ingestion_dag`
   * `mfg_dbt_transform_dag`
   * `mfg_data_quality_monitoring_dag`


