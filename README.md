
# Manufacturing Data Platform

**Snowflake · dbt · Airflow · Docker**

Production-inspired data platform simulating how a Digital Manufacturing team builds **reliable, testable, and observable pipelines** for analytics and AI workloads.

## What this project shows

* End-to-end orchestration with **Airflow (Docker, LocalExecutor, Postgres metadata)**
* Cloud data warehouse ingestion into **Snowflake**
* **dbt** transformations with schema tests and layered modeling
* Built-in **data quality monitoring and audit logging**
* Production-style engineering practices: idempotency, config isolation, separation of concerns

## Architecture (high level)

* **Ingestion (Python)**
  CSV → Snowflake RAW using `write_pandas`
  Idempotent loads (TRUNCATE + reload) with ingestion auditing
* **Transformations (dbt)**

  * STAGING: clean & standardize
  * CORE: integrated entities and facts
  * MARTS: KPIs and ML-ready datasets
    Schema tests: `unique`, `not_null`, `relationships`
* **Orchestration & Monitoring (Airflow)**
  DAGs for ingestion, transforms, and data quality checks
  Results logged to audit tables (DQ + ingestion)

## Snowflake data layers

* **RAW**: source + audit tables
* **STAGING**: cleaned views
* **CORE**: integrated models & facts
* **MARTS**: business KPIs and ML features

## Airflow DAGs

1. **Raw ingestion + full build** (daily)
2. **dbt-only transforms** (prod-style separation)
3. **Data quality monitoring** (row counts, duplicates, freshness)

## Production-readiness highlights

* Idempotent ingestion to prevent downstream test failures
* Environment-driven configuration (`SNOWFLAKE_*`, Docker `env_file`)
* dbt governance via automated testing
* Auditing tables for operational visibility
* Clean separation between ingestion, transformation, and monitoring

## What I learned / solved

* Airflow Docker setup aligned with production (Postgres metadata)
* Secure Airflow config (Fernet key)
* Reliable DAG discovery and volume mounting
* Fixing non-idempotent ingestion causing dbt test failures
* Portable execution with robust path resolution

## Run locally

1. Create `.env` with `SNOWFLAKE_*` credentials
2. Start Airflow via Docker Compose
3. Open Airflow UI: `http://localhost:8080`
4. Trigger ingestion, transform, or monitoring DAGs

