
# Manufacturing Data Platform

**Snowflake · Airflow · dbt · Docker · AWS · Terraform · Kubernetes · GitHub Actions**

Production-inspired data platform demonstrating how a **data product evolves into a cloud-native, DevOps-operated system** with CI/CD, Infrastructure-as-Code, security, and observability.

The repository intentionally shows **design maturity over time**, from a simple local setup to a cloud-ready, production-grade platform.

---

## What this project demonstrates

* End-to-end **DevOps ownership**: build → test → deploy → observe
* **Infrastructure as Code** with Terraform (AWS)
* **CI/CD pipelines** using GitHub Actions (YAML)
* **Containerized workloads** and Kubernetes deployment patterns
* **Secure cloud authentication** using GitHub OIDC (no long-lived secrets)
* **Data platform engineering** with Airflow, dbt, and Snowflake
* Production principles: idempotency, traceability, auditing, and separation of concerns

---

## High-level architecture

* **Ingestion**: Python services (run-aware, auditable)
* **Orchestration**: Airflow
* **Transformation**: dbt (staging → core → marts)
* **Storage**:

  * Object storage: AWS S3
  * Warehouse: Snowflake
* **Runtime**:

  * Local: Docker / kind
  * Cloud-ready: Kubernetes
* **DevOps layer**:

  * GitHub Actions (CI/CD)
  * Terraform (AWS infra)
  * ECR (container registry)
  * IAM + OIDC (secure auth)
  * Monitoring & quality checks

---

## Repository evolution 

This repository is structured to show **how real platforms evolve**, not just a final snapshot.

### v1 — Local baseline (`main`)

* TRUNCATE + reload ingestion
* Dockerized Airflow
* Snowflake RAW → STAGING → CORE → MARTS
* dbt tests and audit tables
* Focus: correctness, simplicity, fast iteration

### v2 — Run-aware ingestion (`v2_run_aware`)

* Immutable RAW_RUN tables
* Deterministic `LOAD_ID` (`dag_id::run_id`)
* Safe retries and backfills
* Full lineage and traceability per run
* Consumer-friendly RAW views

### v3 — Cloud-native DevOps platform (`v3_cloud`)

* AWS infrastructure provisioned via **Terraform**
* S3 for raw/artifact storage
* ECR for container images
* GitHub Actions with **OIDC-based AWS access**
* CI pipelines with linting, testing, security scans
* Kubernetes deployment patterns (kind locally, EKS-ready)
* Foundations for monitoring, GitOps, SecOps, and FinOps

---

## DevOps & Cloud highlights (v3_cloud)

* **CI/CD**: GitHub Actions (lint, test, SAST, container scanning)
* **IaC**: Terraform modules for AWS (S3, ECR, IAM, OIDC)
* **Security**:

  * No AWS secrets in CI
  * Least-privilege IAM
  * Static and dependency scanning
* **Containers**: Docker images pushed to ECR
* **Kubernetes**: Declarative manifests, GitOps-style structure
* **Auditability**: Ingestion and data-quality logs persisted in Snowflake

---

## Run locally (v1 / v2)

1. Create `.env` with `SNOWFLAKE_*` variables (do not commit)
2. Start Airflow:

   ```bash
   docker compose up -d
   ```
3. Open Airflow UI: `http://localhost:8080`
4. Trigger:

   * `mfg_raw_ingestion_dag`
   * `mfg_dbt_transform_dag`
   * `mfg_data_quality_monitoring_dag`

