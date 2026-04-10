# Data Mission Data Platform

![Data Mission ELT Architecture Cover](docs/images/cover-architecture.png)

End-to-end ELT pipeline for DataMission datasets using Python ingestion, Airflow orchestration, MinIO lake layers, and dbt transformations.

## Overview

This project executes the following flow:

1. Download dataset from DataMission API in CSV format.
1. Validate and transform data with pandas.
1. Load transformed rows into PostgreSQL staging.
1. Persist raw and silver artifacts in MinIO.
1. Run dbt models and tests.
1. Materialize gold analytics tables in PostgreSQL.

## Architecture

- Ingestion source: `https://api.datamission.com.br/projects/{project_id}/dataset?format=csv`
- Orchestration: Airflow 3
- Object storage:
  - Raw layer -> MinIO bucket `raw`
  - Silver layer -> MinIO bucket `silver`
- Warehouse:
  - Staging/Silver/Gold models in PostgreSQL (dbt)

## Repository Layout

- `extractors/datamission_roterization/`: ingestion pipeline code and Dockerfile
- `shared/`: reusable clients/config/logging/exceptions
- `airflow/dags/`: DAGs and orchestration entrypoints
- `dbt/`: dbt project, models, and tests
- `scripts/`: helper scripts for ingestion and full pipeline execution

## Prerequisites

- Docker and Docker Compose
- API token and project id for DataMission

## Configuration

Create local environment file:

```bash
cp .env.example .env
```

Set at minimum:

- `DATAMISSION_API_TOKEN`
- `DATAMISSION_PROJECT_ID`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`

Important optional variables:

- `DATAMISSION_EXPECTED_COLUMNS`
- `DATAMISSION_REQUIRED_NON_NULL_COLUMNS`
- `DATAMISSION_STRICT_VALIDATION`
- `MINIO_RAW_BUCKET` (default `raw`)
- `MINIO_SILVER_BUCKET` (default `silver`)

## Run

Start base services:

```bash
docker compose up -d postgres minio airflow
```

Build images:

```bash
docker compose build extractor_datamission_roterization dbt
```

Run ingestion only:

```bash
./scripts/run_ingestion.sh
```

Run ingestion + transformations + tests:

```bash
./scripts/run_pipeline.sh
```

Trigger pipeline through Airflow:

```bash
./scripts/trigger_airflow_pipeline.sh
```

## Airflow

- UI: `http://localhost:8080`
- DAG id: `datamission_roterization_pipeline`
- Task order:
  1. `ingest_to_staging_and_silver`
  1. `dbt_run`
  1. `dbt_test`

## Data Validation and Traceability

Ingestion performs:

- Basic schema checks for expected columns
- Essential non-null checks
- Run metadata enrichment (`ingestion_run_id`, `loaded_at`, `record_uid`)
- Ingestion history persistence:
  - PostgreSQL history table (`staging.datamission_ingestion_history`)
  - JSON history artifact in MinIO raw bucket

## dbt Output

- Staging model: `stg_datamission_records`
- Silver model: `silver_datamission_records`
- Gold model: `gold_datamission_metrics`
- Includes tests for keys and core metrics consistency.
