# DAG Structure

- `common/`: shared DAG helpers and environment utilities.
- `pipelines/`: ingestion and transformation orchestration DAGs.

The `docker-compose.yml` file mounts this folder into `/opt/airflow/dags` in the Airflow container.

## Conventions

- Business logic for extraction lives in `extractors/` and `shared/` at repository root.
- DAGs should orchestrate containers and pipeline steps, not duplicate extraction logic.
- Every new source should have a dedicated extractor folder with its own Dockerfile and README.

## Current pipeline DAG

- DAG id: `datamission_roterization_pipeline`
- Tasks:
	1. `ingest_to_staging_and_silver`: run Python ingestion container.
	2. `dbt_run`: materialize dbt models.
	3. `dbt_test`: validate keys and central metrics.

