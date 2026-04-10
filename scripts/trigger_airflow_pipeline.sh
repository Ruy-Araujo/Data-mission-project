#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose up -d postgres minio airflow
docker compose build extractor_datamission_roterization dbt

docker exec airflow airflow dags trigger datamission_roterization_pipeline
