#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose up -d postgres minio

docker compose build extractor_datamission_roterization dbt

docker compose run --rm extractor_datamission_roterization

docker compose run --rm dbt run --project-dir /dbt --profiles-dir /dbt
docker compose run --rm dbt test --project-dir /dbt --profiles-dir /dbt
