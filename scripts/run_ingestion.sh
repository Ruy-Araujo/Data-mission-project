#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose build extractor_datamission_roterization
docker compose run --rm extractor_datamission_roterization
