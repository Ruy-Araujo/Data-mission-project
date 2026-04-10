# datamission_roterization

Dedicated ingestion pipeline that downloads a DataMission CSV dataset, validates and transforms it with pandas, loads staging in PostgreSQL, and publishes silver data to MinIO.

## Flow

1. Calls `GET /projects/{project_id}/dataset?format=csv`.
2. Stores the raw CSV artifact per execution in MinIO raw bucket.
3. Validates expected columns and essential non-null columns.
4. Applies pandas transformations and metadata enrichment.
5. Loads transformed rows into PostgreSQL staging.
6. Writes silver parquet artifact to MinIO silver bucket.
7. Registers ingestion history in PostgreSQL and MinIO history object.

## CLI parameters

- `--project-id`: overrides `DATAMISSION_PROJECT_ID`

## Local execution

```bash
set -a && source .env && set +a
python3 -m extractors.datamission_roterization.main
```

Parquet example:

```bash
python3 -m extractors.datamission_roterization.main
```

## Docker

Build:

```bash
docker build \
  -f extractors/datamission_roterization/Dockerfile \
  -t datamission_roterization:latest \
  .
```

Run (csv default):

```bash
docker run --rm --network host --env-file .env datamission_roterization:latest
```

## Storage targets

- raw layer: `MINIO_RAW_BUCKET` (default `raw`), object key partition: `{OUTPUT_PREFIX}/raw/YYYY/MM/DD/{run_id}.csv`
- silver layer: `MINIO_SILVER_BUCKET` (default `silver`), object key partition: `{OUTPUT_PREFIX}/silver/YYYY/MM/DD/{run_id}.parquet`
- ingestion history: JSON artifact in raw bucket + table in PostgreSQL staging schema

## Documentation standard

New documentation for this extractor must always be written in English.
