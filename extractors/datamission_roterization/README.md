# datamission_roterization

Dedicated extractor to download a dataset from DataMission API and publish it to local MinIO.

## Flow

1. Calls the dataset endpoint using the `format` query parameter.
2. Validates response and applies configured retries.
3. Ensures the target bucket exists in MinIO.
4. Uploads payload using a date-partitioned object key.

Default format: `csv`.
Supported formats: `csv`, `json`, `parquet`.

## CLI parameters

- `--format`: `csv` (default), `json`, `parquet`
- `--project-id`: overrides `DATAMISSION_PROJECT_ID`
- `--object-key`: sets the full object key in MinIO
- `--output-prefix`: overrides `OUTPUT_PREFIX`

## Local execution

```bash
set -a && source .env && set +a
python3 -m extractors.datamission_roterization.main
```

Parquet example:

```bash
python3 -m extractors.datamission_roterization.main --format parquet
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

Run with override:

```bash
docker run --rm --network host --env-file .env datamission_roterization:latest --format json
```

## MinIO object key

When `--object-key` is not provided, the extractor generates:

`{OUTPUT_PREFIX}/YYYY/MM/DD/YYYYMMDDTHHMMSSZ_{OUTPUT_PREFIX}.<format>`

Example:

`datamission_roterization/2026/04/08/20260408T180501Z_datamission_roterization.csv`

## Documentation standard

New documentation for this extractor must always be written in English.
