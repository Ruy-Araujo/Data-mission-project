from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from shared.core.exceptions import ConfigurationError


SUPPORTED_FORMATS = {"csv", "json", "parquet"}


@dataclass(frozen=True)
class DataMissionSettings:
    api_base_url: str
    api_token: str
    project_id: str
    timeout_seconds: int
    max_retries: int


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    raw_bucket: str
    silver_bucket: str
    secure: bool
    region: str | None


@dataclass(frozen=True)
class RuntimeSettings:
    extractor_name: str
    output_prefix: str
    source_name: str
    staging_table: str
    ingestion_history_table: str
    expected_columns: tuple[str, ...]
    required_non_null_columns: tuple[str, ...]
    strict_validation: bool
    log_level: str


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    staging_schema: str


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(f"Missing required environment variable: {name}")
    return value


def _get_bool_env(name: str, default: str = "false") -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "y"}


def _get_csv_env(name: str, default: str = "") -> tuple[str, ...]:
    raw = os.getenv(name, default)
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def load_settings() -> tuple[DataMissionSettings, MinioSettings, RuntimeSettings, PostgresSettings]:
    load_dotenv()

    datamission = DataMissionSettings(
        api_base_url=os.getenv("DATAMISSION_API_BASE_URL", "https://api.datamission.com.br").rstrip("/"),
        api_token=_get_required_env("DATAMISSION_API_TOKEN"),
        project_id=os.getenv("DATAMISSION_PROJECT_ID"),
        timeout_seconds=int(os.getenv("DATAMISSION_TIMEOUT_SECONDS", "30")),
        max_retries=int(os.getenv("DATAMISSION_MAX_RETRIES", "3")),
    )

    minio = MinioSettings(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=_get_required_env("MINIO_ACCESS_KEY"),
        secret_key=_get_required_env("MINIO_SECRET_KEY"),
        raw_bucket=os.getenv("MINIO_RAW_BUCKET", os.getenv("MINIO_BUCKET", "raw")),
        silver_bucket=os.getenv("MINIO_SILVER_BUCKET", "silver"),
        secure=_get_bool_env("MINIO_SECURE", "false"),
        region=os.getenv("MINIO_REGION", "us-east-1") or None,
    )

    runtime = RuntimeSettings(
        extractor_name=os.getenv("EXTRACTOR_NAME", "datamission_roterization"),
        output_prefix=os.getenv("OUTPUT_PREFIX", "datamission_roterization"),
        source_name=os.getenv("SOURCE_NAME", "datamission"),
        staging_table=os.getenv("POSTGRES_STAGING_TABLE", "datamission_records_raw"),
        ingestion_history_table=os.getenv("POSTGRES_INGESTION_HISTORY_TABLE", "datamission_ingestion_history"),
        expected_columns=_get_csv_env("DATAMISSION_EXPECTED_COLUMNS", "source_record_id"),
        required_non_null_columns=_get_csv_env("DATAMISSION_REQUIRED_NON_NULL_COLUMNS", "source_record_id"),
        strict_validation=_get_bool_env("DATAMISSION_STRICT_VALIDATION", "false"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )

    postgres = PostgresSettings(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=_get_required_env("POSTGRES_DB"),
        user=_get_required_env("POSTGRES_USER"),
        password=_get_required_env("POSTGRES_PASSWORD"),
        staging_schema=os.getenv("POSTGRES_STAGING_SCHEMA", "staging"),
    )

    return datamission, minio, runtime, postgres


def ensure_format_is_supported(output_format: str) -> str:
    normalized = output_format.strip().lower()
    if normalized not in SUPPORTED_FORMATS:
        raise ConfigurationError(
            f"Unsupported format '{output_format}'. Expected one of: {sorted(SUPPORTED_FORMATS)}"
        )
    return normalized
