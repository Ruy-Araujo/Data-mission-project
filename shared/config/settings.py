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
    bucket: str
    secure: bool
    region: str | None


@dataclass(frozen=True)
class RuntimeSettings:
    extractor_name: str
    output_prefix: str
    log_level: str


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(f"Missing required environment variable: {name}")
    return value


def _get_bool_env(name: str, default: str = "false") -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "y"}


def load_settings() -> tuple[DataMissionSettings, MinioSettings, RuntimeSettings]:
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
        bucket=os.getenv("MINIO_BUCKET", "raw"),
        secure=_get_bool_env("MINIO_SECURE", "false"),
        region=os.getenv("MINIO_REGION", "us-east-1") or None,
    )

    runtime = RuntimeSettings(
        extractor_name=os.getenv("EXTRACTOR_NAME", "datamission_roterization"),
        output_prefix=os.getenv("OUTPUT_PREFIX", "datamission_roterization"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )

    return datamission, minio, runtime


def ensure_format_is_supported(output_format: str) -> str:
    normalized = output_format.strip().lower()
    if normalized not in SUPPORTED_FORMATS:
        raise ConfigurationError(
            f"Unsupported format '{output_format}'. Expected one of: {sorted(SUPPORTED_FORMATS)}"
        )
    return normalized
