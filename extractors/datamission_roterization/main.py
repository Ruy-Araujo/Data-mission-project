from __future__ import annotations

import argparse
import logging

from extractors.datamission_roterization.pipeline import DataMissionIngestionPipeline, build_ingestion_request
from shared.clients.datamission_api import DataMissionApiClient
from shared.config.settings import load_settings
from shared.core.exceptions import ConfigurationError, ExtractionError, ValidationError
from shared.core.logging import configure_logging
from shared.storage.minio_writer import MinioWriter

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ingestion pipeline: download csv, validate, stage in Postgres, publish silver to MinIO."
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help="Override project id from environment variable.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    datamission_cfg, minio_cfg, runtime_cfg, postgres_cfg = load_settings()
    configure_logging(runtime_cfg.log_level)

    project_id = args.project_id or datamission_cfg.project_id

    api_client = DataMissionApiClient(
        base_url=datamission_cfg.api_base_url,
        token=datamission_cfg.api_token,
        timeout_seconds=datamission_cfg.timeout_seconds,
        max_retries=datamission_cfg.max_retries,
    )

    minio_writer = MinioWriter(
        endpoint=minio_cfg.endpoint,
        access_key=minio_cfg.access_key,
        secret_key=minio_cfg.secret_key,
        secure=minio_cfg.secure,
        region=minio_cfg.region,
    )

    pipeline = DataMissionIngestionPipeline(
        api_client=api_client,
        storage=minio_writer,
        postgres=postgres_cfg,
        request=build_ingestion_request(
            runtime=runtime_cfg,
            project_id=project_id,
            raw_bucket=minio_cfg.raw_bucket,
            silver_bucket=minio_cfg.silver_bucket,
        ),
    )

    result = pipeline.run()
    LOGGER.info("Pipeline finished successfully: %s", result)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ConfigurationError, ExtractionError, ValidationError) as error:
        logging.getLogger(__name__).error("Extraction failed: %s", error)
        raise SystemExit(1) from error
    except KeyboardInterrupt as exc:
        logging.getLogger(__name__).error("Extraction interrupted by user.")
        raise SystemExit(130) from exc
