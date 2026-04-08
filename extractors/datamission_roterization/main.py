from __future__ import annotations

import argparse
import logging

from extractors.datamission_roterization.extractor import (
    DataMissionRoterizationExtractor,
    ExtractionRequest,
)
from shared.clients.datamission_api import DataMissionApiClient
from shared.config.settings import ensure_format_is_supported, load_settings
from shared.core.exceptions import ConfigurationError, ExtractionError
from shared.core.logging import configure_logging
from shared.storage.minio_writer import MinioWriter

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract DataMission dataset and upload to local MinIO."
    )
    parser.add_argument(
        "--format",
        default="csv",
        choices=["csv", "json", "parquet"],
        help="Output format to request from DataMission API.",
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help="Override project id from environment variable.",
    )
    parser.add_argument(
        "--object-key",
        default=None,
        help="Optional object key override in MinIO.",
    )
    parser.add_argument(
        "--output-prefix",
        default=None,
        help="Override object key prefix in MinIO.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    datamission_cfg, minio_cfg, runtime_cfg = load_settings()
    configure_logging(runtime_cfg.log_level)

    project_id = args.project_id or datamission_cfg.project_id
    output_format = ensure_format_is_supported(args.format)
    output_prefix = args.output_prefix or runtime_cfg.output_prefix

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

    extractor = DataMissionRoterizationExtractor(
        api_client=api_client,
        storage=minio_writer,
        request=ExtractionRequest(
            project_id=project_id,
            output_format=output_format,
            output_prefix=output_prefix,
            bucket=minio_cfg.bucket,
            object_key_override=args.object_key,
        ),
    )

    object_key = extractor.run()
    LOGGER.info("Extraction finished successfully. Uploaded object: %s", object_key)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ConfigurationError, ExtractionError) as error:
        logging.getLogger(__name__).error("Extraction failed: %s", error)
        raise SystemExit(1) from error
    except KeyboardInterrupt as exc:
        logging.getLogger(__name__).error("Extraction interrupted by user.")
        raise SystemExit(130) from exc
