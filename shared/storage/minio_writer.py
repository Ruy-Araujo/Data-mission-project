from __future__ import annotations

import io
import logging

from minio import Minio
from minio.error import S3Error

from shared.core.exceptions import StorageError

LOGGER = logging.getLogger(__name__)


class MinioWriter:
    """Reusable MinIO writer for extracted data payloads."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        region: str | None = None,
    ) -> None:
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
        )

    def ensure_bucket(self, bucket_name: str) -> None:
        try:
            exists = self.client.bucket_exists(bucket_name)
            if not exists:
                self.client.make_bucket(bucket_name)
                LOGGER.info("Created bucket '%s'.", bucket_name)
        except S3Error as error:
            raise StorageError(f"Failed while ensuring bucket '{bucket_name}': {error}") from error

    def upload_bytes(
        self,
        bucket_name: str,
        object_key: str,
        payload: bytes,
        content_type: str,
    ) -> None:
        try:
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_key,
                data=io.BytesIO(payload),
                length=len(payload),
                content_type=content_type,
            )
        except S3Error as error:
            raise StorageError(
                f"Failed to upload object '{object_key}' to bucket '{bucket_name}': {error}"
            ) from error
