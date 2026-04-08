from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from extractors.base_extractor import BaseExtractor
from shared.clients.datamission_api import DataMissionApiClient
from shared.storage.minio_writer import MinioWriter


@dataclass(frozen=True)
class ExtractionRequest:
    project_id: str
    output_format: str
    output_prefix: str
    bucket: str
    object_key_override: str | None = None


_FORMAT_TO_CONTENT_TYPE = {
    "csv": "text/csv",
    "json": "application/json",
    "parquet": "application/octet-stream",
}


class DataMissionRoterizationExtractor(BaseExtractor):
    def __init__(self, api_client: DataMissionApiClient, storage: MinioWriter, request: ExtractionRequest) -> None:
        self.api_client = api_client
        self.storage = storage
        self.request = request

    def build_object_key(self) -> str:
        if self.request.object_key_override:
            return self.request.object_key_override

        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        return (
            f"{self.request.output_prefix}/{date_partition}/"
            f"{stamp}_{self.request.output_prefix}.{self.request.output_format}"
        )

    def run(self) -> str:
        payload = self.api_client.download_dataset(
            project_id=self.request.project_id,
            output_format=self.request.output_format,
        )

        object_key = self.build_object_key()
        content_type = _FORMAT_TO_CONTENT_TYPE[self.request.output_format]

        self.storage.ensure_bucket(self.request.bucket)
        self.storage.upload_bytes(
            bucket_name=self.request.bucket,
            object_key=object_key,
            payload=payload,
            content_type=content_type,
        )
        return object_key
