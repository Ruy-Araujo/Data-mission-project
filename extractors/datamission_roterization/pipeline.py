from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256

import pandas as pd
from sqlalchemy import create_engine, text

from shared.clients.datamission_api import DataMissionApiClient
from shared.config.settings import PostgresSettings, RuntimeSettings
from shared.core.exceptions import ValidationError
from shared.storage.minio_writer import MinioWriter

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionRequest:
    project_id: str
    source_name: str
    output_prefix: str
    raw_bucket: str
    silver_bucket: str
    staging_table: str
    ingestion_history_table: str
    expected_columns: tuple[str, ...]
    required_non_null_columns: tuple[str, ...]
    strict_validation: bool


class DataMissionIngestionPipeline:
    def __init__(
        self,
        api_client: DataMissionApiClient,
        storage: MinioWriter,
        postgres: PostgresSettings,
        request: IngestionRequest,
    ) -> None:
        self.api_client = api_client
        self.storage = storage
        self.postgres = postgres
        self.request = request

    def _build_db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres.user}:{self.postgres.password}"
            f"@{self.postgres.host}:{self.postgres.port}/{self.postgres.database}"
        )

    def _run_context(self) -> tuple[str, datetime]:
        now = datetime.now(timezone.utc)
        run_id = now.strftime("%Y%m%dT%H%M%S%fZ")
        return run_id, now

    def _build_object_key(self, layer: str, run_id: str, extension: str, now: datetime) -> str:
        partition = now.strftime("%Y/%m/%d")
        return f"{self.request.output_prefix}/{layer}/{partition}/{run_id}.{extension}"

    def _validate_dataframe(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            raise ValidationError("Dataset is empty.")

        missing_columns = [c for c in self.request.expected_columns if c not in frame.columns]
        if missing_columns:
            if self.request.strict_validation:
                raise ValidationError(f"Missing expected columns: {missing_columns}")
            LOGGER.warning("Expected columns not found (non-strict mode): %s", missing_columns)

        null_failures: dict[str, int] = {}
        for column in self.request.required_non_null_columns:
            if column not in frame.columns:
                if self.request.strict_validation:
                    raise ValidationError(f"Required non-null column '{column}' is missing from dataset")
                LOGGER.warning("Required non-null column missing (non-strict mode): %s", column)
                continue
            null_count = int(frame[column].isna().sum())
            if null_count > 0:
                null_failures[column] = null_count

        if null_failures:
            if self.request.strict_validation:
                raise ValidationError(f"Non-null validation failed: {null_failures}")
            LOGGER.warning("Non-null validation failures (non-strict mode): %s", null_failures)

    def _transform_dataframe(self, frame: pd.DataFrame, run_id: str, loaded_at: datetime) -> pd.DataFrame:
        transformed = frame.copy()

        if "source_record_id" not in transformed.columns:
            transformed["source_record_id"] = transformed.astype(str).agg("|".join, axis=1).apply(
                lambda value: sha256(value.encode("utf-8")).hexdigest()
            )

        transformed["source_name"] = self.request.source_name
        transformed["project_id"] = self.request.project_id
        transformed["loaded_at"] = loaded_at.isoformat()
        transformed["ingestion_run_id"] = run_id

        uid_basis = transformed["source_record_id"].astype(str)

        transformed["record_uid"] = uid_basis.apply(
            lambda value: sha256(f"{self.request.project_id}|{run_id}|{value}".encode("utf-8")).hexdigest()
        )

        return transformed

    def _write_staging(self, frame: pd.DataFrame) -> None:
        def quote_ident(identifier: str) -> str:
            return '"' + identifier.replace('"', '""') + '"'

        def sql_type_for_dtype(series: pd.Series) -> str:
            if pd.api.types.is_integer_dtype(series):
                return "BIGINT"
            if pd.api.types.is_float_dtype(series):
                return "DOUBLE PRECISION"
            if pd.api.types.is_bool_dtype(series):
                return "BOOLEAN"
            if pd.api.types.is_datetime64_any_dtype(series):
                return "TIMESTAMPTZ"
            return "TEXT"

        engine = create_engine(self._build_db_url())
        try:
            with engine.begin() as connection:
                schema_name = self.postgres.staging_schema
                table_name = self.request.staging_table

                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema_name)}"))

                existing_columns_rows = connection.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = :table_schema
                          AND table_name = :table_name
                        """
                    ),
                    {"table_schema": schema_name, "table_name": table_name},
                ).fetchall()

                existing_columns = {row[0] for row in existing_columns_rows}

                if existing_columns:
                    for column in frame.columns:
                        if column in existing_columns:
                            continue
                        column_type = sql_type_for_dtype(frame[column])
                        connection.execute(
                            text(
                                f"ALTER TABLE {quote_ident(schema_name)}.{quote_ident(table_name)} "
                                f"ADD COLUMN {quote_ident(column)} {column_type}"
                            )
                        )

            frame.to_sql(
                name=table_name,
                con=engine,
                schema=schema_name,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
        finally:
            engine.dispose()

    def _write_history(
        self,
        run_id: str,
        loaded_at: datetime,
        raw_object_key: str,
        silver_object_key: str,
        row_count: int,
    ) -> None:
        payload = {
            "run_id": run_id,
            "project_id": self.request.project_id,
            "source_name": self.request.source_name,
            "loaded_at": loaded_at.isoformat(),
            "rows_loaded": row_count,
            "raw_object_key": raw_object_key,
            "silver_object_key": silver_object_key,
            "staging_table": f"{self.postgres.staging_schema}.{self.request.staging_table}",
            "status": "success",
        }

        history_key = self._build_object_key("history", run_id, "json", loaded_at)
        self.storage.ensure_bucket(self.request.raw_bucket)
        self.storage.upload_bytes(
            bucket_name=self.request.raw_bucket,
            object_key=history_key,
            payload=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
        )

        engine = create_engine(self._build_db_url())
        try:
            with engine.begin() as connection:
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.postgres.staging_schema}"))
                connection.execute(
                    text(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self.postgres.staging_schema}.{self.request.ingestion_history_table} (
                            run_id TEXT PRIMARY KEY,
                            project_id TEXT NOT NULL,
                            source_name TEXT NOT NULL,
                            loaded_at TIMESTAMPTZ NOT NULL,
                            rows_loaded INTEGER NOT NULL,
                            raw_object_key TEXT NOT NULL,
                            silver_object_key TEXT NOT NULL,
                            status TEXT NOT NULL,
                            details JSONB NOT NULL
                        )
                        """
                    )
                )
                connection.execute(
                    text(
                        f"""
                        INSERT INTO {self.postgres.staging_schema}.{self.request.ingestion_history_table}
                        (run_id, project_id, source_name, loaded_at, rows_loaded, raw_object_key, silver_object_key, status, details)
                        VALUES
                        (:run_id, :project_id, :source_name, :loaded_at, :rows_loaded, :raw_object_key, :silver_object_key, :status, CAST(:details AS jsonb))
                        ON CONFLICT (run_id) DO NOTHING
                        """
                    ),
                    {
                        "run_id": run_id,
                        "project_id": self.request.project_id,
                        "source_name": self.request.source_name,
                        "loaded_at": loaded_at.isoformat(),
                        "rows_loaded": row_count,
                        "raw_object_key": raw_object_key,
                        "silver_object_key": silver_object_key,
                        "status": "success",
                        "details": json.dumps(payload),
                    },
                )
        finally:
            engine.dispose()

    def run(self) -> dict[str, object]:
        run_id, loaded_at = self._run_context()

        raw_payload = self.api_client.download_dataset(
            project_id=self.request.project_id,
            output_format="csv",
        )

        raw_object_key = self._build_object_key("raw", run_id, "csv", loaded_at)
        self.storage.ensure_bucket(self.request.raw_bucket)
        self.storage.upload_bytes(
            bucket_name=self.request.raw_bucket,
            object_key=raw_object_key,
            payload=raw_payload,
            content_type="text/csv",
        )

        frame = pd.read_csv(io.BytesIO(raw_payload))
        self._validate_dataframe(frame)
        transformed = self._transform_dataframe(frame, run_id=run_id, loaded_at=loaded_at)

        self._write_staging(transformed)

        silver_buffer = io.BytesIO()
        transformed.to_parquet(silver_buffer, index=False)
        silver_payload = silver_buffer.getvalue()

        silver_object_key = self._build_object_key("silver", run_id, "parquet", loaded_at)
        self.storage.ensure_bucket(self.request.silver_bucket)
        self.storage.upload_bytes(
            bucket_name=self.request.silver_bucket,
            object_key=silver_object_key,
            payload=silver_payload,
            content_type="application/octet-stream",
        )

        row_count = int(len(transformed))
        self._write_history(
            run_id=run_id,
            loaded_at=loaded_at,
            raw_object_key=raw_object_key,
            silver_object_key=silver_object_key,
            row_count=row_count,
        )

        summary = {
            "run_id": run_id,
            "rows_loaded": row_count,
            "raw_object_key": raw_object_key,
            "silver_object_key": silver_object_key,
            "staging_table": f"{self.postgres.staging_schema}.{self.request.staging_table}",
        }
        LOGGER.info("Ingestion summary: %s", summary)
        return summary


def build_ingestion_request(runtime: RuntimeSettings, project_id: str, raw_bucket: str, silver_bucket: str) -> IngestionRequest:
    return IngestionRequest(
        project_id=project_id,
        source_name=runtime.source_name,
        output_prefix=runtime.output_prefix,
        raw_bucket=raw_bucket,
        silver_bucket=silver_bucket,
        staging_table=runtime.staging_table,
        ingestion_history_table=runtime.ingestion_history_table,
        expected_columns=runtime.expected_columns,
        required_non_null_columns=runtime.required_non_null_columns,
        strict_validation=runtime.strict_validation,
    )
