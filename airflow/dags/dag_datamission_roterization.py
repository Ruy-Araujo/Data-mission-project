from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="datamission_roterization_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ingestion", "datamission", "dbt"],
) as dag:
    ingest_to_staging_and_silver = DockerOperator(
        task_id="ingest_to_staging_and_silver",
        image="datamission_roterization:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="pipeline_net",
        mount_tmp_dir=False,
        environment={
            "DATAMISSION_PROJECT_ID": os.getenv("DATAMISSION_PROJECT_ID", ""),
            "DATAMISSION_API_TOKEN": os.getenv("DATAMISSION_API_TOKEN", ""),
            "DATAMISSION_API_BASE_URL": os.getenv("DATAMISSION_API_BASE_URL", "https://api.datamission.com.br"),
            "DATAMISSION_TIMEOUT_SECONDS": os.getenv("DATAMISSION_TIMEOUT_SECONDS", "30"),
            "DATAMISSION_MAX_RETRIES": os.getenv("DATAMISSION_MAX_RETRIES", "3"),
            "DATAMISSION_EXPECTED_COLUMNS": os.getenv("DATAMISSION_EXPECTED_COLUMNS", "source_record_id"),
            "DATAMISSION_REQUIRED_NON_NULL_COLUMNS": os.getenv(
                "DATAMISSION_REQUIRED_NON_NULL_COLUMNS", "source_record_id"
            ),
            "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "minio:9000"),
            "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", ""),
            "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", ""),
            "MINIO_RAW_BUCKET": os.getenv("MINIO_RAW_BUCKET", "raw"),
            "MINIO_SILVER_BUCKET": os.getenv("MINIO_SILVER_BUCKET", "silver"),
            "MINIO_SECURE": os.getenv("MINIO_SECURE", "false"),
            "MINIO_REGION": os.getenv("MINIO_REGION", "us-east-1"),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", ""),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
            "POSTGRES_STAGING_SCHEMA": os.getenv("POSTGRES_STAGING_SCHEMA", "staging"),
            "POSTGRES_STAGING_TABLE": os.getenv("POSTGRES_STAGING_TABLE", "datamission_records_raw"),
            "POSTGRES_INGESTION_HISTORY_TABLE": os.getenv(
                "POSTGRES_INGESTION_HISTORY_TABLE", "datamission_ingestion_history"
            ),
            "SOURCE_NAME": os.getenv("SOURCE_NAME", "datamission"),
            "OUTPUT_PREFIX": os.getenv("OUTPUT_PREFIX", "datamission_roterization"),
            "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
        },
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="datamission-dbt:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="pipeline_net",
        mount_tmp_dir=False,
        command="dbt run --project-dir /dbt --profiles-dir /dbt",
        environment={
            "DBT_POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "DBT_POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "DBT_POSTGRES_DB": os.getenv("POSTGRES_DB", ""),
            "DBT_POSTGRES_USER": os.getenv("POSTGRES_USER", ""),
            "DBT_POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
            "DBT_POSTGRES_SCHEMA": os.getenv("POSTGRES_SILVER_SCHEMA", "silver"),
            "POSTGRES_STAGING_SCHEMA": os.getenv("POSTGRES_STAGING_SCHEMA", "staging"),
        },
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="datamission-dbt:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="pipeline_net",
        mount_tmp_dir=False,
        command="dbt test --project-dir /dbt --profiles-dir /dbt",
        environment={
            "DBT_POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "DBT_POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "DBT_POSTGRES_DB": os.getenv("POSTGRES_DB", ""),
            "DBT_POSTGRES_USER": os.getenv("POSTGRES_USER", ""),
            "DBT_POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
            "DBT_POSTGRES_SCHEMA": os.getenv("POSTGRES_SILVER_SCHEMA", "silver"),
            "POSTGRES_STAGING_SCHEMA": os.getenv("POSTGRES_STAGING_SCHEMA", "staging"),
        },
    )

    ingest_to_staging_and_silver >> dbt_run >> dbt_test
