## Plan: DataMission Extractor for MinIO

Create a project foundation for folder-isolated extractors, with reusable components for the API client and local MinIO upload. The first extractor will be `datamission_roterization`, with `csv` as the default format and configurable support for `json` and `parquet`. Execution will be done with one `docker run` call per extraction, and the token will be provided only through environment variables.

**Steps**
1. Phase 1 - Base repository structure
1. Define a root structure for multiple extractors, separating shared modules from independent extractor folders.
1. Create naming conventions for scripts, Docker image, and per-extractor documentation to allow growth without tight coupling.
1. Phase 2 - Reusable modules (core)
1. Define the API client class contract with: Bearer authentication via env var, format query parameter, timeout/retry, and a single download method.
1. Define the MinIO persistence class contract with: connection via env vars, optional bucket check/create, stream/file upload, and object key organization.
1. Define shared utilities: configuration loading, structured logging, and exception handling with operational messages.
1. Phase 3 - First isolated extractor: datamission_roterization
1. Create the extractor entrypoint to receive parameters (format, dataset/project id, object name, optional timestamp), defaulting format to `csv`.
1. Implement flow orchestration: fetch from API -> validate content -> publish to MinIO bucket `raw`.
1. Generate predictable file/object naming for traceability (extractor prefix + date/time + extension by format).
1. Phase 4 - Per-extraction containerization
1. Create an extractor-specific Dockerfile with minimal dependencies and entrypoint execution command.
1. Define the execution interface via `docker run` with required env vars and optional arguments, keeping the container stateless.
1. Ensure each future extractor has its own Dockerfile and local documentation, without requiring docker compose.
1. Phase 5 - Documentation and operations
1. Create a root README with a multi-extractor architecture overview and prerequisites.
1. Create extractor-specific docs with local and Docker examples, required/optional variables, and csv/json/parquet examples.
1. Add an example environment file for secure execution without token hardcoding.
1. Phase 6 - Verification
1. Validate local extractor execution with default `csv` format and confirm object creation in local MinIO.
1. Validate overrides for `json` and `parquet`.
1. Validate execution via `docker run` and consistency of output object name/format.
1. Validate logs and exit codes for authentication failures, timeouts, and MinIO unavailability.

**Relevant files**
- /home/ruy/Documents/Data_mission/README.md - multi-extractor architecture overview and usage instructions.
- /home/ruy/Documents/Data_mission/.env.example - configuration variables without secrets.
- /home/ruy/Documents/Data_mission/shared/clients/datamission_api.py - reusable API class.
- /home/ruy/Documents/Data_mission/shared/storage/minio_writer.py - reusable MinIO writer class.
- /home/ruy/Documents/Data_mission/shared/config/settings.py - configuration loading and validation.
- /home/ruy/Documents/Data_mission/shared/core/exceptions.py - standardized exceptions.
- /home/ruy/Documents/Data_mission/shared/core/logging.py - structured logger.
- /home/ruy/Documents/Data_mission/extractors/datamission_roterization/main.py - extractor entrypoint.
- /home/ruy/Documents/Data_mission/extractors/datamission_roterization/extractor.py - extraction flow for this dataset.
- /home/ruy/Documents/Data_mission/extractors/datamission_roterization/Dockerfile - dedicated extractor image.
- /home/ruy/Documents/Data_mission/extractors/datamission_roterization/README.md - extractor-specific documentation.

**Verification**
1. Run locally with environment variables and without format parameter to confirm `csv` default.
1. Run locally with `format=json` and `format=parquet` to confirm override behavior.
1. Run container via `docker run` with env vars and validate upload to bucket `raw`.
1. Verify in local MinIO that objects were written with correct extension and non-zero size.
1. Force invalid-token and MinIO-endpoint-down scenarios to confirm error handling and logging.

**Decisions**
- First extractor name: `datamission_roterization`.
- Default format: `csv`, with override for `json` and `parquet`.
- Default MinIO destination: local endpoint `localhost:9000`, bucket `raw`.
- Per-extraction container trigger via `docker run`.
- API token only through environment variable (no hardcoded token).

**Further Considerations**
1. MinIO object naming policy: simple (timestamp) vs. date partitioned (`yyyy/mm/dd`). Recommendation: date partitioned for easier future governance.
2. Idempotency strategy: overwrite same name vs. always version with unique timestamp. Recommendation: unique timestamp to avoid accidental loss.
3. Authentication evolution: keep only static Bearer vs. prepare interface for secret rotation/external secret stores. Recommendation: keep the interface rotation-ready without changing extractor code.
