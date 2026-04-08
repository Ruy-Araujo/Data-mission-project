from __future__ import annotations

import logging
import time

import requests

from shared.core.exceptions import ApiRequestError

LOGGER = logging.getLogger(__name__)


class DataMissionApiClient:
    """Reusable DataMission API client for dataset downloads."""

    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_seconds: int = 30,
        max_retries: int = 3,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "*/*",
                "User-Agent": "datamission-extractor/1.0",
            }
        )

    def download_dataset(self, project_id: str, output_format: str) -> bytes:
        url = f"{self.base_url}/projects/{project_id}/dataset"
        params = {"format": output_format}

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
                if response.status_code >= 400:
                    raise ApiRequestError(
                        f"DataMission API request failed with status {response.status_code}: {response.text[:500]}"
                    )
                if not response.content:
                    raise ApiRequestError("DataMission API returned empty content.")
                return response.content
            except (requests.RequestException, ApiRequestError) as error:
                last_error = error
                LOGGER.warning(
                    "Attempt %s/%s failed while fetching dataset: %s",
                    attempt,
                    self.max_retries,
                    error,
                )
                if attempt < self.max_retries:
                    time.sleep(min(2**attempt, 8))

        raise ApiRequestError(f"Could not download dataset after retries: {last_error}")
