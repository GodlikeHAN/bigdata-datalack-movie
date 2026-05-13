from __future__ import annotations

import base64
import time

from src.config.settings import (
    AIRBYTE_API_URL,
    AIRBYTE_CLIENT_ID,
    AIRBYTE_CLIENT_SECRET,
    AIRBYTE_CONNECTION_ID,
    AIRBYTE_POLL_SECONDS,
    AIRBYTE_TIMEOUT_SECONDS,
)
from src.ingestion.api_client import ApiClient


client = ApiClient(timeout=60, max_retries=5, sleep_seconds=2.0)


def _auth_headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET:
        token = base64.b64encode(f"{AIRBYTE_CLIENT_ID}:{AIRBYTE_CLIENT_SECRET}".encode("utf-8")).decode("utf-8")
        headers["Authorization"] = f"Basic {token}"
    return headers


def trigger_airbyte_sync() -> dict:
    if not AIRBYTE_CONNECTION_ID:
        raise ValueError("AIRBYTE_CONNECTION_ID is not configured.")

    start_payload = client.session.post(
        f"{AIRBYTE_API_URL}/jobs",
        json={"connectionId": AIRBYTE_CONNECTION_ID, "jobType": "sync"},
        headers=_auth_headers(),
        timeout=60,
    )
    start_payload.raise_for_status()
    job = start_payload.json()
    job_id = job.get("jobId") or job.get("job", {}).get("id")

    if not job_id:
        raise RuntimeError(f"Unable to extract Airbyte job id from response: {job}")

    started_at = time.time()

    while time.time() - started_at < AIRBYTE_TIMEOUT_SECONDS:
        status_response = client.session.get(
            f"{AIRBYTE_API_URL}/jobs/{job_id}",
            headers=_auth_headers(),
            timeout=60,
        )
        status_response.raise_for_status()
        job_status = status_response.json()
        status = (
            job_status.get("status")
            or job_status.get("job", {}).get("status")
            or job_status.get("data", {}).get("status")
        )

        if status in {"succeeded", "completed"}:
            return {"job_id": job_id, "status": status}
        if status in {"failed", "cancelled", "incomplete"}:
            raise RuntimeError(f"Airbyte sync failed: {job_status}")

        time.sleep(AIRBYTE_POLL_SECONDS)

    raise TimeoutError(f"Airbyte sync job {job_id} did not finish in time.")
