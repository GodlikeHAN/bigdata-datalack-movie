import time
from typing import Any, Dict, Optional

import requests


class ApiClient:
    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 3,
        sleep_seconds: float = 0.5,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.sleep_seconds = sleep_seconds
        self.session = requests.Session()

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                )

                if response.status_code == 429:
                    time.sleep(self.sleep_seconds * attempt * 3)
                    continue

                response.raise_for_status()
                return response.json()

            except Exception as exc:
                last_exception = exc
                time.sleep(self.sleep_seconds * attempt)

        raise RuntimeError(
            f"API request failed after {self.max_retries} attempts: {url}"
        ) from last_exception
