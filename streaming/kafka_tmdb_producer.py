from __future__ import annotations

import json
import time

from kafka import KafkaProducer

from src.config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TRENDING_TOPIC, TMDB_BASE_URL
from src.ingestion.api_client import ApiClient
from src.ingestion.extract_tmdb import _tmdb_params
from src.utils.date_utils import utc_now_iso
from src.utils.file_utils import ensure_dir, read_json, write_json
from src.config.settings import ARTIFACTS_ROOT


STATE_FILE = ARTIFACTS_ROOT / "realtime" / "last_trending_state.json"
client = ApiClient(timeout=30, max_retries=3, sleep_seconds=1.0)


def _load_state() -> dict[str, dict]:
    if STATE_FILE.exists():
        return read_json(STATE_FILE)
    return {}


def _save_state(state: dict[str, dict]) -> None:
    ensure_dir(STATE_FILE.parent)
    write_json(STATE_FILE, state)


def _build_events(previous_state: dict[str, dict], current_movies: list[dict], emit_heartbeat: bool) -> tuple[list[dict], dict[str, dict]]:
    new_state = {}
    events = []

    for rank, movie in enumerate(current_movies, start=1):
        tmdb_id = str(movie["id"])
        snapshot = {
            "rank": rank,
            "popularity": movie.get("popularity"),
            "vote_average": movie.get("vote_average"),
            "vote_count": movie.get("vote_count"),
        }
        new_state[tmdb_id] = snapshot

        previous_snapshot = previous_state.get(tmdb_id)
        if previous_snapshot != snapshot:
            events.append(
                {
                    "event_time_utc": utc_now_iso(),
                    "source": "tmdb",
                    "event_type": "trending_snapshot",
                    "tmdb_id": movie["id"],
                    "title": movie.get("title"),
                    "rank": rank,
                    "popularity": movie.get("popularity"),
                    "vote_average": movie.get("vote_average"),
                    "vote_count": movie.get("vote_count"),
                }
            )

    if not events and emit_heartbeat:
        events.append(
            {
                "event_time_utc": utc_now_iso(),
                "source": "tmdb",
                "event_type": "heartbeat",
                "tmdb_id": None,
                "title": "heartbeat",
                "rank": None,
                "popularity": None,
                "vote_average": None,
                "vote_count": None,
            }
        )

    return events, new_state


def produce_trending_events(iterations: int = 1, interval_seconds: int = 60, emit_heartbeat: bool = True) -> int:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    previous_state = _load_state()
    sent_count = 0

    for iteration in range(iterations):
        payload = client.get(f"{TMDB_BASE_URL}/trending/movie/day", params=_tmdb_params())
        current_movies = payload.get("results", [])
        events, previous_state = _build_events(previous_state, current_movies, emit_heartbeat=emit_heartbeat)

        for event in events:
            producer.send(KAFKA_TRENDING_TOPIC, event)
            sent_count += 1

        producer.flush()
        _save_state(previous_state)

        if iteration < iterations - 1:
            time.sleep(interval_seconds)

    producer.close()
    return sent_count


if __name__ == "__main__":
    produce_trending_events()
