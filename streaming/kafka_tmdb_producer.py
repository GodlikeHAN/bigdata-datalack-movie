from __future__ import annotations

import json
import time

from kafka import KafkaProducer

from src.config.settings import (
    ARTIFACTS_ROOT,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TRENDING_TOPIC,
    REALTIME_TRENDING_LIMIT,
    TMDB_BASE_URL,
)
from src.ingestion.api_client import ApiClient
from src.ingestion.extract_tmdb import _tmdb_params
from src.utils.date_utils import utc_now_iso
from src.utils.file_utils import ensure_dir, read_json, write_json


STATE_FILE = ARTIFACTS_ROOT / "realtime" / "last_trending_state.json"
TRACKED_FIELDS = ("rank", "popularity", "vote_average", "vote_count")
POSTER_BASE_URL = "https://image.tmdb.org/t/p/w500"
client = ApiClient(timeout=30, max_retries=3, sleep_seconds=1.0)


def _load_state() -> dict[str, dict]:
    if STATE_FILE.exists():
        return read_json(STATE_FILE)
    return {}


def _save_state(state: dict[str, dict]) -> None:
    ensure_dir(STATE_FILE.parent)
    write_json(STATE_FILE, state)


def _fetch_all_trending_movies() -> list[dict]:
    deduped_movies: list[dict] = []
    seen_tmdb_ids: set[int] = set()
    page = 1
    total_pages = 1

    while page <= total_pages and len(deduped_movies) < REALTIME_TRENDING_LIMIT:
        payload = client.get(f"{TMDB_BASE_URL}/trending/movie/day", params=_tmdb_params({"page": page}))
        total_pages = max(int(payload.get("total_pages") or 1), 1)

        for movie in payload.get("results", []):
            tmdb_id = movie.get("id")
            if tmdb_id is None or tmdb_id in seen_tmdb_ids:
                continue

            seen_tmdb_ids.add(tmdb_id)
            deduped_movies.append(movie)

            if len(deduped_movies) >= REALTIME_TRENDING_LIMIT:
                break

        page += 1

    return deduped_movies


def _snapshot_for_movie(movie: dict, rank: int) -> dict:
    poster_path = movie.get("poster_path")
    return {
        "tmdb_id": movie["id"],
        "title": movie.get("title"),
        "poster_path": poster_path,
        "poster_url": f"{POSTER_BASE_URL}{poster_path}" if poster_path else None,
        "rank": rank,
        "popularity": movie.get("popularity"),
        "vote_average": movie.get("vote_average"),
        "vote_count": movie.get("vote_count"),
    }


def _change_direction(previous_value, current_value) -> str | None:
    if previous_value is None or current_value is None or previous_value == current_value:
        return None
    return "up" if current_value > previous_value else "down"


def _build_state_record(previous_record: dict | None, snapshot: dict, event_time_utc: str) -> tuple[dict, list[str]]:
    state_record = {
        "title": snapshot.get("title"),
        "last_observed_utc": event_time_utc,
    }
    changed_fields: list[str] = []

    for field in TRACKED_FIELDS:
        current_value = snapshot.get(field)
        previous_value = previous_record.get(field) if previous_record else None
        changed = previous_record is None or previous_value != current_value
        direction = _change_direction(previous_value, current_value)
        if changed:
            changed_fields.append(field)

        state_record[field] = current_value
        if changed:
            state_record[f"{field}_previous_value"] = previous_value
            state_record[f"{field}_previous_changed_utc"] = (
                previous_record.get(f"{field}_last_changed_utc")
                if previous_record
                else None
            )
            state_record[f"{field}_last_changed_utc"] = event_time_utc
            state_record[f"{field}_last_change_direction"] = direction
        else:
            state_record[f"{field}_previous_value"] = (
                previous_record.get(f"{field}_previous_value")
                if previous_record
                else None
            )
            state_record[f"{field}_previous_changed_utc"] = (
                previous_record.get(f"{field}_previous_changed_utc")
                if previous_record
                else None
            )
            state_record[f"{field}_last_changed_utc"] = (
                previous_record.get(f"{field}_last_changed_utc")
                or previous_record.get("last_observed_utc")
                or event_time_utc
            )
            state_record[f"{field}_last_change_direction"] = (
                previous_record.get(f"{field}_last_change_direction")
                if previous_record
                else None
            )

    return state_record, changed_fields


def _build_snapshot_event(snapshot: dict, state_record: dict, event_time_utc: str, changed_fields: list[str]) -> dict:
    event = {
        "event_id": f"tmdb-{snapshot['tmdb_id']}-{event_time_utc}",
        "event_time_utc": event_time_utc,
        "source": "tmdb",
        "event_type": "trending_snapshot",
        "tmdb_id": snapshot["tmdb_id"],
        "title": snapshot.get("title"),
        "poster_path": snapshot.get("poster_path"),
        "poster_url": snapshot.get("poster_url"),
        "changed_fields": changed_fields,
        "has_change": bool(changed_fields),
    }

    for field in TRACKED_FIELDS:
        event[field] = snapshot.get(field)
        event[f"{field}_previous_value"] = state_record.get(f"{field}_previous_value")
        event[f"{field}_previous_changed_utc"] = state_record.get(f"{field}_previous_changed_utc")
        event[f"{field}_last_changed_utc"] = state_record.get(f"{field}_last_changed_utc")
        event[f"{field}_change_direction"] = state_record.get(f"{field}_last_change_direction")

    return event


def _build_events(
    previous_state: dict[str, dict],
    current_movies: list[dict],
    event_time_utc: str,
    emit_heartbeat: bool,
) -> tuple[list[dict], dict[str, dict]]:
    new_state = {}
    events = []

    for rank, movie in enumerate(current_movies, start=1):
        tmdb_id = str(movie["id"])
        previous_snapshot = previous_state.get(tmdb_id)
        snapshot = _snapshot_for_movie(movie, rank)
        state_record, changed_fields = _build_state_record(previous_snapshot, snapshot, event_time_utc)
        new_state[tmdb_id] = state_record
        events.append(_build_snapshot_event(snapshot, state_record, event_time_utc, changed_fields))

    if not current_movies and emit_heartbeat:
        events.append(
            {
                "event_id": f"heartbeat-{event_time_utc}",
                "event_time_utc": event_time_utc,
                "source": "tmdb",
                "event_type": "heartbeat",
                "tmdb_id": None,
                "title": "heartbeat",
                "poster_path": None,
                "poster_url": None,
                "rank": None,
                "popularity": None,
                "vote_average": None,
                "vote_count": None,
                "changed_fields": [],
                "has_change": False,
                "rank_previous_value": None,
                "rank_previous_changed_utc": None,
                "rank_last_changed_utc": None,
                "rank_change_direction": None,
                "popularity_previous_value": None,
                "popularity_previous_changed_utc": None,
                "popularity_last_changed_utc": None,
                "popularity_change_direction": None,
                "vote_average_previous_value": None,
                "vote_average_previous_changed_utc": None,
                "vote_average_last_changed_utc": None,
                "vote_average_change_direction": None,
                "vote_count_previous_value": None,
                "vote_count_previous_changed_utc": None,
                "vote_count_last_changed_utc": None,
                "vote_count_change_direction": None,
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
        current_movies = _fetch_all_trending_movies()
        event_time_utc = utc_now_iso()
        events, previous_state = _build_events(
            previous_state,
            current_movies,
            event_time_utc=event_time_utc,
            emit_heartbeat=emit_heartbeat,
        )

        for event in events:
            key = str(event["tmdb_id"]).encode("utf-8") if event.get("tmdb_id") is not None else b"heartbeat"
            producer.send(KAFKA_TRENDING_TOPIC, key=key, value=event)
            sent_count += 1

        producer.flush()
        _save_state(previous_state)

        if iteration < iterations - 1:
            time.sleep(interval_seconds)

    producer.close()
    return sent_count


if __name__ == "__main__":
    produce_trending_events()
