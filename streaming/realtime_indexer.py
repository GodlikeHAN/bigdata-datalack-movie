from __future__ import annotations

import pandas as pd

from src.config.settings import REALTIME_TRENDING_INDEX
from src.indexing.bulk_indexer import bulk_index_dataframe


TRACKED_FIELDS = ("rank", "popularity", "vote_average", "vote_count")


def _event_id_for(event: dict) -> str:
    tmdb_id = event.get("tmdb_id")
    event_time_utc = event.get("event_time_utc")
    if tmdb_id is not None:
        return f"tmdb-{tmdb_id}-{event_time_utc}"
    return f"{event.get('event_type', 'event')}-{event_time_utc}"


def normalize_realtime_events(events: list[dict]) -> list[dict]:
    normalized_events = []

    for raw_event in events:
        event = dict(raw_event)
        event_time_utc = event.get("event_time_utc")
        event_type = event.get("event_type", "trending_snapshot")

        event["event_type"] = event_type
        event["event_id"] = event.get("event_id") or _event_id_for(event)

        changed_fields = event.get("changed_fields")
        if event_type == "trending_snapshot":
            if not isinstance(changed_fields, list):
                changed_fields = list(TRACKED_FIELDS)
            event["changed_fields"] = changed_fields
            event["has_change"] = bool(changed_fields)
        else:
            event["changed_fields"] = []
            event["has_change"] = False

        for field in TRACKED_FIELDS:
            key = f"{field}_last_changed_utc"
            previous_key = f"{field}_previous_value"
            previous_changed_key = f"{field}_previous_changed_utc"
            direction_key = f"{field}_change_direction"
            if key not in event:
                event[key] = event_time_utc
            if previous_key not in event:
                event[previous_key] = None
            if previous_changed_key not in event:
                event[previous_changed_key] = None
            if direction_key not in event:
                event[direction_key] = None

        normalized_events.append(event)

    return normalized_events


def index_realtime_events(events: list[dict]) -> int:
    if not events:
        return 0
    dataframe = pd.DataFrame(events)
    return bulk_index_dataframe(REALTIME_TRENDING_INDEX, dataframe, id_column="event_id")
