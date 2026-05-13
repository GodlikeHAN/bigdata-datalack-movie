from __future__ import annotations

import json
from pathlib import Path

from kafka import KafkaConsumer

from src.config.settings import DATA_ROOT, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TRENDING_TOPIC
from src.utils.date_utils import current_date_yyyymmdd, utc_now_iso
from src.utils.file_utils import ensure_dir, write_json_lines
from streaming.realtime_indexer import index_realtime_events


def consume_trending_events(max_messages: int = 100, timeout_ms: int = 15000) -> dict:
    consumer = KafkaConsumer(
        KAFKA_TRENDING_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    events = []
    for message in consumer:
        events.append(message.value)
        if len(events) >= max_messages:
            break

    consumer.close()

    if not events:
        return {"stored_events": 0, "indexed_events": 0}

    output_dir = DATA_ROOT / "realtime" / "movie" / "tmdb_trending_events" / current_date_yyyymmdd()
    ensure_dir(output_dir)
    output_file = output_dir / f"events_{utc_now_iso().replace(':', '-')}.jsonl"
    write_json_lines(output_file, events)

    indexed = index_realtime_events(events)
    return {
        "stored_events": len(events),
        "indexed_events": indexed,
        "output_file": str(output_file),
    }


if __name__ == "__main__":
    consume_trending_events()
