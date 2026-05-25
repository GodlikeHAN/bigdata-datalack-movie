import json
from typing import List

from src.config.settings import (
    OMDB_API_KEY,
    OMDB_BASE_URL,
    MAX_MOVIES_FOR_DETAILS,
    build_path,
)
from src.ingestion.api_client import ApiClient
from src.utils.date_utils import utc_now_iso
from src.utils.file_utils import write_json


client = ApiClient(sleep_seconds=1.0)


def collect_imdb_ids_from_tmdb_external_ids() -> List[str]:
    external_ids_root = build_path("raw", "tmdb", "tmdb_external_ids")

    imdb_ids = []

    if not external_ids_root.exists():
        return imdb_ids

    for json_file in external_ids_root.rglob("*.json"):
        with json_file.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        imdb_id = payload.get("data", {}).get("imdb_id")

        if imdb_id:
            imdb_ids.append(imdb_id)

    unique_ids = list(dict.fromkeys(imdb_ids))
    return unique_ids[:MAX_MOVIES_FOR_DETAILS]


def extract_omdb_movie_details() -> List[str]:
    imdb_ids = collect_imdb_ids_from_tmdb_external_ids()
    output_paths = []

    for imdb_id in imdb_ids:
        data = client.get(
            OMDB_BASE_URL,
            params={
                "apikey": OMDB_API_KEY,
                "i": imdb_id,
                "plot": "short",
                "r": "json",
            },
        )

        output_path = (
            build_path("raw", "omdb", "omdb_movie_details")
            / f"{imdb_id}.json"
        )

        payload = {
            "source": "omdb",
            "entity": "omdb_movie_details",
            "imdb_id": imdb_id,
            "ingestion_time_utc": utc_now_iso(),
            "data": data,
        }

        write_json(output_path, payload)
        output_paths.append(str(output_path))

    return output_paths
