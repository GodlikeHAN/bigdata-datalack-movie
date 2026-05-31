import json
from typing import Any, Dict, List

from src.config.settings import (
    TMDB_API_KEY,
    TMDB_BASE_URL,
    MAX_MOVIES_FOR_DETAILS,
    build_path,
)
from src.ingestion.api_client import ApiClient
from src.utils.date_utils import utc_now_iso
from src.utils.file_utils import clear_directory, write_json


client = ApiClient()
TMDB_RESULTS_PER_PAGE = 20
TMDB_MAX_API_PAGE = 500


def _tmdb_params(extra_params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US",
    }

    if extra_params:
        params.update(extra_params)

    return params


def _persist_payload(output_path, payload):
    write_json(output_path, payload)


def extract_tmdb_popular() -> List[str]:
    popular_root = build_path("raw", "tmdb", "tmdb_popular")
    clear_directory(popular_root)

    output_paths = []
    unique_movie_ids = set()
    page = 1
    total_pages = TMDB_MAX_API_PAGE

    while page <= total_pages and len(unique_movie_ids) < MAX_MOVIES_FOR_DETAILS:
        url = f"{TMDB_BASE_URL}/movie/popular"
        data = client.get(url, params=_tmdb_params({"page": page}))
        results = data.get("results", [])
        reported_total_pages = data.get("total_pages") or TMDB_MAX_API_PAGE
        total_pages = min(reported_total_pages, TMDB_MAX_API_PAGE)

        output_path = (
            popular_root
            / f"page_{page}.json"
        )

        payload = {
            "source": "tmdb",
            "entity": "tmdb_popular",
            "page": page,
            "ingestion_time_utc": utc_now_iso(),
            "data": data,
        }

        _persist_payload(output_path, payload)
        output_paths.append(str(output_path))
        for movie in results:
            movie_id = movie.get("id")
            if movie_id:
                unique_movie_ids.add(movie_id)

        if not results:
            break

        page += 1

    return output_paths

def collect_movie_ids_from_raw_popular() -> List[int]:
    popular_root = build_path("raw", "tmdb", "tmdb_popular")

    movie_ids = []

    if not popular_root.exists():
        return movie_ids

    for json_file in sorted(popular_root.rglob("page_*.json"), key=lambda file: file.name):
        with json_file.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        results = payload.get("data", {}).get("results", [])

        for movie in results:
            movie_id = movie.get("id")

            if movie_id:
                movie_ids.append(movie_id)

    unique_ids = list(dict.fromkeys(movie_ids))
    return unique_ids[:MAX_MOVIES_FOR_DETAILS]


def extract_tmdb_movie_details() -> List[str]:
    movie_ids = collect_movie_ids_from_raw_popular()
    clear_directory(build_path("raw", "tmdb", "tmdb_movie_details"))
    output_paths = []

    for movie_id in movie_ids:
        url = f"{TMDB_BASE_URL}/movie/{movie_id}"
        data = client.get(url, params=_tmdb_params({"append_to_response": "videos"}))

        output_path = (
            build_path("raw", "tmdb", "tmdb_movie_details")
            / f"{movie_id}.json"
        )

        payload = {
            "source": "tmdb",
            "entity": "tmdb_movie_details",
            "tmdb_id": movie_id,
            "ingestion_time_utc": utc_now_iso(),
            "data": data,
        }

        _persist_payload(output_path, payload)
        output_paths.append(str(output_path))

    return output_paths


def extract_tmdb_external_ids() -> List[str]:
    movie_ids = collect_movie_ids_from_raw_popular()
    clear_directory(build_path("raw", "tmdb", "tmdb_external_ids"))
    output_paths = []

    for movie_id in movie_ids:
        url = f"{TMDB_BASE_URL}/movie/{movie_id}/external_ids"
        data = client.get(url, params=_tmdb_params())

        output_path = (
            build_path("raw", "tmdb", "tmdb_external_ids")
            / f"{movie_id}.json"
        )

        payload = {
            "source": "tmdb",
            "entity": "tmdb_external_ids",
            "tmdb_id": movie_id,
            "ingestion_time_utc": utc_now_iso(),
            "data": data,
        }

        _persist_payload(output_path, payload)
        output_paths.append(str(output_path))

    return output_paths
