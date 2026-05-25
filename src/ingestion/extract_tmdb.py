import json
from typing import Any, Dict, List

from src.config.settings import (
    TMDB_API_KEY,
    TMDB_BASE_URL,
    MAX_TMDB_PAGES,
    MAX_MOVIES_FOR_DETAILS,
    build_path,
)
from src.ingestion.api_client import ApiClient
from src.utils.date_utils import utc_now_iso
from src.utils.file_utils import ensure_dir, write_json


client = ApiClient()


def _tmdb_params(extra_params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US",
    }

    if extra_params:
        params.update(extra_params)

    return params


def create_raw_directories() -> str:
    directories = [
        build_path("raw", "tmdb", "tmdb_trending"),
        build_path("raw", "tmdb", "tmdb_popular"),
        build_path("raw", "tmdb", "tmdb_movie_details"),
        build_path("raw", "tmdb", "tmdb_external_ids"),
        build_path("raw", "omdb", "omdb_movie_details"),
    ]

    for directory in directories:
        ensure_dir(directory)

    return "Raw directories created successfully"


def _persist_payload(output_path, payload):
    write_json(output_path, payload)


def extract_tmdb_trending() -> str:
    url = f"{TMDB_BASE_URL}/trending/movie/day"
    data = client.get(url, params=_tmdb_params())

    output_path = (
        build_path("raw", "tmdb", "tmdb_trending")
        / "part-0001.json"
    )

    payload = {
        "source": "tmdb",
        "entity": "tmdb_trending",
        "ingestion_time_utc": utc_now_iso(),
        "data": data,
    }

    _persist_payload(output_path, payload)
    return str(output_path)


def extract_tmdb_popular() -> List[str]:
    output_paths = []

    for page in range(1, MAX_TMDB_PAGES + 1):
        url = f"{TMDB_BASE_URL}/movie/popular"
        data = client.get(url, params=_tmdb_params({"page": page}))

        output_path = (
            build_path("raw", "tmdb", "tmdb_popular")
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

    return output_paths


def collect_movie_ids_from_raw_trending() -> List[int]:
    trending_root = build_path("raw", "tmdb", "tmdb_trending")
    movie_ids = []

    if not trending_root.exists():
        return movie_ids

    for json_file in trending_root.rglob("*.json"):
        with json_file.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        results = payload.get("data", {}).get("results", [])

        for movie in results:
            movie_id = movie.get("id")
            if movie_id:
                movie_ids.append(movie_id)

    return list(dict.fromkeys(movie_ids))


def collect_movie_ids_from_raw_popular() -> List[int]:
    popular_root = build_path("raw", "tmdb", "tmdb_popular")

    movie_ids = []

    if not popular_root.exists():
        return movie_ids

    for json_file in popular_root.rglob("*.json"):
        with json_file.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        results = payload.get("data", {}).get("results", [])

        for movie in results:
            movie_id = movie.get("id")

            if movie_id:
                movie_ids.append(movie_id)

    unique_ids = list(dict.fromkeys(movie_ids))
    return unique_ids[:MAX_MOVIES_FOR_DETAILS]


def collect_candidate_movie_ids() -> List[int]:
    merged_ids = collect_movie_ids_from_raw_trending() + collect_movie_ids_from_raw_popular()
    unique_ids = list(dict.fromkeys(merged_ids))
    return unique_ids[:MAX_MOVIES_FOR_DETAILS]


def extract_tmdb_movie_details() -> List[str]:
    movie_ids = collect_candidate_movie_ids()
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
    movie_ids = collect_candidate_movie_ids()
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
