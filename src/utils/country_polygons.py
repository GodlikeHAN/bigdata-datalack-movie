from __future__ import annotations

import hashlib
import json
import random
from functools import lru_cache
from pathlib import Path
from typing import Any

from shapely.geometry import Point, shape
from shapely.geometry.base import BaseGeometry

from src.config.settings import ARTIFACTS_ROOT
from src.utils.country_geo import COUNTRY_CENTROIDS


COUNTRY_TOPOJSON_PATH = ARTIFACTS_ROOT / "world_countries_v7.topo.json"


def _decode_arc(raw_arc: list[list[float]], scale: list[float], translate: list[float]) -> list[list[float]]:
    x = 0
    y = 0
    coordinates = []
    for dx, dy in raw_arc:
        x += dx
        y += dy
        coordinates.append([x * scale[0] + translate[0], y * scale[1] + translate[1]])
    return coordinates


def _arc_coordinates(arc_ref: int, arcs: list[list[list[float]]]) -> list[list[float]]:
    if arc_ref >= 0:
        return arcs[arc_ref]
    return list(reversed(arcs[-arc_ref - 1]))


def _stitch_arcs(arc_refs: list[int], arcs: list[list[list[float]]]) -> list[list[float]]:
    coordinates: list[list[float]] = []
    for index, arc_ref in enumerate(arc_refs):
        arc = _arc_coordinates(arc_ref, arcs)
        coordinates.extend(arc if index == 0 else arc[1:])
    return coordinates


def _topology_geometry_to_geojson(geometry: dict[str, Any], arcs: list[list[list[float]]]) -> dict[str, Any] | None:
    geometry_type = geometry.get("type")
    geometry_arcs = geometry.get("arcs")

    if geometry_type == "Polygon":
        return {
            "type": "Polygon",
            "coordinates": [_stitch_arcs(ring, arcs) for ring in geometry_arcs],
        }

    if geometry_type == "MultiPolygon":
        return {
            "type": "MultiPolygon",
            "coordinates": [
                [_stitch_arcs(ring, arcs) for ring in polygon]
                for polygon in geometry_arcs
            ],
        }

    return None


@lru_cache(maxsize=1)
def _country_geometries() -> dict[str, BaseGeometry]:
    if not COUNTRY_TOPOJSON_PATH.exists():
        raise FileNotFoundError(
            f"Country boundary file not found: {COUNTRY_TOPOJSON_PATH}. "
            "Download Elastic Maps Service World Countries before running the batch pipeline."
        )

    topology = json.loads(Path(COUNTRY_TOPOJSON_PATH).read_text(encoding="utf-8"))
    transform = topology["transform"]
    decoded_arcs = [
        _decode_arc(raw_arc, transform["scale"], transform["translate"])
        for raw_arc in topology["arcs"]
    ]

    geometries: dict[str, BaseGeometry] = {}
    for geometry in topology["objects"]["data"]["geometries"]:
        country_code = geometry.get("properties", {}).get("iso2")
        geojson_geometry = _topology_geometry_to_geojson(geometry, decoded_arcs)
        if not country_code or not geojson_geometry:
            continue

        country_geometry = shape(geojson_geometry)
        if not country_geometry.is_valid:
            country_geometry = country_geometry.buffer(0)
        geometries[country_code.upper()] = country_geometry

    return geometries


def _seed_value(*parts: object) -> int:
    key = "|".join("" if part is None else str(part) for part in parts)
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:16], 16)


def sample_country_point(country_code: str | None, stable_key: object, sequence: int = 0) -> dict[str, float] | None:
    if not country_code:
        return None

    country_geometry = _country_geometries().get(country_code.upper())
    if country_geometry is None or country_geometry.is_empty:
        return None

    min_x, min_y, max_x, max_y = country_geometry.bounds
    width = max(max_x - min_x, 0.01)
    height = max(max_y - min_y, 0.01)
    configured_centroid = COUNTRY_CENTROIDS.get(country_code.upper())
    configured_center = (
        Point(configured_centroid[1], configured_centroid[0])
        if configured_centroid
        else None
    )
    geometry_centroid = country_geometry.centroid
    if configured_center and country_geometry.contains(configured_center):
        center = configured_center
    elif country_geometry.contains(geometry_centroid):
        center = geometry_centroid
    else:
        center = country_geometry.representative_point()
    random_generator = random.Random(_seed_value(country_code, stable_key, sequence))

    for attempt in range(1200):
        phase = 1 + attempt // 180
        lon_sigma = width * min(0.045 * phase, 0.24)
        lat_sigma = height * min(0.045 * phase, 0.24)
        lon = random_generator.gauss(center.x, lon_sigma)
        lat = random_generator.gauss(center.y, lat_sigma)
        point = Point(lon, lat)
        if country_geometry.contains(point):
            return {"lat": float(lat), "lon": float(lon)}

    return {"lat": float(center.y), "lon": float(center.x)}
