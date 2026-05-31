from __future__ import annotations

from datetime import datetime
from typing import Any


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text and text != "N/A" else None


def safe_int(value: Any) -> int | None:
    text = _clean_string(value)
    if text is None:
        return None
    text = text.replace(",", "").replace("%", "").replace("$", "").replace(" min", "")
    if "/" in text:
        text = text.split("/", 1)[0]
    try:
        return int(float(text))
    except ValueError:
        return None


def safe_float(value: Any) -> float | None:
    text = _clean_string(value)
    if text is None:
        return None
    text = text.replace(",", "").replace("%", "").replace("$", "").replace(" min", "")
    if "/" in text:
        text = text.split("/", 1)[0]
    try:
        return float(text)
    except ValueError:
        return None


def parse_currency(value: Any) -> int | None:
    return safe_int(value)

def parse_percentage(value: Any) -> float | None:
    return safe_float(value)


def parse_omdb_date(value: Any) -> str | None:
    text = _clean_string(value)
    if text is None:
        return None
    try:
        return datetime.strptime(text, "%d %b %Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_rating_value(ratings: list[dict[str, Any]], source_name: str) -> float | None:
    for rating in ratings or []:
        if rating.get("Source") == source_name:
            return parse_percentage(rating.get("Value"))
    return None
