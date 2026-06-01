import json
import shutil
from pathlib import Path
from typing import Any


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def clear_directory(path: Path) -> None:
    ensure_dir(path)
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json_lines(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False))
            file.write("\n")


def write_text(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")


def list_partition_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted([child for child in root.iterdir() if child.is_dir()], key=lambda item: item.name)


def latest_partition_dir(root: Path) -> Path | None:
    partitions = list_partition_dirs(root)
    return partitions[-1] if partitions else None
