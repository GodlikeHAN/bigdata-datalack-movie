from __future__ import annotations

from pathlib import Path
import shutil
import uuid

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import DATA_ROOT
from src.utils.file_utils import latest_partition_dir, read_json


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def resolve_partition(layer: str, group: str, table: str, run_date: str | None = None) -> Path:
    root = DATA_ROOT / layer / group / table
    if run_date:
        return root / run_date

    partition = latest_partition_dir(root)
    if not partition:
        raise FileNotFoundError(f"No partition found under {root}")
    return partition


def load_json_payloads(partition: Path) -> list[dict]:
    if not partition.exists():
        raise FileNotFoundError(f"Partition does not exist: {partition}")
    return [read_json(file) for file in sorted(partition.glob("*.json"))]


def write_parquet(df: DataFrame, output_path: Path) -> str:
    temp_path = output_path.parent / f".{output_path.name}__tmp_{uuid.uuid4().hex}"
    if temp_path.exists():
        shutil.rmtree(temp_path)

    df.coalesce(1).write.mode("overwrite").parquet(str(temp_path))

    if not list(temp_path.glob("part-*.parquet")):
        raise FileNotFoundError(f"Spark write completed without parquet part files in {temp_path}")

    if output_path.exists():
        shutil.rmtree(output_path)

    temp_path.replace(output_path)
    return str(output_path)
