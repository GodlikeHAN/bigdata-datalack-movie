from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import DATA_ROOT
from src.utils.file_utils import latest_partition_dir, read_json
from src.utils.s3_utils import upload_file_to_s3


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
    df.coalesce(1).write.mode("overwrite").parquet(str(output_path))
    for file in output_path.rglob("*"):
        if file.is_file():
            upload_file_to_s3(file)
    return str(output_path)
