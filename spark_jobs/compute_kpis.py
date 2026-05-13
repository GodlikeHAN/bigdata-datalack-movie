from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.common import create_spark_session, resolve_partition, write_parquet
from src.config.settings import build_path


def aggregate_genre_year_performance(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupBy("release_year", "main_genre").agg(
        F.count("*").alias("movie_count"),
        F.round(F.avg("rating_consensus_score"), 2).alias("avg_rating_consensus_score"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.round(F.avg("roi"), 4).alias("avg_roi"),
        F.round(F.avg("performance_gap"), 2).alias("avg_performance_gap"),
        F.sum(F.when(F.col("is_hidden_gem"), 1).otherwise(0)).alias("hidden_gem_count"),
        F.sum(F.when(F.col("is_blockbuster_paradox"), 1).otherwise(0)).alias("blockbuster_paradox_count"),
        F.sum(F.when(F.col("is_commercial_overperformer"), 1).otherwise(0)).alias("commercial_overperformer_count"),
        F.sum(F.when(F.col("is_commercial_underperformer"), 1).otherwise(0)).alias("commercial_underperformer_count"),
    )


def run(run_date: str | None = None) -> str:
    usage_partition = resolve_partition("usage", "ratings_boxoffice_analysis", "movie_performance_gap", run_date)

    spark = create_spark_session("compute_kpis")
    dataframe = spark.read.parquet(str(usage_partition))
    aggregated = aggregate_genre_year_performance(dataframe)

    output_path = build_path("usage", "ratings_boxoffice_analysis", "genre_year_performance", usage_partition.name)
    result = write_parquet(aggregated, output_path)
    spark.stop()
    return result


if __name__ == "__main__":
    run()
