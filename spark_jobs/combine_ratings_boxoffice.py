from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import Window

from spark_jobs.common import create_spark_session, resolve_partition, write_parquet
from src.config.settings import build_path


def run(run_date: str | None = None) -> str:
    tmdb_partition = resolve_partition("formatted", "tmdb", "tmdb_movie_details", run_date)
    omdb_partition = resolve_partition("formatted", "omdb", "omdb_movie_ratings", run_date)

    spark = create_spark_session("combine_ratings_boxoffice")
    tmdb = spark.read.parquet(str(tmdb_partition))
    omdb = spark.read.parquet(str(omdb_partition))

    omdb_prefixed = omdb.select(
        *[
            F.col(column).alias(column if column == "imdb_id" else f"omdb_{column}")
            for column in omdb.columns
        ]
    )

    joined = tmdb.join(omdb_prefixed, on="imdb_id", how="left")

    rating_weight_sum = (
        F.when(F.col("vote_average").isNotNull(), F.lit(0.35)).otherwise(F.lit(0.0))
        + F.when(F.col("omdb_imdb_score_100").isNotNull(), F.lit(0.35)).otherwise(F.lit(0.0))
        + F.when(F.col("omdb_rt_score_100").isNotNull(), F.lit(0.20)).otherwise(F.lit(0.0))
        + F.when(F.col("omdb_metacritic_score_100").isNotNull(), F.lit(0.10)).otherwise(F.lit(0.0))
    )

    rating_numerator = (
        F.coalesce(F.col("vote_average") * 10 * 0.35, F.lit(0.0))
        + F.coalesce(F.col("omdb_imdb_score_100") * 0.35, F.lit(0.0))
        + F.coalesce(F.col("omdb_rt_score_100") * 0.20, F.lit(0.0))
        + F.coalesce(F.col("omdb_metacritic_score_100") * 0.10, F.lit(0.0))
    )

    revenue_window = Window.orderBy(F.coalesce(F.col("revenue"), F.lit(0)))
    roi_window = Window.orderBy(F.coalesce(F.when(F.col("budget") > 0, (F.col("revenue") - F.col("budget")) / F.col("budget")), F.lit(0.0)))
    popularity_window = Window.orderBy(F.coalesce(F.col("popularity"), F.lit(0.0)))

    enriched = (
        joined.withColumn("release_year", F.year(F.col("release_date")))
        .withColumn("main_genre", F.element_at(F.col("genres"), 1))
        .withColumn("main_production_country", F.element_at(F.col("production_countries"), 1))
        .withColumn("profit", F.col("revenue") - F.col("budget"))
        .withColumn("roi", F.when(F.col("budget") > 0, F.col("profit") / F.col("budget")))
        .withColumn("tmdb_vote_average", F.col("vote_average"))
        .withColumn("tmdb_score_100", F.col("vote_average") * 10)
        .withColumn("imdb_rating", F.col("omdb_imdb_rating"))
        .withColumn("imdb_score_100", F.col("omdb_imdb_score_100"))
        .withColumn("rt_score_100", F.col("omdb_rt_score_100"))
        .withColumn("metacritic_score_100", F.col("omdb_metacritic_score_100"))
        .withColumn("box_office_omdb", F.col("omdb_omdb_boxoffice_usd"))
        .withColumn("rating_consensus_score", F.when(rating_weight_sum > 0, rating_numerator / rating_weight_sum))
        .withColumn("revenue_percentile", F.percent_rank().over(revenue_window) * 100)
        .withColumn("roi_percentile", F.percent_rank().over(roi_window) * 100)
        .withColumn("popularity_percentile", F.percent_rank().over(popularity_window) * 100)
        .withColumn(
            "commercial_score",
            (F.col("revenue_percentile") * 0.45) + (F.col("roi_percentile") * 0.35) + (F.col("popularity_percentile") * 0.20),
        )
        .withColumn("performance_gap", F.col("commercial_score") - F.col("rating_consensus_score"))
        .withColumn(
            "performance_category",
            F.when((F.col("rating_consensus_score") >= 60) & (F.col("commercial_score") >= 60), F.lit("Balanced Success"))
            .when((F.col("rating_consensus_score") >= 60) & (F.col("commercial_score") < 60), F.lit("Hidden Gem"))
            .when((F.col("rating_consensus_score") < 60) & (F.col("commercial_score") >= 60), F.lit("Blockbuster Paradox"))
            .otherwise(F.lit("Weak Performer")),
        )
        .withColumn("is_hidden_gem", F.col("performance_category") == "Hidden Gem")
        .withColumn("is_blockbuster_paradox", F.col("performance_category") == "Blockbuster Paradox")
        .withColumn("ml_expected_revenue", F.lit(None).cast("double"))
        .withColumn("ml_revenue_gap", F.lit(None).cast("double"))
        .withColumn("ml_gap_ratio", F.lit(None).cast("double"))
        .withColumn("is_commercial_overperformer", F.lit(False))
        .withColumn("is_commercial_underperformer", F.lit(False))
        .withColumnRenamed("ingestion_time_utc", "ingestion_time_utc")
    )

    final_dataframe = enriched.select(
        "tmdb_id",
        "imdb_id",
        "title",
        "release_year",
        "release_date",
        "genres",
        "main_genre",
        "main_production_country",
        "runtime",
        "budget",
        "revenue",
        "profit",
        "roi",
        "tmdb_vote_average",
        "tmdb_score_100",
        "imdb_rating",
        "imdb_score_100",
        "rt_score_100",
        "metacritic_score_100",
        "rating_consensus_score",
        "popularity",
        "commercial_score",
        "performance_gap",
        "ml_expected_revenue",
        "ml_revenue_gap",
        "ml_gap_ratio",
        "performance_category",
        "is_hidden_gem",
        "is_blockbuster_paradox",
        "is_commercial_overperformer",
        "is_commercial_underperformer",
        "ingestion_time_utc",
    )

    output_path = build_path("usage", "ratings_boxoffice_analysis", "movie_performance_gap", tmdb_partition.name)
    result = write_parquet(final_dataframe, output_path)
    spark.stop()
    return result


if __name__ == "__main__":
    run()
