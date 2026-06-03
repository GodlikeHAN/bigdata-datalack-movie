from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

from spark_jobs.common import create_spark_session, resolve_partition, write_parquet
from src.config.settings import build_path
from src.utils.country_geo import COUNTRY_CENTROIDS
from src.utils.country_polygons import sample_country_point


GEO_POINT_SCHEMA = StructType(
    [
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]
)
sample_country_point_udf = F.udf(sample_country_point, GEO_POINT_SCHEMA)


def _country_location_expr(country_code_column: str):
    entries = []
    for code, (lat, lon) in COUNTRY_CENTROIDS.items():
        entries.extend(
            [
                F.lit(code),
                F.struct(
                    F.lit(lat).cast("double").alias("lat"),
                    F.lit(lon).cast("double").alias("lon"),
                ),
            ]
        )

    return F.element_at(F.create_map(*entries), F.upper(F.col(country_code_column)))


def run(run_date: str | None = None) -> str:
    tmdb_partition = resolve_partition("formatted", "tmdb", "tmdb_movie_details", run_date)
    omdb_partition = resolve_partition("formatted", "omdb", "omdb_movie_ratings", run_date)

    spark = create_spark_session("combine_ratings_boxoffice")
    tmdb = spark.read.parquet(str(tmdb_partition))
    omdb = spark.read.parquet(str(omdb_partition))

    if "production_country_codes" not in tmdb.columns:
        tmdb = tmdb.withColumn("production_country_codes", F.array().cast("array<string>"))

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

    release_age_months = F.months_between(F.current_date(), F.col("release_date"))
    actual_final_revenue = F.when(F.col("revenue") > 0, F.col("revenue")).otherwise(F.col("omdb_omdb_boxoffice_usd"))

    enriched = (
        joined.withColumn("document_id", F.concat(F.lit("tmdb-"), F.col("tmdb_id").cast("string")))
        .withColumn("release_year", F.year(F.col("release_date")))
        .withColumn("release_age_months", F.round(release_age_months, 2))
        .withColumn(
            "movie_lifecycle",
            F.when(F.col("release_date").isNotNull() & (release_age_months >= 12), F.lit("historical")).otherwise(F.lit("active")),
        )
        .withColumn("main_genre", F.element_at(F.col("genres"), 1))
        .withColumn("main_production_country", F.element_at(F.col("production_countries"), 1))
        .withColumn("main_production_country_code", F.element_at(F.col("production_country_codes"), 1))
        .withColumn("main_production_country_location", _country_location_expr("main_production_country_code"))
        .withColumn(
            "movie_map_location",
            sample_country_point_udf(
                F.col("main_production_country_code"),
                F.col("document_id"),
                F.col("tmdb_id").cast("int"),
            ),
        )
        .withColumn("actual_final_revenue", actual_final_revenue.cast("double"))
        .withColumn("profit", F.col("actual_final_revenue") - F.col("budget"))
        .withColumn("roi", F.when(F.col("budget") > 0, F.col("profit") / F.col("budget")))
        .withColumn("tmdb_vote_average", F.col("vote_average"))
        .withColumn("tmdb_score_100", F.col("vote_average") * 10)
        .withColumn("imdb_rating", F.col("omdb_imdb_rating"))
        .withColumn("imdb_score_100", F.col("omdb_imdb_score_100"))
        .withColumn("rt_score_100", F.col("omdb_rt_score_100"))
        .withColumn("metacritic_score_100", F.col("omdb_metacritic_score_100"))
        .withColumn("box_office_omdb", F.col("omdb_omdb_boxoffice_usd"))
        .withColumn("rating_consensus_score", F.when(rating_weight_sum > 0, rating_numerator / rating_weight_sum))
        .withColumn("predicted_final_revenue", F.lit(None).cast("double"))
        .withColumn("ml_revenue_gap", F.lit(None).cast("double"))
        .withColumn("ml_gap_ratio", F.lit(None).cast("double"))
        .withColumn("performance_category", F.lit(None).cast("string"))
    )

    final_dataframe = enriched.select(
        "document_id",
        "tmdb_id",
        "imdb_id",
        "title",
        "original_title",
        "release_year",
        "release_date",
        "release_age_months",
        "movie_lifecycle",
        "genres",
        "main_genre",
        "main_production_country",
        "main_production_country_code",
        "main_production_country_location",
        "movie_map_location",
        "runtime",
        "budget",
        "revenue",
        "actual_final_revenue",
        "profit",
        "roi",
        "tmdb_vote_average",
        "tmdb_score_100",
        "vote_count",
        "imdb_rating",
        "imdb_score_100",
        "rt_score_100",
        "metacritic_score_100",
        "rating_consensus_score",
        "popularity",
        "poster_path",
        "poster_url",
        "youtube_trailer_key",
        "youtube_trailer_url",
        "predicted_final_revenue",
        "ml_revenue_gap",
        "ml_gap_ratio",
        "performance_category",
        "ingestion_time_utc",
    )

    output_path = build_path("usage", "ratings_boxoffice_analysis", "movie_performance_gap", tmdb_partition.name)
    result = write_parquet(final_dataframe, output_path)
    spark.stop()
    return result


if __name__ == "__main__":
    run()
