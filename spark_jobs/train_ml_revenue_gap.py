from __future__ import annotations

import json

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.common import create_spark_session, resolve_partition, write_parquet
from src.config.settings import ARTIFACTS_ROOT, build_path
from src.utils.file_utils import ensure_dir, write_text


NUMERIC_FEATURES = [
    "budget",
    "runtime",
    "release_year",
    "tmdb_vote_average",
    "vote_count",
    "popularity",
    "rating_consensus_score",
    "imdb_score_100",
    "rt_score_100",
    "metacritic_score_100",
]

FINAL_COLUMNS = [
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
    "runtime",
    "budget",
    "revenue",
    "actual_final_revenue",
    "predicted_final_revenue",
    "ml_revenue_gap",
    "ml_gap_ratio",
    "performance_category",
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
    "source_data_hash",
    "data_hash",
    "ingestion_time_utc",
]


def _prepare_features(dataframe: DataFrame) -> DataFrame:
    prepared = dataframe
    for column in NUMERIC_FEATURES:
        prepared = prepared.withColumn(f"{column}_feature", F.coalesce(F.col(column).cast("double"), F.lit(0.0)))

    return (
        prepared.withColumn("main_genre_feature", F.coalesce(F.col("main_genre"), F.lit("Unknown")))
        .withColumn("main_production_country_feature", F.coalesce(F.col("main_production_country"), F.lit("Unknown")))
        .withColumn("label", F.log1p(F.col("actual_final_revenue").cast("double")))
    )


def _build_pipeline() -> Pipeline:
    main_genre_indexer = StringIndexer(
        inputCol="main_genre_feature",
        outputCol="main_genre_index",
        handleInvalid="keep",
    )
    country_indexer = StringIndexer(
        inputCol="main_production_country_feature",
        outputCol="main_production_country_index",
        handleInvalid="keep",
    )
    encoder = OneHotEncoder(
        inputCols=["main_genre_index", "main_production_country_index"],
        outputCols=["main_genre_vector", "main_production_country_vector"],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=[f"{column}_feature" for column in NUMERIC_FEATURES]
        + ["main_genre_vector", "main_production_country_vector"],
        outputCol="features",
        handleInvalid="keep",
    )
    regressor = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction_log_revenue",
        numTrees=120,
        maxDepth=7,
        minInstancesPerNode=1,
        seed=42,
    )
    return Pipeline(stages=[main_genre_indexer, country_indexer, encoder, assembler, regressor])


def _apply_business_labels(dataframe: DataFrame) -> DataFrame:
    predicted = F.greatest(F.exp(F.col("prediction_log_revenue")) - F.lit(1.0), F.lit(0.0))
    gap = F.col("actual_final_revenue") - F.col("predicted_final_revenue")
    ratio = F.when(F.col("predicted_final_revenue") > 0, gap / F.col("predicted_final_revenue"))

    labeled = (
        dataframe.withColumn("predicted_final_revenue", predicted)
        .withColumn("ml_revenue_gap", gap)
        .withColumn("ml_gap_ratio", ratio)
        .withColumn(
            "performance_category",
            F.when(F.col("ml_gap_ratio") > 0.20, F.lit("Commercial Overperformer"))
            .when(F.col("ml_gap_ratio") < -0.20, F.lit("Commercial Underperformer"))
            .otherwise(F.lit("As Expected")),
        )
    )

    return labeled.withColumn(
        "data_hash",
        F.sha2(
            F.to_json(
                F.struct(
                    "source_data_hash",
                    "movie_lifecycle",
                    "actual_final_revenue",
                    "predicted_final_revenue",
                    "ml_gap_ratio",
                    "performance_category",
                    "poster_url",
                    "youtube_trailer_url",
                )
            ),
            256,
        ),
    )


def run(run_date: str | None = None) -> str:
    usage_partition = resolve_partition("usage", "ratings_boxoffice_analysis", "movie_performance_gap", run_date)
    spark = create_spark_session("spark_ml_revenue_gap")
    base_dataframe = spark.read.parquet(str(usage_partition))
    prepared = _prepare_features(base_dataframe)

    historical_movies = prepared.filter(
        (F.col("movie_lifecycle") == "historical")
        & F.col("actual_final_revenue").isNotNull()
        & (F.col("actual_final_revenue") > 0)
    )
    historical_count = historical_movies.count()
    if historical_count == 0:
        spark.stop()
        raise ValueError("Spark ML training requires at least one historical movie with actual_final_revenue > 0.")

    active_movies = prepared.filter(F.col("movie_lifecycle") == "active")
    historical_for_scoring = prepared.filter(F.col("movie_lifecycle") == "historical")
    active_count = active_movies.count()
    historical_scoring_count = historical_for_scoring.count()

    model = _build_pipeline().fit(historical_movies)
    active_predictions = model.transform(active_movies)
    historical_predictions = model.transform(historical_for_scoring)
    all_predictions = historical_predictions.unionByName(active_predictions, allowMissingColumns=True)

    final_dataframe = _apply_business_labels(all_predictions).select(*FINAL_COLUMNS)

    output_path = build_path("usage", "ratings_boxoffice_analysis", "movie_performance_gap", usage_partition.name)
    result = write_parquet(final_dataframe, output_path)

    model_dir = ARTIFACTS_ROOT / "models" / usage_partition.name
    ensure_dir(model_dir)
    model.write().overwrite().save(str(model_dir / "spark_revenue_model"))
    write_text(
        model_dir / "metrics.json",
        json.dumps(
            {
                "training_rows": int(historical_count),
                "active_prediction_rows": int(active_count),
                "historical_scoring_rows": int(historical_scoring_count),
                "model_type": "Spark ML RandomForestRegressor",
                "label_rule": "Overperformer > 20%, Underperformer < -20%, otherwise As Expected",
            },
            indent=2,
        ),
    )

    spark.stop()
    return result


if __name__ == "__main__":
    run()
