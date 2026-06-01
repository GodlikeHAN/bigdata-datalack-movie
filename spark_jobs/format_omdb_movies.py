from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from spark_jobs.common import create_spark_session, load_json_payloads, resolve_partition, write_parquet
from src.config.settings import build_path
from src.utils.parsing_utils import (
    extract_rating_value,
    parse_currency,
    safe_float,
)


def run(run_date: str | None = None) -> str:
    input_partition = resolve_partition("raw", "omdb", "omdb_movie_details", run_date)
    payloads = load_json_payloads(input_partition)

    records = []
    for payload in payloads:
        data = payload.get("data", {})
        ratings = data.get("Ratings", [])

        records.append(
            {
                "imdb_id": payload.get("imdb_id") or data.get("imdbID"),
                "imdb_rating": safe_float(data.get("imdbRating")),
                "rt_score_100": extract_rating_value(ratings, "Rotten Tomatoes"),
                "metacritic_score_100": extract_rating_value(ratings, "Metacritic") or safe_float(data.get("Metascore")),
                "imdb_score_100": safe_float(data.get("imdbRating")) * 10 if safe_float(data.get("imdbRating")) is not None else None,
                "omdb_boxoffice_usd": parse_currency(data.get("BoxOffice")),
                "response_ok": data.get("Response") == "True",
                "ingestion_time_utc": payload.get("ingestion_time_utc"),
            }
        )

    spark = create_spark_session("format_omdb_movies")
    schema = StructType(
        [
            StructField("imdb_id", StringType(), True),
            StructField("imdb_rating", DoubleType(), True),
            StructField("rt_score_100", DoubleType(), True),
            StructField("metacritic_score_100", DoubleType(), True),
            StructField("imdb_score_100", DoubleType(), True),
            StructField("omdb_boxoffice_usd", LongType(), True),
            StructField("response_ok", BooleanType(), True),
            StructField("ingestion_time_utc", StringType(), True),
        ]
    )
    dataframe = spark.createDataFrame(records, schema=schema)

    formatted = (
        dataframe.filter(F.col("imdb_id").isNotNull())
        .filter(F.col("response_ok") == True)
        .withColumn("imdb_rating", F.col("imdb_rating").cast("double"))
        .withColumn("rt_score_100", F.col("rt_score_100").cast("double"))
        .withColumn("metacritic_score_100", F.col("metacritic_score_100").cast("double"))
        .withColumn("imdb_score_100", F.col("imdb_score_100").cast("double"))
        .withColumn("omdb_boxoffice_usd", F.col("omdb_boxoffice_usd").cast("long"))
        .withColumn("ingestion_time_utc", F.to_timestamp("ingestion_time_utc"))
        .drop("response_ok")
    )

    window = Window.partitionBy("imdb_id").orderBy(F.col("ingestion_time_utc").desc())
    deduplicated = formatted.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

    output_path = build_path("formatted", "omdb", "omdb_movie_ratings", input_partition.name)
    result = write_parquet(deduplicated, output_path)
    spark.stop()
    return result


if __name__ == "__main__":
    run()
