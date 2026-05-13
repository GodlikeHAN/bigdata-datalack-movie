from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import Window

from spark_jobs.common import create_spark_session, load_json_payloads, resolve_partition, write_parquet
from src.config.settings import build_path
from src.utils.parsing_utils import (
    extract_rating_value,
    parse_currency,
    parse_omdb_date,
    parse_runtime_minutes,
    safe_float,
    safe_int,
    split_csv_text,
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
                "title": data.get("Title"),
                "year": safe_int(data.get("Year")),
                "rated": data.get("Rated"),
                "released": parse_omdb_date(data.get("Released")),
                "runtime_minutes": parse_runtime_minutes(data.get("Runtime")),
                "genres": split_csv_text(data.get("Genre")),
                "director": data.get("Director"),
                "actors": split_csv_text(data.get("Actors")),
                "languages": split_csv_text(data.get("Language")),
                "countries": split_csv_text(data.get("Country")),
                "awards": data.get("Awards"),
                "imdb_rating": safe_float(data.get("imdbRating")),
                "imdb_votes": safe_int(data.get("imdbVotes")),
                "metascore": safe_int(data.get("Metascore")),
                "rt_score_100": extract_rating_value(ratings, "Rotten Tomatoes"),
                "metacritic_score_100": extract_rating_value(ratings, "Metacritic") or safe_float(data.get("Metascore")),
                "imdb_score_100": safe_float(data.get("imdbRating")) * 10 if safe_float(data.get("imdbRating")) is not None else None,
                "omdb_boxoffice_usd": parse_currency(data.get("BoxOffice")),
                "box_office_raw": data.get("BoxOffice"),
                "response_ok": data.get("Response") == "True",
                "source": "omdb",
                "ingestion_time_utc": payload.get("ingestion_time_utc"),
            }
        )

    spark = create_spark_session("format_omdb_movies")
    dataframe = spark.createDataFrame(records)

    formatted = (
        dataframe.filter(F.col("imdb_id").isNotNull())
        .filter(F.col("response_ok"))
        .withColumn("released", F.to_date("released"))
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("runtime_minutes", F.col("runtime_minutes").cast("int"))
        .withColumn("imdb_rating", F.col("imdb_rating").cast("double"))
        .withColumn("imdb_votes", F.col("imdb_votes").cast("int"))
        .withColumn("metascore", F.col("metascore").cast("double"))
        .withColumn("rt_score_100", F.col("rt_score_100").cast("double"))
        .withColumn("metacritic_score_100", F.col("metacritic_score_100").cast("double"))
        .withColumn("imdb_score_100", F.col("imdb_score_100").cast("double"))
        .withColumn("omdb_boxoffice_usd", F.col("omdb_boxoffice_usd").cast("long"))
        .withColumn("ingestion_time_utc", F.to_timestamp("ingestion_time_utc"))
        .withColumn(
            "has_omdb_ratings",
            F.col("imdb_score_100").isNotNull() | F.col("rt_score_100").isNotNull() | F.col("metacritic_score_100").isNotNull(),
        )
        .withColumn("has_omdb_boxoffice", F.col("omdb_boxoffice_usd").isNotNull())
    )

    window = Window.partitionBy("imdb_id").orderBy(F.col("ingestion_time_utc").desc())
    deduplicated = formatted.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

    output_path = build_path("formatted", "omdb", "omdb_movie_ratings", input_partition.name)
    result = write_parquet(deduplicated, output_path)
    spark.stop()
    return result


if __name__ == "__main__":
    run()
