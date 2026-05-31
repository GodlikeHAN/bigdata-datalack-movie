from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.common import create_spark_session, load_json_payloads, resolve_partition, write_parquet
from src.config.settings import build_path


POSTER_BASE_URL = "https://image.tmdb.org/t/p/w500"
YOUTUBE_BASE_URL = "https://www.youtube.com/watch?v="


def _youtube_trailer_key(movie: dict) -> str | None:
    videos = movie.get("videos", {}).get("results", []) or []
    youtube_videos = [video for video in videos if video.get("site") == "YouTube" and video.get("key")]

    official_trailers = [
        video for video in youtube_videos
        if video.get("type") == "Trailer" and video.get("official") is True
    ]
    trailers = [video for video in youtube_videos if video.get("type") == "Trailer"]

    selected = (official_trailers or trailers or youtube_videos)
    return selected[0].get("key") if selected else None


def run(run_date: str | None = None) -> str:
    input_partition = resolve_partition("raw", "tmdb", "tmdb_movie_details", run_date)
    payloads = load_json_payloads(input_partition)

    records = []
    for payload in payloads:
        movie = payload.get("data", {})
        poster_path = movie.get("poster_path")
        youtube_trailer_key = _youtube_trailer_key(movie)
        records.append(
            {
                "tmdb_id": movie.get("id") or payload.get("tmdb_id"),
                "title": movie.get("title"),
                "original_title": movie.get("original_title"),
                "release_date": movie.get("release_date"),
                "poster_path": poster_path,
                "poster_url": f"{POSTER_BASE_URL}{poster_path}" if poster_path else None,
                "youtube_trailer_key": youtube_trailer_key,
                "youtube_trailer_url": f"{YOUTUBE_BASE_URL}{youtube_trailer_key}" if youtube_trailer_key else None,
                "genres": [genre.get("name") for genre in movie.get("genres", []) if genre.get("name")],
                "runtime": movie.get("runtime"),
                "budget": movie.get("budget"),
                "revenue": movie.get("revenue"),
                "vote_average": movie.get("vote_average"),
                "vote_count": movie.get("vote_count"),
                "popularity": movie.get("popularity"),
                "production_countries": [
                    country.get("name") for country in movie.get("production_countries", []) if country.get("name")
                ],
                "imdb_id": movie.get("imdb_id"),
                "ingestion_time_utc": payload.get("ingestion_time_utc"),
            }
        )

    spark = create_spark_session("format_tmdb_movies")
    schema = StructType(
        [
            StructField("tmdb_id", LongType(), True),
            StructField("title", StringType(), True),
            StructField("original_title", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("poster_path", StringType(), True),
            StructField("poster_url", StringType(), True),
            StructField("youtube_trailer_key", StringType(), True),
            StructField("youtube_trailer_url", StringType(), True),
            StructField("genres", ArrayType(StringType()), True),
            StructField("runtime", LongType(), True),
            StructField("budget", LongType(), True),
            StructField("revenue", LongType(), True),
            StructField("vote_average", DoubleType(), True),
            StructField("vote_count", LongType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("production_countries", ArrayType(StringType()), True),
            StructField("imdb_id", StringType(), True),
            StructField("ingestion_time_utc", StringType(), True),
        ]
    )
    dataframe = spark.createDataFrame(records, schema=schema)

    formatted = (
        dataframe.filter(F.col("tmdb_id").isNotNull())
        .withColumn("release_date", F.to_date("release_date"))
        .withColumn("runtime", F.col("runtime").cast("int"))
        .withColumn("budget", F.col("budget").cast("long"))
        .withColumn("revenue", F.col("revenue").cast("long"))
        .withColumn("vote_average", F.col("vote_average").cast("double"))
        .withColumn("vote_count", F.col("vote_count").cast("int"))
        .withColumn("popularity", F.col("popularity").cast("double"))
        .withColumn("ingestion_time_utc", F.to_timestamp("ingestion_time_utc"))
    )

    window = Window.partitionBy("tmdb_id").orderBy(F.col("ingestion_time_utc").desc())
    deduplicated = formatted.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

    output_path = build_path("formatted", "tmdb", "tmdb_movie_details", input_partition.name)
    result = write_parquet(deduplicated, output_path)
    spark.stop()
    return result


if __name__ == "__main__":
    run()
