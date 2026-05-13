from __future__ import annotations

import json
from datetime import date, datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from spark_jobs.common import create_spark_session, resolve_partition, write_parquet
from spark_jobs.compute_kpis import aggregate_genre_year_performance
from src.config.settings import ARTIFACTS_ROOT, build_path
from src.utils.file_utils import ensure_dir, write_text


def _normalize_sequence(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, tuple):
        return list(value)
    return value


def _normalize_scalar(value):
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.datetime64):
        return pd.Timestamp(value).to_pydatetime()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _normalize_record(record: dict) -> dict:
    normalized = {}
    for key, value in record.items():
        value = _normalize_sequence(value)
        value = _normalize_scalar(value)
        normalized[key] = value
    return normalized


def run(run_date: str | None = None) -> str:
    usage_partition = resolve_partition("usage", "ratings_boxoffice_analysis", "movie_performance_gap", run_date)
    spark = create_spark_session("train_ml_revenue_gap")
    dataframe = spark.read.parquet(str(usage_partition))
    pandas_df = dataframe.toPandas()

    numeric_features = [
        "budget",
        "runtime",
        "release_year",
        "tmdb_vote_average",
        "popularity",
        "imdb_score_100",
        "rt_score_100",
        "metacritic_score_100",
    ]
    categorical_features = ["main_genre"]
    all_features = numeric_features + categorical_features

    for column in all_features:
        if column not in pandas_df.columns:
            pandas_df[column] = np.nan

    train_mask = pandas_df["revenue"].fillna(0) > 0
    train_df = pandas_df.loc[train_mask].copy()

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numeric_features),
            (
                "cat",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_features,
            ),
        ]
    )

    if len(train_df) >= 8:
        regressor = RandomForestRegressor(
            n_estimators=200,
            min_samples_leaf=2,
            random_state=42,
        )
    else:
        regressor = DummyRegressor(strategy="median")

    pipeline = Pipeline([("preprocessor", preprocessor), ("model", regressor)])
    pipeline.fit(train_df[all_features], np.log1p(train_df["revenue"]))

    predictions = np.expm1(pipeline.predict(pandas_df[all_features]))
    predictions = np.clip(predictions, a_min=0, a_max=None)

    pandas_df["ml_expected_revenue"] = predictions
    pandas_df["ml_revenue_gap"] = pandas_df["revenue"].fillna(0) - pandas_df["ml_expected_revenue"]
    pandas_df["ml_gap_ratio"] = np.where(
        pandas_df["ml_expected_revenue"] > 0,
        pandas_df["revenue"].fillna(0) / pandas_df["ml_expected_revenue"],
        np.nan,
    )
    pandas_df["is_commercial_overperformer"] = pandas_df["ml_gap_ratio"] > 1.5
    pandas_df["is_commercial_underperformer"] = pandas_df["ml_gap_ratio"] < 0.5
    pandas_df["performance_category"] = np.where(
        pandas_df["is_commercial_overperformer"],
        "Commercial Overperformer",
        np.where(
            pandas_df["is_commercial_underperformer"],
            "Commercial Underperformer",
            pandas_df["performance_category"],
        ),
    )

    records = [
        _normalize_record(record)
        for record in pandas_df.replace({np.nan: None}).to_dict(orient="records")
    ]
    output_df = spark.createDataFrame(records, schema=dataframe.schema)
    output_path = build_path("usage", "ratings_boxoffice_analysis", "movie_performance_gap", usage_partition.name)
    result = write_parquet(output_df, output_path)

    aggregated = aggregate_genre_year_performance(output_df)
    aggregate_output_path = build_path("usage", "ratings_boxoffice_analysis", "genre_year_performance", usage_partition.name)
    write_parquet(aggregated, aggregate_output_path)

    model_dir = ARTIFACTS_ROOT / "models" / usage_partition.name
    ensure_dir(model_dir)
    joblib.dump(pipeline, model_dir / "revenue_gap_model.joblib")
    write_text(
        model_dir / "metrics.json",
        json.dumps(
            {
                "training_rows": int(len(train_df)),
                "prediction_rows": int(len(pandas_df)),
                "model_type": pipeline.named_steps["model"].__class__.__name__,
            },
            indent=2,
        ),
    )

    spark.stop()
    return result


if __name__ == "__main__":
    run()
