"""Load and enforce the TV2 -> TV3 input contract."""

from pyspark.sql import functions as F, types as T

from config import (
    EXPECTED_ALS_COLUMNS,
    EXPECTED_MOVIE_COLUMNS,
    EXPECTED_TEST_COUNT,
    EXPECTED_TRAIN_COUNT,
    MOVIES_PATH,
    TEST_PATH,
    TRAIN_PATH,
)


ALS_TYPES = {
    "userId": T.IntegerType(),
    "movieId": T.IntegerType(),
    "rating": T.DoubleType(),
}
MOVIE_TYPES = {
    "movieId": T.IntegerType(),
    "title": T.StringType(),
    "genres": T.StringType(),
}


def _validate_schema(frame, expected_columns, expected_types, name):
    if frame.columns != expected_columns:
        raise ValueError(f"{name} columns mismatch: {frame.columns} != {expected_columns}")
    actual = {field.name: field.dataType for field in frame.schema.fields}
    wrong = {
        column: {"expected": str(dtype), "actual": str(actual.get(column))}
        for column, dtype in expected_types.items()
        if actual.get(column) != dtype
    }
    if wrong:
        raise ValueError(f"{name} types mismatch: {wrong}")


def _validate_als_rows(frame, name, expected_count=None):
    summary = frame.agg(
        F.count("*").alias("row_count"),
        F.sum(
            (
                F.col("userId").isNull()
                | F.col("movieId").isNull()
                | F.col("rating").isNull()
            ).cast("long")
        ).alias("null_rows"),
        F.sum(
            ((F.col("rating") < 0.5) | (F.col("rating") > 5.0)).cast("long")
        ).alias("invalid_rating_rows"),
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
    ).first().asDict()
    summary = {key: (0 if value is None and key.endswith("_rows") else value)
               for key, value in summary.items()}
    if not summary["row_count"]:
        raise ValueError(f"{name} is empty")
    if summary["null_rows"] or summary["invalid_rating_rows"]:
        raise ValueError(f"{name} contains invalid rows: {summary}")
    if expected_count is not None and summary["row_count"] != expected_count:
        raise ValueError(
            f"{name} count mismatch: expected={expected_count}, actual={summary['row_count']}"
        )
    summary.update({"dataset": name, "status": "PASS"})
    return summary


def load_and_validate_inputs(spark, verify_historical_counts=True):
    print("Loading TV2 ALS train/test and TV1 movie metadata", flush=True)
    train = spark.read.parquet(TRAIN_PATH).select(*EXPECTED_ALS_COLUMNS)
    test = spark.read.parquet(TEST_PATH).select(*EXPECTED_ALS_COLUMNS)
    movies = spark.read.parquet(MOVIES_PATH).select(*EXPECTED_MOVIE_COLUMNS)

    _validate_schema(train, EXPECTED_ALS_COLUMNS, ALS_TYPES, "train")
    _validate_schema(test, EXPECTED_ALS_COLUMNS, ALS_TYPES, "test")
    _validate_schema(movies, EXPECTED_MOVIE_COLUMNS, MOVIE_TYPES, "movies")

    expected_train = EXPECTED_TRAIN_COUNT if verify_historical_counts else None
    expected_test = EXPECTED_TEST_COUNT if verify_historical_counts else None
    validations = {
        "train": _validate_als_rows(train, "train", expected_train),
        "test": _validate_als_rows(test, "test", expected_test),
    }
    movie_summary = movies.agg(
        F.count("*").alias("row_count"),
        F.sum(
            (F.col("movieId").isNull() | F.col("title").isNull() | F.col("genres").isNull())
            .cast("long")
        ).alias("null_rows"),
        F.countDistinct("movieId").alias("distinct_movie_ids"),
    ).first().asDict()
    movie_summary["null_rows"] = movie_summary["null_rows"] or 0
    if movie_summary["null_rows"] or movie_summary["row_count"] != movie_summary["distinct_movie_ids"]:
        raise ValueError(f"movies metadata contract failed: {movie_summary}")
    movie_summary.update({"dataset": "movies", "status": "PASS"})
    validations["movies"] = movie_summary
    return train, test, movies, validations
