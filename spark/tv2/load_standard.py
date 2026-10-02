"""Load and validate the TV1 Standard Layer contract."""

from pyspark.sql import functions as F, types as T
from config import EXPECTED_COUNTS, STANDARD_PATHS

EXPECTED_SCHEMAS = {
    "ratings": {"userId": T.IntegerType(), "movieId": T.IntegerType(),
                "rating": T.DoubleType(), "timestamp": T.LongType()},
    "movies": {"movieId": T.IntegerType(), "title": T.StringType(), "genres": T.StringType()},
    "tags": {"userId": T.IntegerType(), "movieId": T.IntegerType(),
             "tag": T.StringType(), "timestamp": T.LongType()},
    "links": {"movieId": T.IntegerType(), "imdbId": T.StringType(), "tmdbId": T.IntegerType()},
}


def load_standard(spark, include_optional=True):
    names = ["ratings", "movies"] + (["tags", "links"] if include_optional else [])
    return {name: spark.read.parquet(STANDARD_PATHS[name]) for name in names}


def validate_contract(name, frame, verify_expected_count=True):
    expected = EXPECTED_SCHEMAS[name]
    actual = {field.name: field.dataType for field in frame.schema.fields}
    missing = sorted(set(expected) - set(actual))
    wrong_types = {column: {"expected": str(dtype), "actual": str(actual.get(column))}
                   for column, dtype in expected.items()
                   if column in actual and actual[column] != dtype}
    if missing or wrong_types:
        raise ValueError(f"{name} contract failed: missing={missing}, wrong_types={wrong_types}")
    row_count = frame.count()
    if not row_count:
        raise ValueError(f"{name} Standard dataset is empty")
    if verify_expected_count and row_count != EXPECTED_COUNTS[name]:
        raise ValueError(f"{name} count mismatch: expected={EXPECTED_COUNTS[name]}, actual={row_count}")
    invalid_count = 0
    if name == "ratings":
        invalid_count = frame.where(
            F.col("userId").isNull() | F.col("movieId").isNull() | F.col("rating").isNull()
            | (F.col("rating") < 0.5) | (F.col("rating") > 5.0)).count()
    elif name == "movies":
        invalid_count = frame.where(
            F.col("movieId").isNull() | F.col("title").isNull() | F.col("genres").isNull()).count()
    if invalid_count:
        raise ValueError(f"{name} boundary validation found {invalid_count} invalid rows")
    return {"dataset": name, "row_count": row_count, "schema": frame.schema.simpleString(),
            "invalid_boundary_rows": invalid_count, "status": "PASS"}
