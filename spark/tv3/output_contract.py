"""TV3 write boundaries and read-back data checks shared by sample jobs."""

import json
from urllib.parse import urlsplit

from pyspark.sql import functions as F, types as T


def validate_output_path(path):
    parsed = urlsplit(path)
    root = "/project/movielens/output/tv3"
    if (parsed.scheme not in ("", "hdfs") or parsed.query or parsed.fragment
            or (parsed.scheme == "hdfs" and parsed.netloc != "namenode:9000")
            or (not parsed.scheme and parsed.netloc)
            or not parsed.path.startswith(root + "/")
            or any(part in ("", ".", "..") for part in parsed.path.split("/")[1:])
            or "%" in parsed.path or "\\" in parsed.path):
        raise ValueError(f"Output must be a child of {root}: {path}")
    return path


def path_exists(spark, path):
    hpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return hpath.getFileSystem(spark._jsc.hadoopConfiguration()).exists(hpath)


def validate_recommendation_path(path):
    validate_output_path(path)
    if not urlsplit(path).path.startswith("/project/movielens/output/tv3/recommendations/"):
        raise ValueError("Recommendation CLI writes only inside TV3/recommendations/")
    return path


def require_new_outputs(spark, paths):
    for path in paths:
        validate_output_path(path)
        if path_exists(spark, path):
            raise ValueError(f"Refusing to overwrite existing output: {path}")


def read_manifest(spark, path):
    rows = spark.read.text(path).take(2)
    if len(rows) != 1:
        raise ValueError("Manifest must contain exactly one JSON row")
    return json.loads(rows[0][0])


def write_manifest(spark, payload, path, overwrite=False):
    validate_output_path(path)
    spark.createDataFrame([(json.dumps(payload, sort_keys=True),)], "json STRING").coalesce(
        1
    ).write.mode("overwrite" if overwrite else "errorifexists").text(path)


def check_factors(model):
    result = {}
    for name, factors in (("users", model.userFactors), ("items", model.itemFactors)):
        summary = factors.agg(
            F.count("*").alias("rows"), F.countDistinct("id").alias("distinct_ids"),
            F.sum((F.col("id").isNull() | (F.col("id") <= 0)
                   | F.col("features").isNull() | (F.size("features") != model.rank)
                   | F.expr("exists(features, x -> x IS NULL OR isnan(x) "
                            "OR abs(x) = cast('Infinity' as float))")).cast("long")
            ).alias("invalid_rows"),
        ).first().asDict()
        if (not summary["rows"] or summary["rows"] != summary["distinct_ids"]
                or summary["invalid_rows"]):
            raise ValueError(f"Invalid {name} factors: {summary}")
        result[name] = summary
    return result


def cold_start_stats(model, test):
    known_users = model.userFactors.select(F.col("id").alias("userId"))
    known_items = model.itemFactors.select(F.col("id").alias("movieId"))
    unknown_users = test.join(known_users, "userId", "left_anti")
    unknown_items = test.join(known_items, "movieId", "left_anti")
    affected = test.join(known_users.withColumn("_user", F.lit(True)), "userId", "left").join(
        known_items.withColumn("_item", F.lit(True)), "movieId", "left"
    ).where(F.col("_user").isNull() | F.col("_item").isNull()).count()
    return {"unknown_users": unknown_users.select("userId").distinct().count(),
            "unknown_movies": unknown_items.select("movieId").distinct().count(),
            "affected_test_rows": affected}


def check_recommendations(frame, train, movies, users, top_n):
    expected = {"userId": T.IntegerType(), "movieId": T.IntegerType(),
                "title": T.StringType(), "genres": T.StringType(),
                "prediction": T.DoubleType(), "rank": T.IntegerType()}
    if frame.columns != list(expected) or any(
        frame.schema[name].dataType != dtype for name, dtype in expected.items()
    ):
        raise ValueError(f"Recommendation schema mismatch: {frame.schema.simpleString()}")
    invalid = F.lit(False)
    for column in expected:
        invalid = invalid | F.col(column).isNull()
    invalid = invalid | F.isnan("prediction") | (F.abs("prediction") == float("inf"))
    invalid = invalid | (F.col("rank") < 1) | (F.col("rank") > top_n)
    result = {"rows": frame.count(), "users": frame.select("userId").distinct().count(),
              "invalid_rows": frame.where(invalid).count(),
              "duplicate_pairs": frame.groupBy("userId", "movieId").count().where("count > 1").count(),
              "duplicate_ranks": frame.groupBy("userId", "rank").count().where("count > 1").count(),
              "train_history_overlap": frame.join(train, ["userId", "movieId"], "inner").count(),
              "unexpected_users": frame.join(users, "userId", "left_anti").count()}
    joined = frame.alias("r").join(movies.alias("m"), "movieId", "left")
    result["metadata_mismatches"] = joined.where(
        F.col("m.movieId").isNull() | ~F.col("r.title").eqNullSafe(F.col("m.title"))
        | ~F.col("r.genres").eqNullSafe(F.col("m.genres"))
    ).count()
    if not result["rows"] or any(result[name] for name in result if name not in ("rows", "users")):
        raise ValueError(f"Recommendation contract failed: {result}")
    return result
