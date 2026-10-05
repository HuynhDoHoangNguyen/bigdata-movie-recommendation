"""Read-only runtime audit of recovered TV1/TV2 inputs; never fit ALS."""

import json

from pyspark.sql import SparkSession, functions as F

from load_inputs import load_and_validate_inputs


def main():
    spark = SparkSession.builder.appName("TV3RecoveryInputGate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Historical counts are references, not constraints on a new runtime.
        train, test, movies, validations = load_and_validate_inputs(
            spark, verify_historical_counts=False
        )
        overlap = train.join(test, ["userId", "movieId", "rating"], "inner").count()
        pair_overlap = train.select("userId", "movieId").join(
            test.select("userId", "movieId"), ["userId", "movieId"], "inner"
        ).count()
        train_users = train.select("userId").distinct()
        train_movies = train.select("movieId").distinct()
        unknown_users = test.select("userId").distinct().join(
            train_users, "userId", "left_anti"
        ).count()
        unknown_movies = test.select("movieId").distinct().join(
            train_movies, "movieId", "left_anti"
        ).count()
        unknown_movie_rows = test.join(train_movies, "movieId", "left_anti").count()
        affected_rows = test.join(
            train_users.withColumn("_known_user", F.lit(True)), "userId", "left"
        ).join(
            train_movies.withColumn("_known_movie", F.lit(True)), "movieId", "left"
        ).where(F.col("_known_user").isNull() | F.col("_known_movie").isNull()).count()
        if overlap or pair_overlap or not validations["movies"]["row_count"]:
            raise ValueError("Input gate failed: overlap or empty metadata")
        print(json.dumps({
            "spark_input_gate": "PASS", "spark_version": spark.version,
            "validations": validations, "overlap_count": overlap,
            "pair_overlap_count": pair_overlap,
            "test_unknown_users": unknown_users,
            "test_unknown_movies": unknown_movies,
            "test_unknown_movie_rows": unknown_movie_rows,
            "affected_test_rows": affected_rows,
            "note": "Overall gate also requires external HDFS fsck/report PASS.",
        }, indent=2), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
