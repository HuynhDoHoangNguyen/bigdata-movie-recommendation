"""Reproducible per-user interaction split and leakage/cold-start checks."""

from pyspark.sql import Window, functions as F


def split_train_test(als_data, train_ratio, seed):
    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio must be between 0 and 1")
    counts = Window.partitionBy("userId")
    order = Window.partitionBy("userId").orderBy(
        F.xxhash64("userId", "movieId", F.lit(seed)), "movieId")
    assigned = (als_data.withColumn("_count", F.count("*").over(counts))
                .withColumn("_position", F.row_number().over(order))
                .withColumn("_train_size", F.greatest(F.lit(1), F.floor(F.col("_count") * train_ratio)))
                .withColumn("split", F.when(F.col("_position") <= F.col("_train_size"), "train")
                            .otherwise("test")).drop("_count", "_position", "_train_size"))
    return assigned.where("split = 'train'").drop("split"), assigned.where("split = 'test'").drop("split")


def split_statistics(train, test):
    train_count, test_count = train.count(), test.count()
    overlap = train.join(test, ["userId", "movieId", "rating"], "inner").count()
    unknown_users = test.select("userId").distinct().join(
        train.select("userId").distinct(), "userId", "left_anti").count()
    unknown_movies = test.select("movieId").distinct().join(
        train.select("movieId").distinct(), "movieId", "left_anti").count()
    unknown_movie_rows = test.join(train.select("movieId").distinct(), "movieId", "left_anti").count()
    if overlap:
        raise ValueError(f"Train/test leakage detected: {overlap} overlapping rows")
    return {"train_count": train_count, "test_count": test_count,
            "actual_train_ratio": train_count / (train_count + test_count),
            "overlap_count": overlap, "test_unknown_users": unknown_users,
            "test_unknown_movies": unknown_movies, "test_unknown_movie_rows": unknown_movie_rows}
