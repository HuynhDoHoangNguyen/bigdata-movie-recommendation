"""TV2 transformations for analytics and ALS preparation."""

from pyspark.sql import functions as F


def preprocess_ratings(ratings):
    before = ratings.count()
    processed = ratings.select(
        F.col("userId").cast("int").alias("userId"), F.col("movieId").cast("int").alias("movieId"),
        F.col("rating").cast("double").alias("rating"), F.col("timestamp").cast("long").alias("timestamp"))
    after = processed.count()
    return processed, {"step": "select_and_enforce_standard_columns", "before_count": before,
                       "removed_count": before - after, "after_count": after,
                       "reason": "No filter required; TV1 Standard passed boundary validation"}


def join_ratings_movies(ratings, movies):
    ratings_count = ratings.count()
    joined = ratings.join(F.broadcast(movies), "movieId", "inner")
    joined_count = joined.count()
    if joined_count != ratings_count:
        raise ValueError(f"ratings-movies join lost rows: {ratings_count} -> {joined_count}")
    return joined, {"step": "inner_join_ratings_movies", "before_count": ratings_count,
                    "removed_count": ratings_count - joined_count, "after_count": joined_count,
                    "reason": "Attach title and genres for analytics using movieId"}


def create_als_dataset(ratings):
    return ratings.select("userId", "movieId", "rating")
