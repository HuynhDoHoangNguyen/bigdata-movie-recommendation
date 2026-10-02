"""Distributed MovieLens analytics consumed by reports and TV4."""

from pyspark.sql import functions as F


def dataset_summary(ratings, movies):
    rating_stats = ratings.agg(
        F.count("*").alias("total_ratings"), F.countDistinct("userId").alias("total_users"),
        F.countDistinct("movieId").alias("rated_movies"), F.avg("rating").alias("average_rating"),
        F.min("rating").alias("min_rating"), F.max("rating").alias("max_rating"),
        F.min("timestamp").alias("first_rating_timestamp"), F.max("timestamp").alias("last_rating_timestamp"))
    return rating_stats.crossJoin(movies.agg(F.count("*").alias("catalog_movies"))).select(
        "total_ratings", "total_users", "catalog_movies", "rated_movies", "average_rating",
        "min_rating", "max_rating", "first_rating_timestamp", "last_rating_timestamp")


def rating_distribution(ratings):
    total = ratings.count()
    return (ratings.groupBy("rating").agg(F.count("*").alias("count"))
            .withColumn("percentage", F.col("count") * 100.0 / F.lit(total)).orderBy("rating"))


def user_activity(ratings):
    return ratings.groupBy("userId").agg(
        F.count("*").alias("rating_count"), F.avg("rating").alias("average_rating"))


def movie_popularity(ratings_movies, minimum):
    return (ratings_movies.groupBy("movieId", "title", "genres").agg(
        F.count("*").alias("rating_count"), F.avg("rating").alias("average_rating"))
        .withColumn("highly_rated_eligible", F.col("rating_count") >= F.lit(minimum)))


def genre_statistics(ratings_movies, movies):
    movie_genres = (movies.select("movieId", F.explode(F.split("genres", r"\|")).alias("genre"))
                    .groupBy("genre").agg(F.countDistinct("movieId").alias("movie_count")))
    rating_genres = (ratings_movies.select("rating", F.explode(F.split("genres", r"\|")).alias("genre"))
                     .groupBy("genre").agg(F.count("*").alias("rating_count"),
                                                  F.avg("rating").alias("average_rating")))
    return movie_genres.join(rating_genres, "genre", "left").fillna(0, ["rating_count"])


def rating_trend(ratings):
    return (ratings.withColumn("year", F.year(F.from_unixtime("timestamp")))
            .groupBy("year").agg(F.count("*").alias("rating_count"),
                                  F.avg("rating").alias("average_rating")).orderBy("year"))


def tag_statistics(tags):
    normalized = tags.select(F.lower(F.trim("tag")).alias("tag")).where(
        F.col("tag").isNotNull() & (F.length("tag") > 0))
    return normalized.groupBy("tag").agg(F.count("*").alias("tag_count"))
