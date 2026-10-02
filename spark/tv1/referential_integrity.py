from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    LongType,
    StringType,
)


# =====================================================
# 1. SparkSession
# =====================================================

spark = (
    SparkSession.builder
    .appName("MovieLensReferentialIntegrity")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =====================================================
# 2. HDFS base path
# =====================================================

BASE_PATH = "hdfs://namenode:9000/project/movielens/raw"


# =====================================================
# 3. Schemas
# =====================================================

ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])

movies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
])

tags_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("tag", StringType(), True),
    StructField("timestamp", LongType(), True),
])

links_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("imdbId", StringType(), True),
    StructField("tmdbId", IntegerType(), True),
])


# =====================================================
# 4. Read HDFS data
# =====================================================

ratings_df = (
    spark.read
    .option("header", "true")
    .schema(ratings_schema)
    .csv(f"{BASE_PATH}/ratings.csv")
)

movies_df = (
    spark.read
    .option("header", "true")
    .schema(movies_schema)
    .csv(f"{BASE_PATH}/movies.csv")
)

tags_df = (
    spark.read
    .option("header", "true")
    .schema(tags_schema)
    .csv(f"{BASE_PATH}/tags.csv")
)

links_df = (
    spark.read
    .option("header", "true")
    .schema(links_schema)
    .csv(f"{BASE_PATH}/links.csv")
)


# =====================================================
# 5. Duplicate movieId in movies
# =====================================================

print("\n========== DUPLICATE MOVIE IDS ==========")

duplicate_movie_ids = (
    movies_df
    .groupBy("movieId")
    .count()
    .filter(col("count") > 1)
)

print(
    "Duplicate movieId in movies:",
    duplicate_movie_ids.count()
)


# =====================================================
# 6. ratings.movieId -> movies.movieId
# =====================================================

print("\n========== RATINGS -> MOVIES ==========")

invalid_rating_movies = (
    ratings_df
    .select("movieId")
    .distinct()
    .join(
        movies_df.select("movieId"),
        on="movieId",
        how="left_anti"
    )
)

print(
    "movieId in ratings but missing from movies:",
    invalid_rating_movies.count()
)

invalid_rating_movies.show(10, truncate=False)


# =====================================================
# 7. tags.movieId -> movies.movieId
# =====================================================

print("\n========== TAGS -> MOVIES ==========")

invalid_tag_movies = (
    tags_df
    .select("movieId")
    .distinct()
    .join(
        movies_df.select("movieId"),
        on="movieId",
        how="left_anti"
    )
)

print(
    "movieId in tags but missing from movies:",
    invalid_tag_movies.count()
)

invalid_tag_movies.show(10, truncate=False)


# =====================================================
# 8. links.movieId -> movies.movieId
# =====================================================

print("\n========== LINKS -> MOVIES ==========")

invalid_link_movies = (
    links_df
    .select("movieId")
    .distinct()
    .join(
        movies_df.select("movieId"),
        on="movieId",
        how="left_anti"
    )
)

print(
    "movieId in links but missing from movies:",
    invalid_link_movies.count()
)


# =====================================================
# 9. movies missing links
# =====================================================

print("\n========== MOVIES -> LINKS ==========")

movies_without_links = (
    movies_df
    .select("movieId")
    .join(
        links_df.select("movieId"),
        on="movieId",
        how="left_anti"
    )
)

print(
    "Movies without links record:",
    movies_without_links.count()
)


# =====================================================
# 10. Rating granularity
# =====================================================

print("\n========== RATING STEP CHECK ==========")

valid_ratings = [
    0.5, 1.0, 1.5, 2.0, 2.5,
    3.0, 3.5, 4.0, 4.5, 5.0
]

invalid_rating_steps = (
    ratings_df
    .filter(~col("rating").isin(valid_ratings))
)

print(
    "Ratings not following 0.5 step:",
    invalid_rating_steps.count()
)


spark.stop()