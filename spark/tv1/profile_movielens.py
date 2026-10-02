from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    countDistinct,
    min,
    max,
    avg,
    from_unixtime
)
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
    .appName("MovieLensProfiling")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Để chuyển timestamp nhất quán
spark.conf.set("spark.sql.session.timeZone", "UTC")


# =====================================================
# 2. HDFS
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


# =====================================================
# 4. Read data
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


# =====================================================
# 5. Ratings statistics
# =====================================================

print("\n========== RATINGS PROFILE ==========")

ratings_stats = ratings_df.agg(
    count("*").alias("total_ratings"),
    countDistinct("userId").alias("unique_users"),
    countDistinct("movieId").alias("rated_movies"),
    min("rating").alias("min_rating"),
    max("rating").alias("max_rating"),
    min("timestamp").alias("min_timestamp"),
    max("timestamp").alias("max_timestamp")
)

ratings_stats.show(truncate=False)


# =====================================================
# 6. Human-readable timestamp range
# =====================================================

print("\n========== TIME RANGE ==========")

ratings_df.select(
    from_unixtime(min("timestamp")).alias("first_rating_time"),
    from_unixtime(max("timestamp")).alias("last_rating_time")
).show(truncate=False)


# =====================================================
# 7. Movie catalog statistics
# =====================================================

print("\n========== MOVIE PROFILE ==========")

movies_df.agg(
    count("*").alias("total_movies"),
    countDistinct("movieId").alias("unique_movie_ids")
).show(truncate=False)


# =====================================================
# 8. Ratings per user
# =====================================================

print("\n========== RATINGS PER USER ==========")

ratings_per_user = (
    ratings_df
    .groupBy("userId")
    .count()
)

ratings_per_user.agg(
    min("count").alias("min_ratings_per_user"),
    max("count").alias("max_ratings_per_user"),
    avg("count").alias("avg_ratings_per_user")
).show(truncate=False)


# =====================================================
# 9. Ratings per movie
# =====================================================

print("\n========== RATINGS PER MOVIE ==========")

ratings_per_movie = (
    ratings_df
    .groupBy("movieId")
    .count()
)

ratings_per_movie.agg(
    min("count").alias("min_ratings_per_movie"),
    max("count").alias("max_ratings_per_movie"),
    avg("count").alias("avg_ratings_per_movie")
).show(truncate=False)


spark.stop()