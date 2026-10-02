from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    LongType,
)


# ==========================================
# 1. SparkSession
# ==========================================

spark = (
    SparkSession.builder
    .appName("MovieLensDataQuality")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 2. Schema ratings
# ==========================================

ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])


# ==========================================
# 3. Đọc ratings từ HDFS
# ==========================================

ratings_path = (
    "hdfs://namenode:9000"
    "/project/movielens/raw/ratings.csv"
)

ratings_df = (
    spark.read
    .option("header", "true")
    .schema(ratings_schema)
    .csv(ratings_path)
)


# ==========================================
# 4. Tổng số dòng
# ==========================================

total_rows = ratings_df.count()

print("\n========== TOTAL ROWS ==========")
print("Total ratings:", total_rows)


# ==========================================
# 5. Kiểm tra NULL
# ==========================================

print("\n========== NULL CHECK ==========")

ratings_df.select([
    spark_sum(
        col(c).isNull().cast("int")
    ).alias(c)
    for c in ratings_df.columns
]).show()


# ==========================================
# 6. Exact duplicate
# ==========================================

print("\n========== EXACT DUPLICATES ==========")

distinct_rows = ratings_df.distinct().count()

exact_duplicates = total_rows - distinct_rows

print("Total rows      :", total_rows)
print("Distinct rows   :", distinct_rows)
print("Exact duplicates:", exact_duplicates)


# ==========================================
# 7. Duplicate userId + movieId
# ==========================================

print("\n========== DUPLICATE USER-MOVIE PAIRS ==========")

duplicate_pairs = (
    ratings_df
    .groupBy("userId", "movieId")
    .count()
    .filter(col("count") > 1)
)

duplicate_pair_count = duplicate_pairs.count()

print("Duplicate userId-movieId pairs:", duplicate_pair_count)

duplicate_pairs.show(10, truncate=False)


# ==========================================
# 8. Invalid IDs
# ==========================================

print("\n========== INVALID IDS ==========")

invalid_user_ids = ratings_df.filter(
    col("userId").isNull() | (col("userId") <= 0)
).count()

invalid_movie_ids = ratings_df.filter(
    col("movieId").isNull() | (col("movieId") <= 0)
).count()

print("Invalid userId rows :", invalid_user_ids)
print("Invalid movieId rows:", invalid_movie_ids)


# ==========================================
# 9. Invalid ratings
# ==========================================

print("\n========== INVALID RATINGS ==========")

invalid_ratings = ratings_df.filter(
    col("rating").isNull()
    | (col("rating") < 0.5)
    | (col("rating") > 5.0)
)

invalid_rating_count = invalid_ratings.count()

print("Invalid rating rows:", invalid_rating_count)

invalid_ratings.show(10, truncate=False)


spark.stop()
