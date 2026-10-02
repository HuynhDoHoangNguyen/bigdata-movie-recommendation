from pyspark.sql import SparkSession
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
    .appName("MovieLensBuildStandardLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =====================================================
# 2. HDFS paths
# =====================================================

RAW_PATH = "hdfs://namenode:9000/project/movielens/raw"
STANDARD_PATH = "hdfs://namenode:9000/project/movielens/standard"


# =====================================================
# 3. Explicit schemas
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
# 4. Read RAW CSV
# =====================================================

ratings_df = (
    spark.read
    .option("header", "true")
    .schema(ratings_schema)
    .csv(f"{RAW_PATH}/ratings.csv")
)

movies_df = (
    spark.read
    .option("header", "true")
    .schema(movies_schema)
    .csv(f"{RAW_PATH}/movies.csv")
)

tags_df = (
    spark.read
    .option("header", "true")
    .schema(tags_schema)
    .csv(f"{RAW_PATH}/tags.csv")
)

links_df = (
    spark.read
    .option("header", "true")
    .schema(links_schema)
    .csv(f"{RAW_PATH}/links.csv")
)


# =====================================================
# 5. Write STANDARD layer as Parquet
# =====================================================

print("\n========== WRITING RATINGS ==========")

ratings_df.write \
    .mode("overwrite") \
    .parquet(f"{STANDARD_PATH}/ratings")


print("\n========== WRITING MOVIES ==========")

movies_df.write \
    .mode("overwrite") \
    .parquet(f"{STANDARD_PATH}/movies")


print("\n========== WRITING TAGS ==========")

tags_df.write \
    .mode("overwrite") \
    .parquet(f"{STANDARD_PATH}/tags")


print("\n========== WRITING LINKS ==========")

links_df.write \
    .mode("overwrite") \
    .parquet(f"{STANDARD_PATH}/links")


print("\n========== STANDARD LAYER CREATED ==========")


# =====================================================
# 6. Read back for verification
# =====================================================

standard_ratings = spark.read.parquet(
    f"{STANDARD_PATH}/ratings"
)

standard_movies = spark.read.parquet(
    f"{STANDARD_PATH}/movies"
)

standard_tags = spark.read.parquet(
    f"{STANDARD_PATH}/tags"
)

standard_links = spark.read.parquet(
    f"{STANDARD_PATH}/links"
)


print("\n========== VERIFY STANDARD COUNTS ==========")

print("Ratings :", standard_ratings.count())
print("Movies  :", standard_movies.count())
print("Tags    :", standard_tags.count())
print("Links   :", standard_links.count())


print("\n========== STANDARD RATINGS SCHEMA ==========")

standard_ratings.printSchema()


spark.stop()