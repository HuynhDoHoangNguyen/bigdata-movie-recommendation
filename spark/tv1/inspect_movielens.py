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
    .appName("MovieLensInspection")
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

    # Giữ IMDb ID là String để không mất số 0 đầu
    StructField("imdbId", StringType(), True),

    # TMDB có thể có giá trị null
    StructField("tmdbId", IntegerType(), True),
])


# =====================================================
# 4. Đọc từ HDFS
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
# 5. In schema
# =====================================================

print("\n========== RATINGS SCHEMA ==========")
ratings_df.printSchema()

print("\n========== MOVIES SCHEMA ==========")
movies_df.printSchema()

print("\n========== TAGS SCHEMA ==========")
tags_df.printSchema()

print("\n========== LINKS SCHEMA ==========")
links_df.printSchema()


# =====================================================
# 6. Đếm số dòng
# =====================================================

print("\n========== DATASET SIZE ==========")

print("Ratings :", ratings_df.count())
print("Movies  :", movies_df.count())
print("Tags    :", tags_df.count())
print("Links   :", links_df.count())


# =====================================================
# 7. Xem sample
# =====================================================

print("\n========== RATINGS SAMPLE ==========")
ratings_df.show(5, truncate=False)

print("\n========== MOVIES SAMPLE ==========")
movies_df.show(5, truncate=False)

print("\n========== TAGS SAMPLE ==========")
tags_df.show(5, truncate=False)

print("\n========== LINKS SAMPLE ==========")
links_df.show(5, truncate=False)


spark.stop()