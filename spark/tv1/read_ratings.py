from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    LongType,
)


# ==============================
# 1. Tạo SparkSession
# ==============================
spark = (
    SparkSession.builder
    .appName("MovieLensReadRatings")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==============================
# 2. Khai báo schema
# ==============================
ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])


# ==============================
# 3. Đường dẫn HDFS
# ==============================
ratings_path = (
    "hdfs://namenode:9000"
    "/project/movielens/raw/ratings.csv"
)


# ==============================
# 4. Đọc CSV từ HDFS
# ==============================
ratings_df = (
    spark.read
    .option("header", "true")
    .schema(ratings_schema)
    .csv(ratings_path)
)


# ==============================
# 5. Kiểm tra DataFrame
# ==============================
print("\n========== RATINGS SCHEMA ==========")
ratings_df.printSchema()


print("\n========== NUMBER OF PARTITIONS ==========")
print(ratings_df.rdd.getNumPartitions())


print("\n========== FIRST 10 ROWS ==========")
ratings_df.show(10, truncate=False)


print("\n========== TOTAL RATINGS ==========")
total_ratings = ratings_df.count()
print("Total ratings:", total_ratings)


# ==============================
# 6. Dừng Spark
# ==============================
spark.stop() 