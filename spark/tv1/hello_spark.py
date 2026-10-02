from pyspark.sql import SparkSession

# 1. Tạo SparkSession
spark = (
    SparkSession.builder
    .appName("SparkClusterTest")
    .getOrCreate()
)

# Giảm log để dễ nhìn kết quả
spark.sparkContext.setLogLevel("WARN")


# 2. In thông tin Spark hiện tại
print("\n========== SPARK INFORMATION ==========")
print("Spark version:", spark.version)
print("Application ID:", spark.sparkContext.applicationId)
print("Master:", spark.sparkContext.master)
print("Default parallelism:", spark.sparkContext.defaultParallelism)


# 3. Tạo DataFrame phân tán đơn giản
df = spark.range(
    start=1,
    end=1000001,
    step=1,
    numPartitions=4
)


print("\n========== DATAFRAME ==========")

print("Number of partitions:")
print(df.rdd.getNumPartitions())

print("\nFirst 10 rows:")
df.show(10)


# 4. Thực hiện một Action
count = df.count()

print("\n========== RESULT ==========")
print("Total rows:", count)


# 5. Dừng SparkSession
spark.stop()