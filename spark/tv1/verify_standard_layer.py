from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("VerifyMovieLensStandardLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


BASE_PATH = "hdfs://namenode:9000/project/movielens/standard"


datasets = [
    "ratings",
    "movies",
    "tags",
    "links",
]


for name in datasets:

    print(f"\n========== {name.upper()} ==========")

    df = spark.read.parquet(
        f"{BASE_PATH}/{name}"
    )

    print("Rows:", df.count())

    df.printSchema()

    df.show(5, truncate=False)


spark.stop()