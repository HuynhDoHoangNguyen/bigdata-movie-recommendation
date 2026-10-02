"""End-to-end TV2 Data Processing & Analytics pipeline."""

import json
from datetime import datetime, timezone
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from analytics import (dataset_summary, genre_statistics, movie_popularity,
                       rating_distribution, rating_trend, tag_statistics, user_activity)
from config import (ALS_PATHS, ANALYTICS_PATHS, HIGHLY_RATED_MIN_COUNT, MANIFEST_PATH,
                    OUTPUT_BASE, PROCESSED_PATHS, SEED, TRAIN_RATIO)
from load_standard import load_standard, validate_contract
from preprocess import create_als_dataset, join_ratings_movies, preprocess_ratings
from split_data import split_statistics, split_train_test


def write_parquet(frame, path):
    frame.write.mode("overwrite").option("compression", "snappy").parquet(path)


def verify_output(spark, path, expected_columns, expected_count=None):
    frame = spark.read.parquet(path)
    if frame.columns != expected_columns:
        raise ValueError(f"Output columns mismatch at {path}: {frame.columns}")
    count = frame.count()
    if expected_count is not None and count != expected_count:
        raise ValueError(f"Output count mismatch at {path}: {count} != {expected_count}")
    return count


def main():
    spark = SparkSession.builder.appName("MovieLensTV2ProcessingAnalytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    cached = []
    manifest = {"status": "RUNNING", "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "spark_version": spark.version, "input_layer": "TV1 HDFS STANDARD Parquet",
                "output_base": OUTPUT_BASE, "seed": SEED,
                "split_method": "per-user deterministic interaction hash",
                "requested_train_ratio": TRAIN_RATIO,
                "filter_rules": "None; Standard data passed the TV2 boundary contract",
                "highly_rated_min_count": HIGHLY_RATED_MIN_COUNT}
    try:
        print("[1/7] Loading TV1 Standard Layer", flush=True)
        frames = load_standard(spark, include_optional=True)
        ratings = frames["ratings"].persist(StorageLevel.MEMORY_AND_DISK)
        movies = frames["movies"].persist(StorageLevel.MEMORY_AND_DISK)
        tags = frames["tags"]
        cached.extend([ratings, movies])
        print("[2/7] Validating TV1 -> TV2 input contract", flush=True)
        manifest["input_validation"] = [validate_contract(name, frames[name])
                                        for name in ("ratings", "movies", "tags", "links")]
        print("[3/7] Preprocessing and joining metadata", flush=True)
        processed, process_step = preprocess_ratings(ratings)
        processed = processed.persist(StorageLevel.MEMORY_AND_DISK); cached.append(processed)
        ratings_movies, join_step = join_ratings_movies(processed, movies)
        ratings_movies = ratings_movies.persist(StorageLevel.MEMORY_AND_DISK); cached.append(ratings_movies)
        manifest["processing"] = [process_step, join_step]
        write_parquet(processed, PROCESSED_PATHS["ratings"])
        write_parquet(ratings_movies, PROCESSED_PATHS["ratings_movies"])
        print("[4/7] Building distributed analytics", flush=True)
        outputs = {
            "dataset_summary": dataset_summary(processed, movies),
            "rating_distribution": rating_distribution(processed),
            "user_activity": user_activity(processed),
            "movie_popularity": movie_popularity(ratings_movies, HIGHLY_RATED_MIN_COUNT),
            "genre_statistics": genre_statistics(ratings_movies, movies),
            "rating_trend": rating_trend(processed), "tag_statistics": tag_statistics(tags)}
        manifest["analytics_counts"] = {}
        for name, frame in outputs.items():
            write_parquet(frame, ANALYTICS_PATHS[name])
            manifest["analytics_counts"][name] = verify_output(spark, ANALYTICS_PATHS[name], frame.columns)
        print("[5/7] Creating ALS dataset and deterministic Train/Test", flush=True)
        als_data = create_als_dataset(processed)
        train, test = split_train_test(als_data, TRAIN_RATIO, SEED)
        train = train.persist(StorageLevel.MEMORY_AND_DISK); test = test.persist(StorageLevel.MEMORY_AND_DISK)
        cached.extend([train, test])
        manifest["split"] = split_statistics(train, test)
        manifest["split"].update({"seed": SEED, "method": "per-user deterministic interaction hash",
            "train_rule": "first floor(80% * user interactions), minimum 1, by seeded xxhash64",
            "test_rule": "remaining interactions", "schema": als_data.schema.simpleString()})
        write_parquet(train, ALS_PATHS["train"]); write_parquet(test, ALS_PATHS["test"])
        print("[6/7] Reading outputs back from HDFS", flush=True)
        manifest["read_back"] = {
            "processed_ratings": verify_output(spark, PROCESSED_PATHS["ratings"], processed.columns, process_step["after_count"]),
            "ratings_movies": verify_output(spark, PROCESSED_PATHS["ratings_movies"], ratings_movies.columns, join_step["after_count"]),
            "train": verify_output(spark, ALS_PATHS["train"], ["userId", "movieId", "rating"], manifest["split"]["train_count"]),
            "test": verify_output(spark, ALS_PATHS["test"], ["userId", "movieId", "rating"], manifest["split"]["test_count"])}
        manifest["summary"] = spark.read.parquet(ANALYTICS_PATHS["dataset_summary"]).first().asDict()
        manifest["rating_distribution"] = [row.asDict() for row in spark.read.parquet(
            ANALYTICS_PATHS["rating_distribution"]).orderBy("rating").collect()]
        manifest["user_activity_summary"] = spark.read.parquet(ANALYTICS_PATHS["user_activity"]).agg(
            F.min("rating_count").alias("min_ratings_per_user"),
            F.max("rating_count").alias("max_ratings_per_user"),
            F.avg("rating_count").alias("avg_ratings_per_user")).first().asDict()
        manifest["status"] = "PASS"
        print("[7/7] Writing manifest", flush=True)
        manifest_json = json.dumps(manifest, ensure_ascii=False, sort_keys=True)
        spark.createDataFrame([(manifest_json,)], "manifest_json STRING").write.mode("overwrite").text(MANIFEST_PATH)
        if json.loads(spark.read.text(MANIFEST_PATH).first()[0])["status"] != "PASS":
            raise ValueError("Manifest read-back verification failed")
        print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
        print("TV2 PIPELINE PASSED", flush=True)
    finally:
        for frame in reversed(cached): frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
