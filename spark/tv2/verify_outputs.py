"""Independent read-back verification for TV2 HDFS deliverables."""

import json

from pyspark.sql import SparkSession

from config import ALS_PATHS, ANALYTICS_PATHS, MANIFEST_PATH, PROCESSED_PATHS


EXPECTED_COLUMNS = {
    PROCESSED_PATHS["ratings"]: ["userId", "movieId", "rating", "timestamp"],
    PROCESSED_PATHS["ratings_movies"]: ["movieId", "userId", "rating", "timestamp", "title", "genres"],
    ALS_PATHS["train"]: ["userId", "movieId", "rating"],
    ALS_PATHS["test"]: ["userId", "movieId", "rating"],
    ANALYTICS_PATHS["dataset_summary"]: ["total_ratings", "total_users", "catalog_movies", "rated_movies", "average_rating", "min_rating", "max_rating", "first_rating_timestamp", "last_rating_timestamp"],
    ANALYTICS_PATHS["rating_distribution"]: ["rating", "count", "percentage"],
    ANALYTICS_PATHS["user_activity"]: ["userId", "rating_count", "average_rating"],
    ANALYTICS_PATHS["movie_popularity"]: ["movieId", "title", "genres", "rating_count", "average_rating", "highly_rated_eligible"],
    ANALYTICS_PATHS["genre_statistics"]: ["genre", "movie_count", "rating_count", "average_rating"],
    ANALYTICS_PATHS["rating_trend"]: ["year", "rating_count", "average_rating"],
    ANALYTICS_PATHS["tag_statistics"]: ["tag", "tag_count"],
}


def main():
    spark = SparkSession.builder.appName("VerifyMovieLensTV2Outputs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    manifest = json.loads(spark.read.text(MANIFEST_PATH).first()[0])
    if manifest["status"] != "PASS":
        raise ValueError("TV2 manifest status is not PASS")
    counts = {}
    for path, expected in EXPECTED_COLUMNS.items():
        frame = spark.read.parquet(path)
        if frame.columns != expected:
            raise ValueError(f"Column mismatch at {path}: {frame.columns} != {expected}")
        counts[path] = frame.count()
    if counts[ALS_PATHS["train"]] != manifest["split"]["train_count"]:
        raise ValueError("Train count differs from manifest")
    if counts[ALS_PATHS["test"]] != manifest["split"]["test_count"]:
        raise ValueError("Test count differs from manifest")
    if counts[PROCESSED_PATHS["ratings"]] != manifest["summary"]["total_ratings"]:
        raise ValueError("Processed rating count differs from manifest")
    print(json.dumps({"status": "PASS", "manifest_status": manifest["status"],
                      "verified_output_counts": counts}, indent=2), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
