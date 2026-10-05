"""Small runtime integration test; it does not write production TV3 outputs."""

import json
import time

from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F

from config import MOVIES_PATH, TEST_PATH, TRAIN_PATH
from topn import recommend_unseen


def main():
    started = time.perf_counter()
    spark = SparkSession.builder.appName("MovieLensTV3SmokeTest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    train = spark.read.parquet(TRAIN_PATH).select("userId", "movieId", "rating")
    test = spark.read.parquet(TEST_PATH).select("userId", "movieId", "rating")
    movies = spark.read.parquet(MOVIES_PATH).select("movieId", "title", "genres")
    users = train.select("userId").distinct().orderBy("userId").limit(200)
    sample_train = train.join(F.broadcast(users), "userId", "inner")
    sample_test = test.join(F.broadcast(users), "userId", "inner")
    model = ALS(
        userCol="userId", itemCol="movieId", ratingCol="rating",
        implicitPrefs=False, coldStartStrategy="drop", rank=4,
        maxIter=1, regParam=0.1, seed=42,
    ).fit(sample_train)
    train_rows, test_rows = sample_train.count(), sample_test.count()
    predictions = model.transform(sample_test)
    prediction_rows = predictions.count()
    invalid_predictions = predictions.where(
        F.col("prediction").isNull() | F.isnan("prediction")
    ).count()
    unknown_rows = sample_test.join(
        model.userFactors.select(F.col("id").alias("userId")), "userId", "left_anti"
    ).count()
    unknown_movie_rows = sample_test.join(
        model.itemFactors.select(F.col("id").alias("movieId")), "movieId", "left_anti"
    ).count()
    recommendations = recommend_unseen(
        model, sample_train, movies, users.limit(5), 3, 5
    )
    recommendation_rows = recommendations.count()
    invalid_metadata = recommendations.where(
        F.col("title").isNull() | F.col("genres").isNull()
    ).count()
    watched_recommendations = recommendations.join(
        sample_train, ["userId", "movieId"], "inner"
    ).count()
    if prediction_rows <= 0 or recommendation_rows <= 0:
        raise RuntimeError(
            f"Smoke test produced prediction_rows={prediction_rows}, "
            f"recommendation_rows={recommendation_rows}"
        )
    if (invalid_predictions or invalid_metadata or watched_recommendations
            or unknown_rows or test_rows - prediction_rows != unknown_movie_rows):
        raise RuntimeError("Smoke test cold-start/metadata/history checks failed")
    print(json.dumps({
        "status": "PASS",
        "sample_train_rows": train_rows,
        "sample_test_rows": test_rows,
        "valid_prediction_rows": prediction_rows,
        "dropped_rows": test_rows - prediction_rows,
        "unknown_user_rows": unknown_rows,
        "unknown_movie_rows": unknown_movie_rows,
        "invalid_prediction_rows": invalid_predictions,
        "recommendation_rows": recommendation_rows,
        "invalid_metadata_rows": invalid_metadata,
        "watched_recommendation_rows": watched_recommendations,
        "runtime_sec": time.perf_counter() - started,
    }, indent=2), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
