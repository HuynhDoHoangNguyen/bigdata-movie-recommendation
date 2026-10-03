"""Small runtime integration test; it does not write production TV3 outputs."""

from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F

from config import MOVIES_PATH, TEST_PATH, TRAIN_PATH
from topn import recommend_unseen


def main():
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
    prediction_rows = model.transform(sample_test).count()
    recommendations = recommend_unseen(
        model, sample_train, movies, users.limit(5), 3, 5
    )
    recommendation_rows = recommendations.count()
    if prediction_rows <= 0 or recommendation_rows <= 0:
        raise RuntimeError(
            f"Smoke test produced prediction_rows={prediction_rows}, "
            f"recommendation_rows={recommendation_rows}"
        )
    print({
        "status": "PASS",
        "sample_train_rows": sample_train.count(),
        "sample_test_rows": sample_test.count(),
        "valid_prediction_rows": prediction_rows,
        "recommendation_rows": recommendation_rows,
    }, flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
