"""Rating-prediction and ranking evaluation utilities."""

import time

from pyspark import StorageLevel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F


def evaluate_ratings(model, evaluation, known_total=None):
    """Evaluate with ALS cold-start rows dropped and report their coverage."""
    started = time.perf_counter()
    test_rows = known_total if known_total is not None else evaluation.count()
    predictions = model.transform(evaluation).select(
        "userId", "movieId", "rating", "prediction"
    ).persist(StorageLevel.DISK_ONLY)
    try:
        valid_rows = predictions.count()
        dropped_rows = test_rows - valid_rows
        if valid_rows <= 0:
            raise ValueError("ALS produced no valid predictions")
        rmse = RegressionEvaluator(
            metricName="rmse", labelCol="rating", predictionCol="prediction"
        ).evaluate(predictions)
        mae = RegressionEvaluator(
            metricName="mae", labelCol="rating", predictionCol="prediction"
        ).evaluate(predictions)
        return {
            "evaluation_rows": int(test_rows),
            "prediction_rows_before_filter": int(test_rows),
            "valid_prediction_rows": int(valid_rows),
            "dropped_rows": int(dropped_rows),
            "dropped_percentage": float(dropped_rows * 100.0 / test_rows),
            "rmse": float(rmse),
            "mae": float(mae),
            "evaluation_time_sec": float(time.perf_counter() - started),
            "cold_start_policy": "ALS coldStartStrategy=drop",
        }
    finally:
        predictions.unpersist()


def split_train_validation(train, validation_ratio, seed):
    """Deterministic per-user split that leaves at least one row in train_core."""
    from pyspark.sql import Window

    if not 0 < validation_ratio < 1:
        raise ValueError("validation_ratio must be between 0 and 1")
    counts = Window.partitionBy("userId")
    order = Window.partitionBy("userId").orderBy(
        F.xxhash64("userId", "movieId", F.lit(seed)), "movieId"
    )
    assigned = (
        train.withColumn("_count", F.count("*").over(counts))
        .withColumn("_position", F.row_number().over(order))
        .withColumn(
            "_validation_size",
            F.when(F.col("_count") > 1, F.greatest(F.lit(1), F.floor(F.col("_count") * validation_ratio)))
            .otherwise(F.lit(0)),
        )
    )
    validation = assigned.where(F.col("_position") <= F.col("_validation_size"))
    train_core = assigned.where(F.col("_position") > F.col("_validation_size"))
    helper_columns = ["_count", "_position", "_validation_size"]
    return train_core.drop(*helper_columns), validation.drop(*helper_columns)
