"""Independent verifier for the TV3 -> TV4 output contract."""

import json
import math

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, functions as F, types as T

from config import (
    EXPERIMENTS_PATH,
    FINAL_METRICS_PATH,
    MANIFEST_PATH,
    MODEL_PATH,
    RECOMMENDATIONS_PATH,
    TOPK_METRICS_PATH,
)


RECOMMENDATION_TYPES = {
    "userId": T.IntegerType(),
    "movieId": T.IntegerType(),
    "title": T.StringType(),
    "genres": T.StringType(),
    "prediction": T.DoubleType(),
    "rank": T.IntegerType(),
}


def path_exists(spark, path):
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(hadoop_path)


def require_path(spark, path, label):
    if not path_exists(spark, path):
        raise ValueError(f"Missing {label}: {path}")


def main():
    spark = SparkSession.builder.appName("VerifyMovieLensTV3Outputs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    errors, warnings = [], []
    for path, label in [
        (MODEL_PATH, "ALS model"),
        (EXPERIMENTS_PATH, "experiment metrics"),
        (FINAL_METRICS_PATH, "final metrics"),
        (TOPK_METRICS_PATH, "Top-K metrics"),
        (RECOMMENDATIONS_PATH, "recommendations"),
        (MANIFEST_PATH, "manifest"),
    ]:
        try:
            require_path(spark, path, label)
        except ValueError as exc:
            errors.append(str(exc))

    summary = {}
    if not errors:
        manifest = json.loads(spark.read.text(MANIFEST_PATH).first()[0])
        if manifest.get("status") not in ("PASS", "WARNING"):
            errors.append("Manifest status is neither PASS nor WARNING")
        if manifest.get("status") == "WARNING":
            warnings.append(manifest.get("limitation", "Manifest reports limited scope"))
        ALSModel.load(MODEL_PATH)

        experiments = spark.read.parquet(EXPERIMENTS_PATH)
        final_metrics = spark.read.parquet(FINAL_METRICS_PATH)
        if experiments.where(F.col("status") == "PASS").count() < 1:
            errors.append("No successful ALS experiment")
        metric = final_metrics.first().asDict()
        for name in ("rmse", "mae"):
            value = metric.get(name)
            if value is None or not math.isfinite(value) or value < 0:
                errors.append(f"Invalid final {name}: {value}")

        recommendations = spark.read.parquet(RECOMMENDATIONS_PATH)
        actual_types = {field.name: field.dataType for field in recommendations.schema.fields}
        if recommendations.columns != list(RECOMMENDATION_TYPES):
            errors.append(f"Recommendation columns mismatch: {recommendations.columns}")
        wrong_types = {name: str(actual_types.get(name)) for name, expected in RECOMMENDATION_TYPES.items()
                       if actual_types.get(name) != expected}
        if wrong_types:
            errors.append(f"Recommendation types mismatch: {wrong_types}")
        top_n = int(manifest["recommendations"]["top_n"])
        checks = recommendations.agg(
            F.count("*").alias("rows"),
            F.countDistinct("userId").alias("users"),
            F.sum(F.col("userId").isNull().cast("long")).alias("null_userId"),
            F.sum(F.col("movieId").isNull().cast("long")).alias("null_movieId"),
            F.sum(F.col("title").isNull().cast("long")).alias("null_title"),
            F.sum(F.col("genres").isNull().cast("long")).alias("null_genres"),
            F.sum((F.col("prediction").isNull() | F.isnan("prediction")).cast("long")).alias("invalid_prediction"),
            F.sum(((F.col("rank") < 1) | (F.col("rank") > top_n)).cast("long")).alias("invalid_rank"),
        ).first().asDict()
        duplicates = recommendations.groupBy("userId", "rank").count().where("count > 1").count()
        if any((checks[key] or 0) > 0 for key in (
            "null_userId", "null_movieId", "null_title", "null_genres",
            "invalid_prediction", "invalid_rank"
        )):
            errors.append(f"Recommendation data-quality failure: {checks}")
        if duplicates:
            errors.append(f"Duplicate userId + rank rows: {duplicates}")
        if checks["rows"] != manifest["recommendations"]["rows"]:
            errors.append("Recommendation row count differs from manifest")
        if checks["users"] != manifest["recommendations"]["actual_users"]:
            errors.append("Recommendation user count differs from manifest")
        if checks["rows"] < checks["users"]:
            warnings.append("Some recommendation users may have fewer than one row")
        summary = {
            "manifest_status": manifest.get("status"),
            "successful_experiments": experiments.where(F.col("status") == "PASS").count(),
            "final_rmse": metric.get("rmse"),
            "final_mae": metric.get("mae"),
            "recommendation_rows": checks["rows"],
            "recommendation_users": checks["users"],
            "top_k": spark.read.parquet(TOPK_METRICS_PATH).first().asDict(),
        }

    status = "FAIL" if errors else ("WARNING" if warnings else "PASS")
    result = {"status": status, "errors": errors, "warnings": warnings, "summary": summary}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
    spark.stop()
    if errors:
        raise RuntimeError("TV3 output verification failed")


if __name__ == "__main__":
    main()
