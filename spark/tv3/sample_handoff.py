"""Reload SAMPLE model, export bounded Top-N/Top-K in a separate application."""

import json
import time

from pyspark import StorageLevel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, functions as F

from config import (DEMO_USERS_PATH, MANIFEST_PATH, MODEL_PATH, MOVIES_PATH,
                    RECOMMENDATIONS_PATH, SAMPLE_USERS_PATH, SEED, TEST_PATH,
                    TOPK_METRICS_PATH, TRAIN_PATH, RECOMMENDATION_CANDIDATE_MULTIPLIER)
from output_contract import (check_factors, check_recommendations, read_manifest,
                             require_new_outputs, write_manifest)
from topn import deterministic_user_sample, evaluate_top_k, recommend_unseen


def main():
    spark = SparkSession.builder.appName("TV3SampleHandoff").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    cached = []
    try:
        manifest = read_manifest(spark, MANIFEST_PATH)
        if manifest["status"] != "SAMPLE_MODEL_READY_WARNING" or manifest["model_scope"] != "SAMPLE":
            raise ValueError("Expected ready SAMPLE model before handoff")
        require_new_outputs(spark, [RECOMMENDATIONS_PATH, DEMO_USERS_PATH, TOPK_METRICS_PATH])
        users = spark.read.parquet(SAMPLE_USERS_PATH)
        train = spark.read.parquet(TRAIN_PATH).join(F.broadcast(users), "userId", "inner").persist(StorageLevel.DISK_ONLY)
        test = spark.read.parquet(TEST_PATH).join(F.broadcast(users), "userId", "inner").persist(StorageLevel.DISK_ONLY)
        cached.extend([train, test])
        movies = spark.read.parquet(MOVIES_PATH)
        model = ALSModel.load(MODEL_PATH)
        check_factors(model)
        demo = deterministic_user_sample(users, 20, SEED)
        demo.coalesce(1).write.mode("errorifexists").parquet(DEMO_USERS_PATH)
        demo = spark.read.parquet(DEMO_USERS_PATH)
        print("[1/2] Exporting Top-10 for 20 deterministic demo users", flush=True)
        recommendations = recommend_unseen(model, train, movies, demo, 10, RECOMMENDATION_CANDIDATE_MULTIPLIER)
        recommendations.coalesce(1).write.mode("errorifexists").parquet(RECOMMENDATIONS_PATH)
        rows = spark.read.parquet(RECOMMENDATIONS_PATH)
        checks = check_recommendations(rows, train, movies, demo, 10)
        # This collect is explicitly bounded to twenty IDs for the handoff document.
        demo_ids = [row.userId for row in demo.orderBy("userId").limit(20).collect()]
        manifest["recommendations"] = {"path": RECOMMENDATIONS_PATH, "scope": "SAMPLE_DEMO",
            "top_n": 10, "requested_users": len(demo_ids), "actual_users": checks["users"],
            "rows": checks["rows"], "demo_users_path": DEMO_USERS_PATH, "demo_user_ids": demo_ids,
            "candidate_multiplier": RECOMMENDATION_CANDIDATE_MULTIPLIER,
            "excludes_train_history": True, "checks": checks, "schema": rows.schema.simpleString()}
        print("[2/2] Precision/Recall@10 for 500 eligible users inside SAMPLE", flush=True)
        started = time.perf_counter()
        top_k = evaluate_top_k(model, train, test, movies, 10, 4.0, 500,
                               RECOMMENDATION_CANDIDATE_MULTIPLIER, SEED)
        top_k.update({"model_scope": "SAMPLE", "evaluation_scope": "SAMPLE",
                      "evaluation_dataset": "TV2 test deterministic user sample",
                      "evaluation_time_sec": time.perf_counter() - started})
        spark.createDataFrame([top_k]).coalesce(1).write.mode("errorifexists").parquet(TOPK_METRICS_PATH)
        manifest["top_k"] = top_k
        manifest["outputs"].update({"recommendations": RECOMMENDATIONS_PATH,
                                    "demo_users": DEMO_USERS_PATH, "top_k_metrics": TOPK_METRICS_PATH})
        manifest.update({"status": "SAMPLE_COMPLETE_WARNING", "recommendation_scope": "SAMPLE_DEMO"})
        write_manifest(spark, manifest, MANIFEST_PATH, overwrite=True)
        print(json.dumps({"status": manifest["status"], "recommendations": manifest["recommendations"],
                          "top_k": top_k}, indent=2), flush=True)
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
